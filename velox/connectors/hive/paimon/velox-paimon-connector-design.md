# Velox Paimon Connector Design Document

> Complete read support for Append-Only tables and Primary Key tables

---

## 1. Design Goals

Implement native Apache Paimon table reading capability in the Velox engine, covering two core table types:

| Table Type | Read Semantics | Complexity |
|------------|----------------|------------|
| **Append-Only** | Sequential file reading, no deduplication | Low |
| **Primary Key (rawConvertible)** | Sequential file reading, no merge needed (fully compacted) | Low |
| **Primary Key (merge required)** | Multi-file k-way sorted merge + deduplication within the same (partition, bucket) | High |

Design principles:
- No custom Operators; fully implemented within the Connector/DataSource framework
- Unified read path dispatch via the `rawConvertible` flag; the SplitReader layer is unaware of table types
- Pluggable merge functions; initially implements Deduplicate, with extension points reserved for Partial-Update and Aggregate

### 1.1 rawConvertible Core Concept

`rawConvertible` is a boolean flag on Paimon's `DataSplit` that determines whether files within a split can be **read directly without merge processing**. This is the sole criterion for read path dispatch in the Velox Paimon Connector.

**rawConvertible = true (Raw File Read Path):**
- No key overlap between files; no sort-merge required
- Sequential file-by-file reading, reading only user columns
- Applicable to: Append-Only tables (always true), fully compacted PK tables, PK tables with Deletion Vectors enabled, first_row merge engine
- Future extension: apply Deletion Vector bitmap to filter deleted rows

**rawConvertible = false (Merge Read Path):**
- Key overlap exists between files; requires IntervalPartition grouping + SortMergeReader k-way merge
- Reads the full KV schema (_KEY_*, _SEQUENCE_NUMBER, _VALUE_KIND + user columns)
- Extracts user columns for output after deduplication/aggregation via MergeFunction

**rawConvertible Determination Rules** (consistent with Paimon Java `MergeTreeSplitGenerator`):

For Append-Only tables: always `true` (`AppendOnlySplitGenerator.alwaysRawConvertible() = true`).

For PK tables, `rawConvertible = true` if and only if all conditions are met:
1. All files have `level > 0` (no Level-0 files)
2. All files have `deleteRowCount == 0`
3. At least one of the following holds:
   - `mergeEngine == "first_row"`
   - Deletion vectors are enabled (`deletionVectorsEnabled = true`)
   - All files are at the same level

> **Key Design Point:** The SplitReader layer is completely unaware of `tableType` (Append-Only vs PrimaryKey) and selects the read strategy solely based on `rawConvertible`. `tableType` is retained as metadata on the Split for logging, debugging, and external system interaction, but does not participate in read path decisions.

---

## 2. Overall Architecture

### 2.1 Module Structure

```
velox/connectors/paimon/
|-- PaimonConnector.h/cpp                  # Connector factory
|-- PaimonConnectorSplit.h                 # Split definition (multi-file, rawConvertible flag)
|-- PaimonDataSource.h/cpp                 # DataSource (rawConvertible strategy dispatch)
|-- PaimonMetadata.h/cpp                   # Metadata parsing + Split generation
|
|-- reader/
|   |-- PaimonSplitReader.h                # Abstract base class + ColumnMapping definition
|   |-- PaimonRawFileSplitReader.h/cpp     # rawConvertible=true: sequential file reading
|   |-- PaimonMergeSplitReader.h/cpp       # rawConvertible=false: merge-on-read
|   +-- PaimonFileReader.h/cpp             # Low-level single-file reader wrapper
|
|-- merge/
|   |-- PaimonKeyComparator.h/cpp          # Primary key comparator
|   |-- PaimonMergeFunction.h              # Merge function abstract interface + DeduplicateMerge
|   |-- PaimonSortMergeReader.h/cpp        # Min-heap k-way merge
|   +-- PaimonIntervalPartition.h/cpp      # File grouping into sections by key range
|
+-- tests/
    |-- PaimonMetadataTest.cpp
    |-- PaimonConnectorTest.cpp
    |-- PaimonMergeTest.cpp
    |-- PaimonE2ETest.cpp
    +-- CMakeLists.txt
```

> **Naming Change Notes:**
> - `PaimonAppendOnlySplitReader` -> `PaimonRawFileSplitReader`: Aligns with Paimon's `RawFileSplitRead` concept, expressing "direct file read" semantics rather than being limited to Append-Only tables
> - `PaimonKeyValueSplitReader` -> `PaimonMergeSplitReader`: Aligns with Paimon's `MergeFileSplitRead` concept, expressing "merge-on-read" semantics
> - `ColumnMapping` moved from `PaimonAppendOnlySplitReader` to the `PaimonSplitReader` base class, eliminating cross-class references

### 2.2 Class Relationship Diagram

```
                          +-------------------+
                          | PaimonConnector    |
                          +-------------------+
                                   |
                                   | createDataSource()
                                   v
                          +-------------------+
                          | PaimonDataSource   |
                          +-------------------+
                                   |
                        addSplit() | rawConvertible dispatch
                          +--------+--------+
                          |                 |
                 rawConvertible        rawConvertible
                    = true                = false
                          |                 |
                          v                 v
              +-------------------------+  +----------------------------+
              | PaimonRawFile           |  | PaimonMerge                |
              | SplitReader             |  | SplitReader                |
              +-------------------------+  +----------------------------+
              | Sequential file reading |  |                            |
              | + Deletion Vector       |  |-- IntervalPartition        |
              |   filtering             |  |     (group files into      |
              | (TODO: DV support)      |  |      sections)             |
              |                         |  |                            |
              |                         |  |-- PaimonSortMergeReader    |
              |                         |  |     (k-way merge)          |
              |                         |  |     |-- PaimonKeyComparator|
              |                         |  |     +-- PaimonMergeFunction|
              |                         |  |                            |
                          v                 v
              +-------------------------------------------+
              |         PaimonFileReader                   |
              |  (wraps dwio::common::Reader + RowReader)  |
              +-------------------------------------------+
```

---

## 3. PaimonConnectorSplit Redesign

The current Split contains only a single file path. The new design needs to support multi-file lists so that PK tables can package all files from the same (partition, bucket) into a single Split.

### 3.1 Data File Information

```cpp
/// Describes a single data file within a Split.
struct PaimonDataFileInfo {
  std::string filePath;       // Full file path
  uint64_t fileSize;          // File size in bytes
  int64_t rowCount;           // Number of rows
  int32_t level;              // LSM level (0 = newest layer)
  int64_t minSequenceNumber;  // Minimum sequence number in the file
  int64_t maxSequenceNumber;  // Maximum sequence number in the file

  // min/max key statistics (binary encoded)
  // Used by the IntervalPartition grouping algorithm
  std::optional<std::string> minKey;
  std::optional<std::string> maxKey;
};
```

### 3.2 Complete Split Definition

```cpp
struct PaimonConnectorSplit : public ConnectorSplit {
  // ---- Common fields ----
  std::string tablePath;        // Table root path
  dwio::common::FileFormat fileFormat;
  int32_t bucket;
  int64_t snapshotId;
  int64_t schemaId;
  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;

  // ---- File list ----
  // Append-Only: typically 1 file
  // PK rawConvertible: possibly multiple files (sequential concatenation)
  // PK merge: all files under the same (partition, bucket) that need to be merged together
  std::vector<PaimonDataFileInfo> dataFiles;

  // ---- Table type ----
  enum class TableType { kAppendOnly, kPrimaryKey };
  TableType tableType{TableType::kAppendOnly};

  // ---- PK table specific fields ----
  bool rawConvertible{true};             // true = no merge needed
  std::vector<std::string> primaryKeys;  // Primary key column names
  std::string mergeEngine{"deduplicate"};// Merge strategy name
};
```

### 3.3 Design Notes

- `dataFiles` is a flat file list that **does not contain** section/sorted-run structure. The reading side rebuilds the grouping via the IntervalPartition algorithm. This is consistent with Paimon Java's DataSplit design.
- `rawConvertible` is computed and passed in by the Split generator (external query planner or PaimonMetadataReader).
- `primaryKeys` and `mergeEngine` are carried with the Split, so DataSource does not need to read additional metadata to obtain this information.
- Backward compatible: for Append-Only tables and single-file scenarios, the `dataFiles` list has length 1, and behavior is consistent with the current implementation.

---

## 4. PaimonDataSource Strategy Dispatch

PaimonDataSource is the entry point called by the Velox TableScan operator. After refactoring, it no longer directly manages dwio Readers but delegates to the corresponding SplitReader implementation based on the `rawConvertible` flag.

### 4.1 Core Dispatch Logic

```cpp
void PaimonDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  auto paimonSplit = std::dynamic_pointer_cast<PaimonConnectorSplit>(split);

  // Build column mappings (same as current implementation)
  resolveColumnMappings(paimonSplit);

  // Select read strategy based on rawConvertible only.
  // Note: tableType is not checked. For Append-Only tables, rawConvertible
  // is always true, so they naturally go through the RawFile path.
  // The SplitReader layer is unaware of table type.
  if (paimonSplit->rawConvertible) {
    // Raw path: sequential file reading (+ future Deletion Vector filtering)
    splitReader_ = std::make_unique<PaimonRawFileSplitReader>(
        paimonSplit, outputType_, columnMappings_, pool_, fileSystem_);
  } else {
    // Merge path: IntervalPartition + k-way sorted merge + deduplication
    splitReader_ = std::make_unique<PaimonMergeSplitReader>(
        paimonSplit, outputType_, columnMappings_, pool_, fileSystem_);
  }
}

std::optional<RowVectorPtr> PaimonDataSource::next(
    uint64_t size, ContinueFuture& future) {
  auto result = splitReader_->next(size);
  if (!result) {
    splitReader_.reset();
    return nullptr;
  }
  completedRows_ += result->size();
  completedBytes_ += result->retainedSize();
  return result;
}
```

### 4.2 Path Selection Decision Tree

```
addSplit(split)
  |
  +-- rawConvertible == true?
  |     YES --> PaimonRawFileSplitReader
  |             (Append-Only tables, fully compacted PK tables, DV enabled, first_row engine)
  |
  +-- rawConvertible == false
        --> PaimonMergeSplitReader
            (PK tables with key overlap, requiring merge-on-read)
```
---

## 5. Reader Layer Design

### 5.1 PaimonSplitReader (Abstract Interface + Shared Types)

```cpp
class PaimonSplitReader {
 public:
  /// Column mapping entry describing how an output column maps to its source.
  /// Shared between PaimonRawFileSplitReader and PaimonMergeSplitReader.
  struct ColumnMapping {
    std::string name;
    int32_t fieldId;
    PaimonColumnHandle::ColumnType columnType;
    int32_t fileColumnIndex; // Index in file schema, or -1.
  };

  virtual ~PaimonSplitReader() = default;

  /// Read the next batch of data. Returns nullptr when the split is exhausted.
  virtual RowVectorPtr next(uint64_t batchSize) = 0;

  virtual uint64_t getCompletedRows() const = 0;
  virtual uint64_t getCompletedBytes() const = 0;
};
```

### 5.2 PaimonFileReader (Low-Level Single-File Wrapper)

Wraps `dwio::common::Reader` + `RowReader`, providing a simple interface for upper layers.

```cpp
class PaimonFileReader {
 public:
  PaimonFileReader(
      const std::string& filePath,
      dwio::common::FileFormat format,
      const RowTypePtr& fileSchema,             // File's RowType
      const std::vector<std::string>& readColumns, // Column names to read
      memory::MemoryPool* pool,
      std::shared_ptr<filesystems::FileSystem> fs);

  /// Read the next batch, returns nullptr when the file is exhausted.
  RowVectorPtr next(uint64_t batchSize);

  bool hasNext() const;
  void close();
};
```

Key design points:
- Uses `ColumnSelector(fileSchema, readColumns)` for column name-based selection, avoiding index matching issues
- When reading for the merge path, `readColumns` includes system columns like `_KEY_*`, `_SEQUENCE_NUMBER`, `_VALUE_KIND`
- When reading for the raw path, `readColumns` includes only user columns

### 5.3 PaimonRawFileSplitReader (Raw File Read Path)

Handles split reading for `rawConvertible=true`. Reads files sequentially in list order, unaware of table type.

Applicable scenarios:
- Append-Only tables (all splits)
- Fully compacted PK table splits
- PK table splits with Deletion Vectors enabled
- PK table splits using the first_row merge engine

```cpp
class PaimonRawFileSplitReader : public PaimonSplitReader {
 public:
  PaimonRawFileSplitReader(
      std::shared_ptr<PaimonConnectorSplit> split,
      const RowTypePtr& outputType,
      const std::vector<ColumnMapping>& columnMappings,
      memory::MemoryPool* pool,
      std::shared_ptr<filesystems::FileSystem> fs);

  RowVectorPtr next(uint64_t batchSize) override {
    while (currentFileIdx_ < dataFiles_.size()) {
      if (!currentReader_) {
        openFile(currentFileIdx_);
      }
      auto result = currentReader_->next(batchSize);
      if (result) {
        return projectOutputColumns(result);
      }
      // Current file exhausted, advance to next file
      currentReader_.reset();
      currentFileIdx_++;
    }
    return nullptr;  // All files exhausted
  }

 private:
  void openFile(size_t idx);
  RowVectorPtr projectOutputColumns(const RowVectorPtr& input);
  void setPartitionValues(RowVectorPtr& output, vector_size_t size);

  // TODO: applyDeletionVector() -- apply Deletion Vector bitmap to filter deleted rows

  std::vector<PaimonDataFileInfo> dataFiles_;
  size_t currentFileIdx_{0};
  std::unique_ptr<PaimonFileReader> currentReader_;
};
```

> **Naming Alignment Note:** The class name `PaimonRawFileSplitReader` aligns with Paimon Java's `RawFileSplitRead` and C++'s `RawFileSplitRead::Match()`. This is more accurate than `PaimonAppendOnlySplitReader` since this reader also handles rawConvertible PK table splits.

### 5.4 PaimonMergeSplitReader (Merge Read Path)

Handles split reading for `rawConvertible=false` PK tables, performing merge-on-read. Internal flow:

1. At construction: build KeyValue schema from `primaryKeys` + `outputType`
2. At construction: run IntervalPartition algorithm on `dataFiles` to produce sections
3. Read sections sequentially: create a SortMergeReader for each section to perform k-way merge
4. Concatenate sections sequentially (key ranges do not overlap)

```cpp
class PaimonMergeSplitReader : public PaimonSplitReader {
 public:
  PaimonMergeSplitReader(
      std::shared_ptr<PaimonConnectorSplit> split,
      const RowTypePtr& outputType,
      const std::vector<ColumnMapping>& columnMappings,
      memory::MemoryPool* pool,
      std::shared_ptr<filesystems::FileSystem> fs)
  {
    // 1. Build KeyValue read schema
    buildKvSchema();

    // 2. IntervalPartition: group files into sections
    initializeSections();

    // 3. Open the first section
    if (!sections_.empty()) {
      openSection(0);
    }
  }

  RowVectorPtr next(uint64_t batchSize) override {
    while (currentSectionIdx_ < sections_.size()) {
      if (!currentMergeReader_) {
        if (!openSection(currentSectionIdx_)) break;
      }
      auto kvBatch = currentMergeReader_->next(batchSize);
      if (kvBatch) {
        return projectOutputColumns(kvBatch);
      }
      // Section exhausted, advance to next
      currentMergeReader_.reset();
      currentSectionIdx_++;
    }
    return nullptr;
  }

 private:
  void buildKvSchema();                         // Build KV schema
  void initializeSections();                     // IntervalPartition grouping
  bool openSection(size_t sectionIdx);           // Create SortMergeReader for a section
  RowVectorPtr projectOutputColumns(const RowVectorPtr& kvBatch);
  void setPartitionValues(RowVectorPtr& output, vector_size_t size);

  // KV schema and column indices.
  RowTypePtr kvSchema_;
  std::vector<std::string> kvReadColumns_;
  size_t numKeyColumns_{0};
  size_t seqColumnIndex_{0};
  size_t valueKindColumnIndex_{0};
  std::vector<int32_t> outputColumnToKvIndex_;

  // Sections from IntervalPartition.
  std::vector<Section> sections_;
  size_t currentSectionIdx_{0};
  std::unique_ptr<PaimonSortMergeReader> currentMergeReader_;
};
```

> **Naming Alignment Note:** The class name `PaimonMergeSplitReader` aligns with Paimon Java's `MergeFileSplitRead`. This is more accurate than `PaimonKeyValueSplitReader` since its core responsibility is merge-on-read, not "key-value reading" in general.

---

## 6. Merge Core Algorithms

### 6.1 KeyValue Schema Construction

PK table physical files use the KeyValue format. The corresponding schema must be constructed for reading:

```
User schema:     (id BIGINT, name VARCHAR, score DOUBLE)
Primary keys:    (id)

KeyValue read schema:
  _KEY_id           BIGINT      # index 0  -- key column (prefixed)
  _SEQUENCE_NUMBER  BIGINT      # index 1  -- system column
  _VALUE_KIND       TINYINT     # index 2  -- system column
  id                BIGINT      # index 3  -- value column (user column)
  name              VARCHAR     # index 4
  score             DOUBLE      # index 5
```

Construction rules:
1. Add `_KEY_<column_name>` columns in primary key order, with types matching the original columns
2. Add `_SEQUENCE_NUMBER` (BIGINT)
3. Add `_VALUE_KIND` (TINYINT)
4. Add all user columns (including the primary key columns themselves)

Sorting uses key columns (index 0) + sequence number (index 1).
Deduplication uses _VALUE_KIND (index 2).
Final output uses value columns (index 3+).

### 6.2 PaimonKeyComparator (Primary Key Comparator)

Column-by-column comparison based on Velox's `BaseVector::compare()`.

```cpp
class PaimonKeyComparator {
 public:
  PaimonKeyComparator(
      const RowTypePtr& kvSchema,
      size_t numKeyColumns);

  /// Compare primary keys of two rows. Returns <0, 0, >0.
  /// rowA is from batchA at index idxA, rowB is from batchB at index idxB.
  int32_t compareKeys(
      const RowVectorPtr& batchA, vector_size_t idxA,
      const RowVectorPtr& batchB, vector_size_t idxB) const;

  /// Compare primary keys + sequenceNumber (full sort key).
  int32_t compareKeysAndSequence(
      const RowVectorPtr& batchA, vector_size_t idxA,
      const RowVectorPtr& batchB, vector_size_t idxB) const;

  /// Compare two min/max key statistics (used for IntervalPartition).
  int32_t compareKeyStats(
      const std::string& keyA,
      const std::string& keyB) const;

 private:
  // Key column indices in kvSchema: [0, numKeyColumns_)
  size_t numKeyColumns_;
  // sequenceNumber column index: numKeyColumns_
  size_t seqColumnIndex_;
  CompareFlags flags_;  // nullsFirst=true, ascending=true
};
```

The comparison logic is consistent with Paimon Java's `SortMergeReaderWithMinHeap`:
1. Compare primary key columns (`_KEY_*`) column by column using `BaseVector::compare()`
2. When primary keys are equal, compare `_SEQUENCE_NUMBER`

### 6.3 IntervalPartition (File Grouping Algorithm)

Groups the file list into sections based on key range overlap, with each section further divided into the minimum number of sorted runs.

```cpp
struct SortedRun {
  std::vector<PaimonDataFileInfo> files;
  // Files within a SortedRun have non-overlapping key ranges, sorted by key
};

struct Section {
  std::vector<SortedRun> sortedRuns;
  // Key ranges between Sections do not overlap
  // Key ranges of multiple SortedRuns within a Section may overlap
};

class PaimonIntervalPartition {
 public:
  /// Group files into sections.
  /// Consistent with Paimon Java IntervalPartition.partition() algorithm.
  static std::vector<Section> partition(
      const std::vector<PaimonDataFileInfo>& files,
      const PaimonKeyComparator& comparator);
};
```

Two-phase algorithm:

**Phase 1: Section Division**
- Sort all files by (minKey, maxKey)
- Maintain a sweep line `bound` (maximum maxKey of scanned files)
- When a new file's `minKey > bound`, start a new section
- Guarantees non-overlapping key ranges between sections

**Phase 2: Sorted Run Assignment within Sections**
- Uses a greedy interval coloring algorithm (priority queue, sorted by last file's maxKey)
- For each file: if some run's maximum maxKey < current file's minKey, append to that run; otherwise create a new run
- Produces the minimum number of sorted runs

### 6.4 PaimonSortMergeReader (k-way Sorted Merge)

Uses a min-heap (`std::*_heap`) for k-way merge of multiple sorted runs.

```cpp
class PaimonSortMergeReader {
 public:
  PaimonSortMergeReader(
      std::vector<std::unique_ptr<SortedRunReader>> runReaders,
      std::shared_ptr<PaimonKeyComparator> comparator,
      std::unique_ptr<PaimonMergeFunction> mergeFunction,
      const RowTypePtr& kvSchema,
      memory::MemoryPool* pool);

  /// Returns the next batch of merged rows.
  /// Returns nullptr when all runs are exhausted.
  RowVectorPtr next(uint64_t batchSize);

 private:
  // Heap entry: identifies a run reader's position in the min-heap
  struct HeapEntry {
    size_t runIndex;
  };

  // Compare two HeapEntries (indirectly accesses current row via runReaders_)
  bool heapCompare(const HeapEntry& a, const HeapEntry& b) const;

  // Build output batch from collected RowRefs
  RowVectorPtr buildOutputBatch(const std::vector<RowRef>& rows);

  std::vector<std::unique_ptr<SortedRunReader>> runReaders_;
  std::shared_ptr<PaimonKeyComparator> comparator_;
  std::unique_ptr<PaimonMergeFunction> mergeFunction_;

  /// Min-heap stored as a vector, managed with std::*_heap functions.
  std::vector<HeapEntry> heap_;
};
```

Core flow (each next call):

```
1. Initialize min-heap (populate with current row from all runs on first call)
2. outputRows = []
3. while outputRows.size() < batchSize && heap is not empty:
   a. Pop the minimum element (winner) from the min-heap
   b. Record the winner's primary key
   c. mergeFunction->reset()
   d. Collect all elements with the same primary key as winner (pop consecutively from heap):
      - mergeFunction->add(batch, row)
      - advance that run; if it has a new row, push back into heap
   e. Check mergeFunction->hasResult()
      - If true (not deleted) -> add result to outputRows
      - If false (DELETE) -> skip
4. If outputRows is empty -> return nullptr
5. Otherwise buildOutputBatch(outputRows), return
```

### 6.5 PaimonMergeFunction (Merge Function Interface)

```cpp
class PaimonMergeFunction {
 public:
  virtual ~PaimonMergeFunction() = default;

  /// Reset state, preparing to process a new group of rows with the same primary key.
  virtual void reset() = 0;

  /// Add rows in ascending sequenceNumber order.
  /// batch is a RowVector with KeyValue schema.
  virtual void add(const RowVectorPtr& batch, vector_size_t row) = 0;

  /// Whether a valid result exists (INSERT/UPDATE_AFTER).
  /// Returns false if the key has been deleted (DELETE/UPDATE_BEFORE); caller should skip.
  virtual bool hasResult() const = 0;

  /// Get the row reference (batch + index) of the merge result.
  /// Only call when hasResult() == true.
  virtual std::pair<RowVectorPtr, vector_size_t> getResult() const = 0;
};
```

### 6.6 PaimonDeduplicateMerge (Deduplicate Strategy)

The simplest merge function: keep the last row (highest sequenceNumber).

```cpp
class PaimonDeduplicateMerge : public PaimonMergeFunction {
 public:
  explicit PaimonDeduplicateMerge(size_t valueKindColumnIndex);

  void reset() override {
    hasData_ = false;
    latestBatch_ = nullptr;
  }

  void add(const RowVectorPtr& batch, vector_size_t row) override {
    // Rows arrive in ascending sequenceNumber order
    // Simply overwrite; the last one is the latest
    latestBatch_ = batch;
    latestIndex_ = row;
    hasData_ = true;
  }

  bool hasResult() const override {
    if (!hasData_) return false;
    // Check _VALUE_KIND: DELETE(3) and UPDATE_BEFORE(1) are not output
    auto kind = latestBatch_->childAt(valueKindColumnIndex_)
        ->asFlatVector<int8_t>()->valueAt(latestIndex_);
    return kind == 0 /* INSERT */ || kind == 2 /* UPDATE_AFTER */;
  }

  std::pair<RowVectorPtr, vector_size_t> getResult() const override {
    return {latestBatch_, latestIndex_};
  }
};
```

---

## 7. End-to-End Data Flow

### 7.1 Append-Only Table Read (rawConvertible = true)

```
TableScan operator
  |
  +-- addSplit(Split{rawConvertible=true, dataFiles=[file1]})
  |
  +-- PaimonDataSource
        |
        +-- PaimonRawFileSplitReader
              |
              +-- PaimonFileReader(file1)
              |     |-- ColumnSelector(fileSchema, userColumns)
              |     +-- dwio RowReader
              |
              +-- projectOutputColumns()
              |     |-- Map file columns to output columns by name
              |     +-- Schema evolution: fill null for missing columns
              |
              +-- setPartitionValues()
                    +-- Fill partition columns with constant values
```

### 7.2 PK Table Read (rawConvertible = true)

```
TableScan operator
  |
  +-- addSplit(Split{rawConvertible=true,
  |                  dataFiles=[file1, file2, file3]})
  |
  +-- PaimonDataSource
        |
        +-- PaimonRawFileSplitReader (same reader as Append-Only)
              |
              +-- PaimonFileReader(file1) -> PaimonFileReader(file2) -> ...
              |     Note: rawConvertible PK table files contain _KEY_*/_SEQ/_VALUE_KIND columns,
              |     but these are not in readColumns; only user columns are read
              |
              +-- projectOutputColumns()
              +-- setPartitionValues()
              |
              +-- [TODO] applyDeletionVector()
                    If DeletionFile exists, apply Roaring Bitmap to filter deleted rows
```

### 7.3 PK Table Read (rawConvertible = false, merge required)

```
TableScan operator
  |
  +-- addSplit(Split{rawConvertible=false,
  |                  dataFiles=[f1,f2,f3,f4,f5],
  |                  primaryKeys=["id"], mergeEngine="deduplicate"})
  |
  +-- PaimonDataSource
        |
        +-- PaimonMergeSplitReader
              |
              +-- 1. buildKvSchema()
              |     Build: [_KEY_id, _SEQ, _VALUE_KIND, id, name, score]
              |
              +-- 2. IntervalPartition::partition(dataFiles)
              |     Input: [f1(L0), f2(L0), f3(L1), f4(L1), f5(L2)]
              |     Output:
              |       Section1: SortedRun1=[f5], SortedRun2=[f3,f4], SortedRun3=[f1]
              |       Section2: SortedRun1=[f2]   (key range does not overlap with Section1)
              |
              +-- 3. Read sections sequentially:
              |     |
              |     +-- Section1:
              |     |     PaimonSortMergeReader
              |     |       |-- Run1: SortedRunReader(f5, kvSchema)
              |     |       |-- Run2: SortedRunReader(f3 -> f4, kvSchema)
              |     |       +-- Run3: SortedRunReader(f1, kvSchema)
              |     |       |
              |     |       +-- Min-Heap (3-way merge)
              |     |       |     Compare: _KEY_id ASC, _SEQ ASC
              |     |       |
              |     |       +-- Rows with same _KEY_id -> DeduplicateMerge
              |     |       |     Keep row with highest _SEQ
              |     |       |     _VALUE_KIND == DELETE -> skip
              |     |       |
              |     |       +-- Output: [_KEY_id, _SEQ, _KIND, id, name, score]
              |     |
              |     +-- Section2:
              |           Similar processing...
              |
              +-- 4. projectOutputColumns()
              |     Drop _KEY_*, _SEQ, _VALUE_KIND
              |     Output: [id, name, score]
              |
              +-- 5. setPartitionValues()
```

---

## 8. Split Generation (PaimonMetadataReader Extension)

### 8.1 New Methods

```cpp
class PaimonMetadataReader {
 public:
  // ... Existing methods remain unchanged ...

  /// Generate splits for Append-Only tables.
  /// Each active data file generates an independent split.
  std::vector<PaimonConnectorSplit> generateAppendOnlySplits(
      const PaimonSnapshot& snapshot,
      const PaimonTableSchema& schema);

  /// Generate splits for PK tables.
  /// Groups by (partition, bucket), packaging based on
  /// rawConvertible determination results.
  std::vector<PaimonConnectorSplit> generatePrimaryKeySplits(
      const PaimonSnapshot& snapshot,
      const PaimonTableSchema& schema);
};
```

### 8.2 rawConvertible Determination Rules

See [1.1 rawConvertible Core Concept](#11-rawconvertible-core-concept).

The determination logic is implemented in the `PaimonMetadataReader::isRawConvertible()` static method, called by the Split generator. Not needed for Append-Only tables (always true).

### 8.3 Split Packaging Strategy

**Append-Only tables:**
- Each active file = one Split
- `tableType = kAppendOnly, rawConvertible = true`

**PK tables rawConvertible:**
- Files from the same (partition, bucket) can be split into multiple Splits by targetSplitSize
- Files within each Split are concatenated sequentially
- `tableType = kPrimaryKey, rawConvertible = true`

**PK tables requiring merge:**
- All files from the same (partition, bucket) are packaged into one Split
- DataSource rebuilds section structure for merging during reading
- `tableType = kPrimaryKey, rawConvertible = false`
- Carries `primaryKeys` and `mergeEngine`

---

## 9. Key Design Decisions

| Decision | Options | Rationale |
|----------|---------|-----------|
| **Read path dispatch criterion** | tableType + rawConvertible vs rawConvertible only | **rawConvertible only**. Append-Only tables always have rawConvertible=true (Paimon's `AppendOnlySplitGenerator.alwaysRawConvertible()=true`), making the tableType check redundant. Using rawConvertible alone for dispatch keeps the SplitReader layer unaware of table types, aligning semantics with Paimon Java/C++'s `RawFileSplitRead` / `MergeFileSplitRead`. |
| **Reader naming** | AppendOnly/KeyValue vs RawFile/Merge | **RawFile/Merge**. Aligns with Paimon's native concepts (`RawFileSplitRead` / `MergeFileSplitRead`). The old name `AppendOnlySplitReader` implied it only handled Append-Only tables, but it also handled rawConvertible PK tables, which was misleading. |
| **Keep two Reader classes** | Merge into one vs keep two | **Keep two**. The Raw path (file iterator + DV filtering) and Merge path (KV schema construction + IntervalPartition + SortMergeReader) have completely different state and algorithms; merging would create a "god class", and both evolve in different directions (Raw path will add DV support, Merge path will add more MergeFunctions). |
| **ColumnMapping location** | Defined in subclass vs base class | **Base class**. `ColumnMapping` is shared by both subclasses; placing it in `PaimonSplitReader` base class eliminates cross-class dependency. |
| **Merge implementation layer** | Inside DataSource vs custom Operator | Inside DataSource. No custom PlanNode/Operator needed; fully compatible with Velox's standard TableScan framework. |
| **Split granularity** | Single-file vs multi-file | Multi-file (Super-Split). PK table files from the same bucket must be merged together and cannot be split into independent splits. |
| **Section structure passing** | Carried in Split vs rebuilt at read time | Rebuilt at read time. Consistent with Paimon Java: Split carries only the file list; section structure is rebuilt via IntervalPartition during reading. The algorithm is deterministic. |
| **Merge algorithm** | Min-heap vs Loser tree | Currently uses min-heap (std::*_heap) for implementation simplicity. Can be optimized to loser tree later. |
| **MergeFunction pluggability** | Hardcoded vs interface abstraction | Interface abstraction. Initially implements Deduplicate, with future extensibility for Partial-Update and Aggregate. |
| **KeyValue schema identification** | Fixed position vs column name matching | Column name matching. Identifies columns via `_KEY_*`, `_SEQUENCE_NUMBER`, `_VALUE_KIND` prefixes/names rather than fixed column positions, which is more robust. |
| **PK table rawConvertible reading** | Read KV schema vs read user columns | Read user columns. rawConvertible means no merge is needed; user columns can be read directly, skipping system column parsing. |

---

## 10. Implementation Steps

### Step 1: Foundation Refactoring

- Refactor `PaimonConnectorSplit`: support `dataFiles` file list, `tableType`, `rawConvertible`, `primaryKeys`, `mergeEngine`
- Implement `PaimonFileReader`: wrap dwio Reader with a unified file reading interface
- Define `PaimonSplitReader` abstract interface, including the shared `ColumnMapping` type

### Step 2: Raw File Read Path

- Implement `PaimonRawFileSplitReader` (replacing the original `PaimonAppendOnlySplitReader`)
- Refactor `PaimonDataSource`: dispatch using only `rawConvertible`, removing the `tableType` check
- Migrate existing tests, ensuring backward compatibility

### Step 3: Merge Infrastructure

- Implement `PaimonKeyComparator`: primary key comparison based on `BaseVector::compare()`
- Implement `PaimonIntervalPartition`: file grouping algorithm
- Write unit tests for merge infrastructure

### Step 4: Merge Read Path

- Implement `PaimonSortMergeReader`: min-heap k-way merge
- Implement `PaimonMergeFunction` interface + `PaimonDeduplicateMerge`
- Implement `PaimonMergeSplitReader` (replacing the original `PaimonKeyValueSplitReader`): assemble the complete merge-on-read flow
- Write end-to-end merge read tests

### Step 5: Split Generation

- Extend `PaimonMetadataReader`: `generateAppendOnlySplits()` + `generatePrimaryKeySplits()`
- Implement `isRawConvertible()` determination logic
- Write split generation tests

### Step 6: Integration and Testing

- Complete end-to-end tests (covering both rawConvertible=true and rawConvertible=false paths)
- Schema evolution tests (PK table add/drop columns)
- Large data volume stress tests
- Multi-partition / multi-bucket scenario tests

### Step 7: Deletion Vector Support (Future)

- Add Deletion Vector bitmap filtering in `PaimonRawFileSplitReader`
- Support `PaimonDeletionFile` (Roaring Bitmap)
- For PK table rawConvertible splits with DV enabled, apply DV filtering to remove deleted rows after reading

---

## 11. Test Strategy

### 11.1 Unit Test Matrix

| Test Class | Coverage |
|------------|----------|
| `PaimonMetadataTest` | JSON parsing, type conversion, path construction, rawConvertible determination |
| `PaimonConnectorTest` | Connector factory, Split/Handle properties, end-to-end reading |
| `PaimonKeyComparatorTest` | Primary key comparison: single/multi column, NULL handling, different types |
| `PaimonIntervalPartitionTest` | File grouping: single file, no overlap, full overlap, partial overlap, multi-section |
| `PaimonSortMergeReaderTest` | k-way merge: 2-way, multi-way, single-way, empty input, large batch |
| `PaimonDeduplicateMergeTest` | Deduplication: INSERT overwrite, DELETE removal, UPDATE sequence, single record |
| `PaimonMergeSplitReaderTest` | Complete merge-on-read flow: build KV schema, merge, extract user columns |
| `PaimonE2ETest` | End-to-end: write DWRF files -> construct split -> read -> verify results |

### 11.2 Read Path Test Points

**Raw File Path (rawConvertible = true):**
- Append-Only table single-file/multi-file reading
- PK table rawConvertible split reading (read only user columns, skip system columns)
- Schema evolution (fill null for missing columns)
- Partition column population
- [TODO] Deletion Vector filtering verification

**Merge Path (rawConvertible = false):**
- Write DWRF files containing `_KEY_*`, `_SEQUENCE_NUMBER`, `_VALUE_KIND` system columns
- Simulate multiple level files (same primary key appearing in different files)
- Verify DEDUPLICATE deduplication: rows with higher sequenceNumber overwrite those with lower sequenceNumber
- Verify DELETE filtering: rows with `_VALUE_KIND = 3` do not appear in output
- IntervalPartition section correctness
- Multi-section sequential concatenation

**rawConvertible Determination Tests:**
- Append-Only table always true
- PK table with Level-0 files -> false
- PK table with deleteRowCount > 0 -> false
- PK table with all files at same level, no L0, no delete -> true
- PK table with mergeEngine=first_row -> true
- PK table with deletionVectorsEnabled -> true

---

## 12. Appendix: Paimon to Velox Type Mapping Reference

| Paimon Type | Velox Type | Notes |
|-------------|-----------|-------|
| INT | INTEGER() | |
| BIGINT | BIGINT() | |
| FLOAT | REAL() | |
| DOUBLE | DOUBLE() | |
| BOOLEAN | BOOLEAN() | |
| TINYINT | TINYINT() | Used by _VALUE_KIND |
| SMALLINT | SMALLINT() | |
| VARCHAR(n) / STRING | VARCHAR() | |
| CHAR(n) | VARCHAR() | Velox has no CHAR type |
| BINARY / VARBINARY | VARBINARY() | |
| DECIMAL(p,s) | DECIMAL(p,s) | p<=18 uses SHORT_DECIMAL |
| DATE | DATE() | |
| TIMESTAMP | TIMESTAMP() | |
| TIMESTAMP_LTZ | TIMESTAMP() | |
| ARRAY\<T> | ARRAY(T) | |
| MAP\<K,V> | MAP(K,V) | |
| ROW\<...> | ROW(...) | |
