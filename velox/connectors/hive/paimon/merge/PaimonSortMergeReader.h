/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/connectors/hive/paimon/PaimonDataFileMeta.h"
#include "velox/connectors/hive/paimon/merge/PaimonKeyComparator.h"
#include "velox/connectors/hive/paimon/merge/PaimonMergeFunction.h"
#include "velox/dwio/common/Reader.h"

namespace facebook::velox::connector::hive::paimon {

/// Reads a sorted run (list of files with non-overlapping key ranges) as a
/// sequential row stream. Files are read in order, and rows are exposed one
/// at a time via a cursor interface.
///
/// The returned batches are re-ordered to match the KV schema, so column
/// indices are consistent for the comparator and merge function.
class SortedRunReader {
 public:
  SortedRunReader(
      std::vector<PaimonDataFile> files,
      dwio::common::FileFormat format,
      const RowTypePtr& kvSchema,
      memory::MemoryPool* pool);

  /// Whether there is a current valid row.
  bool hasData() const {
    return currentBatch_ != nullptr;
  }

  /// The current batch (in KV schema order).
  const RowVectorPtr& currentBatch() const {
    return currentBatch_;
  }

  /// The current row index within the batch.
  vector_size_t currentRow() const {
    return currentRow_;
  }

  /// Advance to the next row. Returns true if a new row is available.
  bool advance();

 private:
  void openFile(size_t idx);
  bool readNextBatch();
  RowVectorPtr reorderToKvSchema(const RowVectorPtr& fileBatch);

  std::vector<PaimonDataFile> files_;
  dwio::common::FileFormat format_;
  RowTypePtr kvSchema_;
  memory::MemoryPool* pool_;

  size_t currentFileIdx_{0};
  std::unique_ptr<dwio::common::Reader> currentReader_;
  std::unique_ptr<dwio::common::RowReader> currentRowReader_;
  RowVectorPtr currentBatch_;
  vector_size_t currentRow_{0};
};

/// K-way merge reader that merges rows from multiple sorted runs using a
/// min-heap. Applies a MergeFunction to deduplicate/merge rows with the same
/// primary key, and filters out DELETE/RETRACT rows.
///
/// Output: RowVectors in the KV schema, containing only surviving rows.
///
/// The merge order is:
///   1. Primary key columns: ASC
///   2. _SEQUENCE_NUMBER: ASC
/// This ensures same-key rows arrive at the MergeFunction in sequence order,
/// with the newest row last (matching Paimon's behavior).
class PaimonSortMergeReader {
 public:
  PaimonSortMergeReader(
      std::vector<std::unique_ptr<SortedRunReader>> runReaders,
      std::shared_ptr<PaimonKeyComparator> comparator,
      std::unique_ptr<PaimonMergeFunction> mergeFunction,
      const RowTypePtr& kvSchema,
      memory::MemoryPool* pool);

  /// Get the next batch of merged rows. Returns nullptr when all runs are
  /// exhausted.
  RowVectorPtr next(uint64_t batchSize);

  uint64_t getCompletedRows() const {
    return completedRows_;
  }

 private:
  /// Row reference: a (batch, rowIndex) pair pointing to a specific row.
  struct RowRef {
    RowVectorPtr batch;
    vector_size_t row;
  };

  /// Heap entry: identifies a run reader in the min-heap.
  struct HeapEntry {
    size_t runIndex;
  };

  void initializeHeap();

  /// Heap comparator: returns true if a should come AFTER b (max-heap style
  /// for std::*_heap which is a max-heap by default; we invert to get
  /// min-heap).
  bool heapCompare(const HeapEntry& a, const HeapEntry& b) const;

  /// Build output RowVector from collected row references.
  RowVectorPtr buildOutputBatch(const std::vector<RowRef>& rows);

  std::vector<std::unique_ptr<SortedRunReader>> runReaders_;
  std::shared_ptr<PaimonKeyComparator> comparator_;
  std::unique_ptr<PaimonMergeFunction> mergeFunction_;
  RowTypePtr kvSchema_;
  memory::MemoryPool* pool_;

  /// Min-heap stored as a vector, managed with std::*_heap functions.
  std::vector<HeapEntry> heap_;
  bool initialized_{false};
  uint64_t completedRows_{0};
};

} // namespace facebook::velox::connector::hive::paimon
