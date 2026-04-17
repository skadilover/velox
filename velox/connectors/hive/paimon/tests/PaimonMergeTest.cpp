/*
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

#include <gtest/gtest.h>

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/paimon/PaimonConnectorSplit.h"
#include "velox/connectors/hive/paimon/merge/PaimonIntervalPartition.h"
#include "velox/connectors/hive/paimon/merge/PaimonKeyComparator.h"
#include "velox/connectors/hive/paimon/merge/PaimonMergeFunction.h"
#include "velox/connectors/hive/paimon/merge/PaimonSortMergeReader.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/tests/utils/TempFilePath.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::hive::paimon;

class PaimonMergeTest : public testing::Test,
                        public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    filesystems::registerLocalFileSystem();
    dwrf::registerDwrfReaderFactory();
    dwrf::registerDwrfWriterFactory();
  }

  void TearDown() override {
    dwrf::unregisterDwrfReaderFactory();
    dwrf::unregisterDwrfWriterFactory();
  }

  // Helper to create a PaimonDataFile with key stats.
  PaimonDataFile makeFileInfo(
      const std::string& filePath,
      const std::string& minKey,
      const std::string& maxKey,
      int32_t level = 0) {
    PaimonDataFile file;
    file.path = filePath;
    file.minKey = minKey;
    file.maxKey = maxKey;
    file.level = level;
    file.size = 1024;
    file.rowCount = 100;
    return file;
  }

  // Helper to create a PaimonDataFile without key stats.
  PaimonDataFile makeFileInfoNoStats(const std::string& filePath) {
    PaimonDataFile file;
    file.path = filePath;
    file.size = 1024;
    file.rowCount = 100;
    return file;
  }

  /// Write vectors to a DWRF file at the given path.
  void writeToFile(
      const std::string& filePath,
      const std::vector<RowVectorPtr>& vectors) {
    facebook::velox::dwrf::WriterOptions options;
    options.config = std::make_shared<facebook::velox::dwrf::Config>();
    options.schema = vectors[0]->type();
    auto fs = filesystems::getFileSystem(filePath, {});
    auto writeFile = fs->openFileForWrite(
        filePath,
        {.shouldCreateParentDirectories = true,
         .shouldThrowOnFileAlreadyExists = false});
    auto sink = std::make_unique<dwio::common::WriteFileSink>(
        std::move(writeFile), filePath);
    auto childPool = rootPool_->addAggregateChild("PaimonMergeTest.Writer");
    options.memoryPool = childPool.get();
    facebook::velox::dwrf::Writer writer(
        options, std::move(sink), *childPool);
    for (const auto& vec : vectors) {
      writer.write(vec);
    }
    writer.close();
  }

  /// Create a KV file with the schema:
  /// [_KEY_id(BIGINT), _SEQUENCE_NUMBER(BIGINT), _VALUE_KIND(TINYINT),
  ///  id(BIGINT), name(VARCHAR)]
  std::shared_ptr<exec::test::TempFilePath> writeKvFile(
      const std::vector<int64_t>& keyIds,
      const std::vector<int64_t>& seqNums,
      const std::vector<int8_t>& valueKinds,
      const std::vector<int64_t>& ids,
      const std::vector<std::string>& names) {
    auto filePath = exec::test::TempFilePath::create();
    std::vector<StringView> nameViews;
    nameViews.reserve(names.size());
    for (const auto& n : names) {
      nameViews.push_back(StringView(n));
    }
    auto batch = makeRowVector(
        {"_KEY_id", "_SEQUENCE_NUMBER", "_VALUE_KIND", "id", "name"},
        {
            makeFlatVector<int64_t>(keyIds),
            makeFlatVector<int64_t>(seqNums),
            makeFlatVector<int8_t>(valueKinds),
            makeFlatVector<int64_t>(ids),
            makeFlatVector<StringView>(nameViews),
        });
    writeToFile(filePath->getPath(), {batch});
    return filePath;
  }
};

// ===========================================================================
// PaimonKeyComparator tests
// ===========================================================================

TEST_F(PaimonMergeTest, keyComparatorSingleColumn) {
  // KV schema: [_KEY_id(BIGINT), _SEQ(BIGINT), _VALUE_KIND(TINYINT), ...]
  // numKeyColumns=1, seqColumnIndex=1
  PaimonKeyComparator cmp(1, 1);

  // Create two batches with key column and sequence number.
  auto batchA = makeRowVector(
      {"_KEY_id", "_SEQ", "_VALUE_KIND"},
      {
          makeFlatVector<int64_t>({10, 20, 30}),
          makeFlatVector<int64_t>({1, 2, 3}),
          makeFlatVector<int8_t>({0, 0, 0}),
      });

  auto batchB = makeRowVector(
      {"_KEY_id", "_SEQ", "_VALUE_KIND"},
      {
          makeFlatVector<int64_t>({15, 20, 25}),
          makeFlatVector<int64_t>({4, 5, 6}),
          makeFlatVector<int8_t>({0, 0, 0}),
      });

  // Key comparison: 10 vs 15
  EXPECT_LT(cmp.compareKeys(batchA, 0, batchB, 0), 0);
  // Key comparison: 20 vs 15
  EXPECT_GT(cmp.compareKeys(batchA, 1, batchB, 0), 0);
  // Key comparison: 20 vs 20
  EXPECT_EQ(cmp.compareKeys(batchA, 1, batchB, 1), 0);
  // Key comparison: 30 vs 25
  EXPECT_GT(cmp.compareKeys(batchA, 2, batchB, 2), 0);
}

TEST_F(PaimonMergeTest, keyComparatorMultiColumn) {
  // KV schema: [_KEY_a(BIGINT), _KEY_b(VARCHAR), _SEQ(BIGINT), ...]
  // numKeyColumns=2, seqColumnIndex=2
  PaimonKeyComparator cmp(2, 2);

  auto batchA = makeRowVector(
      {"_KEY_a", "_KEY_b", "_SEQ"},
      {
          makeFlatVector<int64_t>({1, 1, 2}),
          makeFlatVector<std::string>({"aaa", "bbb", "aaa"}),
          makeFlatVector<int64_t>({10, 20, 30}),
      });

  auto batchB = makeRowVector(
      {"_KEY_a", "_KEY_b", "_SEQ"},
      {
          makeFlatVector<int64_t>({1, 1, 3}),
          makeFlatVector<std::string>({"aaa", "ccc", "aaa"}),
          makeFlatVector<int64_t>({40, 50, 60}),
      });

  // (1, "aaa") vs (1, "aaa") -> equal
  EXPECT_EQ(cmp.compareKeys(batchA, 0, batchB, 0), 0);
  // (1, "bbb") vs (1, "ccc") -> bbb < ccc
  EXPECT_LT(cmp.compareKeys(batchA, 1, batchB, 1), 0);
  // (2, "aaa") vs (3, "aaa") -> 2 < 3
  EXPECT_LT(cmp.compareKeys(batchA, 2, batchB, 2), 0);
}

TEST_F(PaimonMergeTest, keyComparatorWithSequence) {
  PaimonKeyComparator cmp(1, 1);

  auto batchA = makeRowVector(
      {"_KEY_id", "_SEQ"},
      {
          makeFlatVector<int64_t>({10, 20, 20}),
          makeFlatVector<int64_t>({1, 2, 5}),
      });

  auto batchB = makeRowVector(
      {"_KEY_id", "_SEQ"},
      {
          makeFlatVector<int64_t>({10, 20, 20}),
          makeFlatVector<int64_t>({3, 2, 4}),
      });

  // Key 10, seq 1 vs key 10, seq 3 -> same key, seq 1 < 3
  EXPECT_LT(cmp.compareKeysAndSequence(batchA, 0, batchB, 0), 0);
  // Key 20, seq 2 vs key 20, seq 2 -> equal
  EXPECT_EQ(cmp.compareKeysAndSequence(batchA, 1, batchB, 1), 0);
  // Key 20, seq 5 vs key 20, seq 4 -> same key, seq 5 > 4
  EXPECT_GT(cmp.compareKeysAndSequence(batchA, 2, batchB, 2), 0);
}

TEST_F(PaimonMergeTest, keyComparatorNullHandling) {
  PaimonKeyComparator cmp(1, 1);

  auto batchA = makeRowVector(
      {"_KEY_id", "_SEQ"},
      {
          makeNullableFlatVector<int64_t>({std::nullopt, 10}),
          makeFlatVector<int64_t>({1, 2}),
      });

  auto batchB = makeRowVector(
      {"_KEY_id", "_SEQ"},
      {
          makeNullableFlatVector<int64_t>({std::nullopt, 20}),
          makeFlatVector<int64_t>({3, 4}),
      });

  // null vs null -> equal (nullsFirst, both null)
  EXPECT_EQ(cmp.compareKeys(batchA, 0, batchB, 0), 0);
  // null vs 20 -> null comes first (nullsFirst=true)
  EXPECT_LT(cmp.compareKeys(batchA, 0, batchB, 1), 0);
  // 10 vs null -> non-null comes after null
  EXPECT_GT(cmp.compareKeys(batchA, 1, batchB, 0), 0);
}

TEST_F(PaimonMergeTest, compareKeyBytes) {
  EXPECT_LT(PaimonKeyComparator::compareKeyBytes("abc", "abd"), 0);
  EXPECT_GT(PaimonKeyComparator::compareKeyBytes("abd", "abc"), 0);
  EXPECT_EQ(PaimonKeyComparator::compareKeyBytes("abc", "abc"), 0);
  EXPECT_LT(PaimonKeyComparator::compareKeyBytes("ab", "abc"), 0);
  EXPECT_GT(PaimonKeyComparator::compareKeyBytes("abc", "ab"), 0);
}

// ===========================================================================
// PaimonIntervalPartition tests
// ===========================================================================

TEST_F(PaimonMergeTest, partitionEmpty) {
  auto sections = PaimonIntervalPartition::partition({});
  EXPECT_TRUE(sections.empty());
}

TEST_F(PaimonMergeTest, partitionSingleFile) {
  auto sections = PaimonIntervalPartition::partition(
      {makeFileInfo("f1", "A", "C")});

  ASSERT_EQ(sections.size(), 1);
  ASSERT_EQ(sections[0].sortedRuns.size(), 1);
  ASSERT_EQ(sections[0].sortedRuns[0].files.size(), 1);
  EXPECT_EQ(sections[0].sortedRuns[0].files[0].path, "f1");
}

TEST_F(PaimonMergeTest, partitionNonOverlappingFiles) {
  // Three files with non-overlapping key ranges: [A,B], [D,E], [G,H]
  // Should produce 3 sections, each with 1 sorted run of 1 file.
  std::vector<PaimonDataFile> files = {
      makeFileInfo("f1", "A", "B"),
      makeFileInfo("f2", "D", "E"),
      makeFileInfo("f3", "G", "H"),
  };

  auto sections = PaimonIntervalPartition::partition(files);

  ASSERT_EQ(sections.size(), 3);
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_EQ(sections[i].sortedRuns.size(), 1);
    ASSERT_EQ(sections[i].sortedRuns[0].files.size(), 1);
  }
  EXPECT_EQ(sections[0].sortedRuns[0].files[0].path, "f1");
  EXPECT_EQ(sections[1].sortedRuns[0].files[0].path, "f2");
  EXPECT_EQ(sections[2].sortedRuns[0].files[0].path, "f3");
}

TEST_F(PaimonMergeTest, partitionFullyOverlappingFiles) {
  // Three files with fully overlapping key ranges: [A,Z], [A,Z], [A,Z]
  // Should produce 1 section with 3 sorted runs (each file in its own run).
  std::vector<PaimonDataFile> files = {
      makeFileInfo("f1", "A", "Z"),
      makeFileInfo("f2", "A", "Z"),
      makeFileInfo("f3", "A", "Z"),
  };

  auto sections = PaimonIntervalPartition::partition(files);

  ASSERT_EQ(sections.size(), 1);
  ASSERT_EQ(sections[0].sortedRuns.size(), 3);
  for (const auto& run : sections[0].sortedRuns) {
    ASSERT_EQ(run.files.size(), 1);
  }
}

TEST_F(PaimonMergeTest, partitionPartialOverlap) {
  // Files: [A,D], [C,F], [E,H], [J,K]
  // [A,D] and [C,F] overlap -> same section
  // [E,H] overlaps with [C,F]'s range extended to H -> same section
  // [J,K] doesn't overlap -> new section
  //
  // Section 1: {[A,D], [C,F], [E,H]} -> 2 sorted runs:
  //   Run 1: [A,D], [E,H] (non-overlapping)
  //   Run 2: [C,F]
  // Section 2: {[J,K]} -> 1 sorted run
  std::vector<PaimonDataFile> files = {
      makeFileInfo("f1", "A", "D"),
      makeFileInfo("f2", "C", "F"),
      makeFileInfo("f3", "E", "H"),
      makeFileInfo("f4", "J", "K"),
  };

  auto sections = PaimonIntervalPartition::partition(files);

  ASSERT_EQ(sections.size(), 2);

  // Section 1: 2 sorted runs
  ASSERT_EQ(sections[0].sortedRuns.size(), 2);

  // Section 2: 1 sorted run with 1 file
  ASSERT_EQ(sections[1].sortedRuns.size(), 1);
  ASSERT_EQ(sections[1].sortedRuns[0].files.size(), 1);
  EXPECT_EQ(sections[1].sortedRuns[0].files[0].path, "f4");
}

TEST_F(PaimonMergeTest, partitionLSMStyleFiles) {
  // Simulate LSM-like structure:
  // Level 0: [A,Z] (full range, uncompacted)
  // Level 1: [A,M], [N,Z] (two non-overlapping files)
  // Level 2: [A,G], [H,N], [O,Z] (three non-overlapping files)
  //
  // All overlap in one section. The algorithm should create:
  // - Run for L2 files (non-overlapping): [A,G], [H,N], [O,Z]
  // - Run for L1 files (non-overlapping): [A,M], [N,Z]
  // - Run for L0 file: [A,Z]
  std::vector<PaimonDataFile> files = {
      makeFileInfo("L0-1", "A", "Z", 0),
      makeFileInfo("L1-1", "A", "M", 1),
      makeFileInfo("L1-2", "N", "Z", 1),
      makeFileInfo("L2-1", "A", "G", 2),
      makeFileInfo("L2-2", "H", "N", 2),
      makeFileInfo("L2-3", "O", "Z", 2),
  };

  auto sections = PaimonIntervalPartition::partition(files);

  // All files overlap with [A,Z], so single section.
  ASSERT_EQ(sections.size(), 1);

  // Should have 3 sorted runs (one per "level" of non-overlapping files).
  // The exact assignment depends on the greedy algorithm's processing order.
  ASSERT_EQ(sections[0].sortedRuns.size(), 3);

  // Count total files across all runs.
  size_t totalFiles = 0;
  for (const auto& run : sections[0].sortedRuns) {
    totalFiles += run.files.size();
  }
  EXPECT_EQ(totalFiles, 6);
}

TEST_F(PaimonMergeTest, partitionChainedOverlap) {
  // Files form a chain: [1,3], [2,5], [4,7], [6,9], [11,13]
  // The first four files overlap in a chain -> 1 section
  // The last file is separate -> 1 section
  std::vector<PaimonDataFile> files = {
      makeFileInfo("f1", "1", "3"),
      makeFileInfo("f2", "2", "5"),
      makeFileInfo("f3", "4", "7"),
      makeFileInfo("f4", "6", "9"),
      makeFileInfo("f5", "B", "D"), // "B" > "9" lexicographically
  };

  auto sections = PaimonIntervalPartition::partition(files);

  ASSERT_EQ(sections.size(), 2);
  // Section 1 has 4 files in potentially 2 sorted runs.
  size_t totalInSection1 = 0;
  for (const auto& run : sections[0].sortedRuns) {
    totalInSection1 += run.files.size();
  }
  EXPECT_EQ(totalInSection1, 4);

  // Section 2 has 1 file.
  size_t totalInSection2 = 0;
  for (const auto& run : sections[1].sortedRuns) {
    totalInSection2 += run.files.size();
  }
  EXPECT_EQ(totalInSection2, 1);
}

TEST_F(PaimonMergeTest, partitionNoKeyStats) {
  // Files without key stats should fall back to conservative partitioning:
  // single section, each file in its own sorted run.
  std::vector<PaimonDataFile> files = {
      makeFileInfoNoStats("f1"),
      makeFileInfoNoStats("f2"),
      makeFileInfoNoStats("f3"),
  };

  auto sections = PaimonIntervalPartition::partition(files);

  ASSERT_EQ(sections.size(), 1);
  ASSERT_EQ(sections[0].sortedRuns.size(), 3);
  for (const auto& run : sections[0].sortedRuns) {
    ASSERT_EQ(run.files.size(), 1);
  }
}

TEST_F(PaimonMergeTest, partitionMixedKeyStats) {
  // Some files have key stats, some don't -> fallback to conservative.
  std::vector<PaimonDataFile> files = {
      makeFileInfo("f1", "A", "B"),
      makeFileInfoNoStats("f2"),
      makeFileInfo("f3", "C", "D"),
  };

  auto sections = PaimonIntervalPartition::partition(files);

  // Falls back to single section, each file in its own run.
  ASSERT_EQ(sections.size(), 1);
  ASSERT_EQ(sections[0].sortedRuns.size(), 3);
}

TEST_F(PaimonMergeTest, partitionRunAssignmentOptimal) {
  // Verify the greedy algorithm produces minimum runs within a section.
  // Files: [A,D], [B,E], [C,F], [D,G]
  // All overlap (chained) -> 1 section.
  // Greedy assigns:
  //   Run 1: [A,D], [D+..] can't fit [B,E] since B<=D
  //   -> multiple runs needed
  std::vector<PaimonDataFile> files = {
      makeFileInfo("f1", "A", "D"),
      makeFileInfo("f2", "B", "E"),
      makeFileInfo("f3", "C", "F"),
      makeFileInfo("f4", "G", "I"),
  };

  auto sections = PaimonIntervalPartition::partition(files);

  // [A,D],[B,E],[C,F] overlap -> section 1; [G,I] minKey G > bound F -> section 2
  ASSERT_EQ(sections.size(), 2);

  // Section 1: 3 files, at least 2 runs (since they overlap pairwise).
  ASSERT_GE(sections[0].sortedRuns.size(), 2);
  size_t totalInSection1 = 0;
  for (const auto& run : sections[0].sortedRuns) {
    totalInSection1 += run.files.size();
  }
  EXPECT_EQ(totalInSection1, 3);

  // Section 2: 1 file, 1 run.
  ASSERT_EQ(sections[1].sortedRuns.size(), 1);
  EXPECT_EQ(sections[1].sortedRuns[0].files[0].path, "f4");
}

TEST_F(PaimonMergeTest, partitionRunAssignmentSingleSection) {
  // All files fully overlapping -> single section, each file its own run.
  // Files: [A,Z], [B,Y], [C,X]
  // All within bound -> 1 section.
  // Since B <= Z and C <= Z, they all overlap.
  // Greedy: each file must start a new run since prior run's maxKey >= minKey.
  std::vector<PaimonDataFile> files = {
      makeFileInfo("f1", "A", "Z"),
      makeFileInfo("f2", "B", "Y"),
      makeFileInfo("f3", "C", "X"),
  };

  auto sections = PaimonIntervalPartition::partition(files);
  ASSERT_EQ(sections.size(), 1);
  ASSERT_EQ(sections[0].sortedRuns.size(), 3);
}

TEST_F(PaimonMergeTest, partitionSingleSortedRun) {
  // All files non-overlapping -> 1 section, 1 sorted run containing all files.
  std::vector<PaimonDataFile> files = {
      makeFileInfo("f1", "A", "B"),
      makeFileInfo("f2", "C", "D"),
      makeFileInfo("f3", "E", "F"),
  };

  auto sections = PaimonIntervalPartition::partition(files);

  // These should be 3 separate sections since none overlap.
  ASSERT_EQ(sections.size(), 3);
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_EQ(sections[i].sortedRuns.size(), 1);
    ASSERT_EQ(sections[i].sortedRuns[0].files.size(), 1);
  }
}

TEST_F(PaimonMergeTest, partitionAdjacentFiles) {
  // Files with adjacent but non-overlapping ranges: [A,B], [C,D]
  // B < C so they're in different sections.
  std::vector<PaimonDataFile> files = {
      makeFileInfo("f1", "A", "B"),
      makeFileInfo("f2", "C", "D"),
  };

  auto sections = PaimonIntervalPartition::partition(files);

  ASSERT_EQ(sections.size(), 2);
}

TEST_F(PaimonMergeTest, partitionTouchingFiles) {
  // Files that touch at boundary: [A,C], [C,E]
  // minKey "C" is NOT > maxKey "C", so they're in the same section.
  std::vector<PaimonDataFile> files = {
      makeFileInfo("f1", "A", "C"),
      makeFileInfo("f2", "C", "E"),
  };

  auto sections = PaimonIntervalPartition::partition(files);

  ASSERT_EQ(sections.size(), 1);
  // They overlap at C, so need 2 sorted runs.
  ASSERT_EQ(sections[0].sortedRuns.size(), 2);
}

// ===========================================================================
// PaimonMergeFunction tests
// ===========================================================================

TEST_F(PaimonMergeTest, deduplicateMergeBasic) {
  // KV schema: [_KEY_id, _SEQUENCE_NUMBER, _VALUE_KIND, id, name]
  PaimonDeduplicateMerge merge(/*valueKindColumnIndex=*/2);

  auto batch = makeRowVector(
      {"_KEY_id", "_SEQ", "_VALUE_KIND", "id", "name"},
      {
          makeFlatVector<int64_t>({1, 1, 1}),
          makeFlatVector<int64_t>({1, 2, 3}),
          makeFlatVector<int8_t>({0, 0, 0}), // all INSERT
          makeFlatVector<int64_t>({1, 1, 1}),
          makeFlatVector<StringView>({"old", "mid", "new"}),
      });

  // Add rows in sequence order (ASC). Last one should win.
  merge.reset();
  merge.add(batch, 0);
  merge.add(batch, 1);
  merge.add(batch, 2);

  ASSERT_TRUE(merge.hasResult());
  auto [resultBatch, resultRow] = merge.getResult();
  EXPECT_EQ(resultRow, 2); // Last row wins
}

TEST_F(PaimonMergeTest, deduplicateMergeDeleteFilters) {
  PaimonDeduplicateMerge merge(/*valueKindColumnIndex=*/2);

  auto batch = makeRowVector(
      {"_KEY_id", "_SEQ", "_VALUE_KIND"},
      {
          makeFlatVector<int64_t>({1, 1}),
          makeFlatVector<int64_t>({1, 2}),
          makeFlatVector<int8_t>({0, 3}), // INSERT then DELETE
      });

  merge.reset();
  merge.add(batch, 0);
  merge.add(batch, 1); // DELETE row

  EXPECT_FALSE(merge.hasResult()); // Deleted, no result
}

TEST_F(PaimonMergeTest, deduplicateMergeUpdateAfterSurvives) {
  PaimonDeduplicateMerge merge(/*valueKindColumnIndex=*/2);

  auto batch = makeRowVector(
      {"_KEY_id", "_SEQ", "_VALUE_KIND"},
      {
          makeFlatVector<int64_t>({1, 1}),
          makeFlatVector<int64_t>({1, 2}),
          makeFlatVector<int8_t>({0, 2}), // INSERT then UPDATE_AFTER
      });

  merge.reset();
  merge.add(batch, 0);
  merge.add(batch, 1); // UPDATE_AFTER

  ASSERT_TRUE(merge.hasResult()); // UPDATE_AFTER survives
  auto [resultBatch, resultRow] = merge.getResult();
  EXPECT_EQ(resultRow, 1);
}

TEST_F(PaimonMergeTest, deduplicateMergeUpdateBeforeFilters) {
  PaimonDeduplicateMerge merge(/*valueKindColumnIndex=*/2);

  auto batch = makeRowVector(
      {"_KEY_id", "_SEQ", "_VALUE_KIND"},
      {
          makeFlatVector<int64_t>({1}),
          makeFlatVector<int64_t>({1}),
          makeFlatVector<int8_t>({1}), // UPDATE_BEFORE only
      });

  merge.reset();
  merge.add(batch, 0);

  EXPECT_FALSE(merge.hasResult()); // UPDATE_BEFORE is retracted
}

TEST_F(PaimonMergeTest, deduplicateMergeNoData) {
  PaimonDeduplicateMerge merge(/*valueKindColumnIndex=*/2);

  merge.reset();
  EXPECT_FALSE(merge.hasResult());
}

// ===========================================================================
// PaimonSortMergeReader tests
// ===========================================================================

TEST_F(PaimonMergeTest, sortMergeReaderSingleRun) {
  // Single sorted run with 3 rows, no deduplication needed.
  // KV schema: [_KEY_id(BIGINT), _SEQUENCE_NUMBER(BIGINT),
  //             _VALUE_KIND(TINYINT), id(BIGINT), name(VARCHAR)]
  auto kvFile = writeKvFile(
      /*keyIds=*/{1, 2, 3},
      /*seqNums=*/{1, 1, 1},
      /*valueKinds=*/{0, 0, 0}, // all INSERT
      /*ids=*/{1, 2, 3},
      /*names=*/{"a", "b", "c"});

  auto kvSchema = ROW(
      {"_KEY_id", "_SEQUENCE_NUMBER", "_VALUE_KIND", "id", "name"},
      {BIGINT(), BIGINT(), TINYINT(), BIGINT(), VARCHAR()});

  PaimonDataFile fileInfo;
  fileInfo.path = kvFile->getPath();

  std::vector<std::unique_ptr<SortedRunReader>> readers;
  readers.push_back(std::make_unique<SortedRunReader>(
      std::vector<PaimonDataFile>{fileInfo},
      dwio::common::FileFormat::DWRF,
      kvSchema,
      pool()));

  auto comparator = std::make_shared<PaimonKeyComparator>(1, 1);
  auto mergeFunction = std::make_unique<PaimonDeduplicateMerge>(2);

  PaimonSortMergeReader mergeReader(
      std::move(readers), comparator, std::move(mergeFunction), kvSchema, pool());

  auto result = mergeReader.next(1024);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 3);

  // Verify key values.
  auto keyCol = result->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(keyCol->valueAt(0), 1);
  EXPECT_EQ(keyCol->valueAt(1), 2);
  EXPECT_EQ(keyCol->valueAt(2), 3);

  // Verify names.
  auto nameCol = result->childAt(4)->asFlatVector<StringView>();
  EXPECT_EQ(nameCol->valueAt(0), StringView("a"));
  EXPECT_EQ(nameCol->valueAt(1), StringView("b"));
  EXPECT_EQ(nameCol->valueAt(2), StringView("c"));

  // Should be exhausted.
  EXPECT_EQ(mergeReader.next(1024), nullptr);
}

TEST_F(PaimonMergeTest, sortMergeReaderDeduplication) {
  // Two sorted runs with overlapping keys.
  // Run 1: key=1 seq=1 INSERT "old1", key=2 seq=1 INSERT "old2"
  // Run 2: key=1 seq=2 INSERT "new1", key=3 seq=2 INSERT "new3"
  //
  // Expected after merge:
  // key=1 -> "new1" (higher seq wins)
  // key=2 -> "old2" (only in run 1)
  // key=3 -> "new3" (only in run 2)
  auto kvFile1 = writeKvFile(
      {1, 2}, {1, 1}, {0, 0}, {1, 2}, {"old1", "old2"});
  auto kvFile2 = writeKvFile(
      {1, 3}, {2, 2}, {0, 0}, {1, 3}, {"new1", "new3"});

  auto kvSchema = ROW(
      {"_KEY_id", "_SEQUENCE_NUMBER", "_VALUE_KIND", "id", "name"},
      {BIGINT(), BIGINT(), TINYINT(), BIGINT(), VARCHAR()});

  PaimonDataFile f1;
  f1.path = kvFile1->getPath();
  PaimonDataFile f2;
  f2.path = kvFile2->getPath();

  std::vector<std::unique_ptr<SortedRunReader>> readers;
  readers.push_back(std::make_unique<SortedRunReader>(
      std::vector<PaimonDataFile>{f1},
      dwio::common::FileFormat::DWRF,
      kvSchema,
      pool()));
  readers.push_back(std::make_unique<SortedRunReader>(
      std::vector<PaimonDataFile>{f2},
      dwio::common::FileFormat::DWRF,
      kvSchema,
      pool()));

  auto comparator = std::make_shared<PaimonKeyComparator>(1, 1);
  auto mergeFunction = std::make_unique<PaimonDeduplicateMerge>(2);

  PaimonSortMergeReader mergeReader(
      std::move(readers), comparator, std::move(mergeFunction), kvSchema, pool());

  auto result = mergeReader.next(1024);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 3);

  auto keyCol = result->childAt(0)->asFlatVector<int64_t>();
  auto nameCol = result->childAt(4)->asFlatVector<StringView>();

  // Results should be sorted by key: 1, 2, 3
  EXPECT_EQ(keyCol->valueAt(0), 1);
  EXPECT_EQ(nameCol->valueAt(0), StringView("new1")); // higher seq wins
  EXPECT_EQ(keyCol->valueAt(1), 2);
  EXPECT_EQ(nameCol->valueAt(1), StringView("old2"));
  EXPECT_EQ(keyCol->valueAt(2), 3);
  EXPECT_EQ(nameCol->valueAt(2), StringView("new3"));
}

TEST_F(PaimonMergeTest, sortMergeReaderDeleteFiltering) {
  // Run 1: key=1 seq=1 INSERT "val1", key=2 seq=1 INSERT "val2"
  // Run 2: key=1 seq=2 DELETE (valueKind=3)
  //
  // Expected after merge:
  // key=1 -> filtered out (DELETE)
  // key=2 -> "val2" (survives)
  auto kvFile1 = writeKvFile(
      {1, 2}, {1, 1}, {0, 0}, {1, 2}, {"val1", "val2"});
  auto kvFile2 = writeKvFile(
      {1}, {2}, {3}, {1}, {"deleted"}); // DELETE

  auto kvSchema = ROW(
      {"_KEY_id", "_SEQUENCE_NUMBER", "_VALUE_KIND", "id", "name"},
      {BIGINT(), BIGINT(), TINYINT(), BIGINT(), VARCHAR()});

  PaimonDataFile f1;
  f1.path = kvFile1->getPath();
  PaimonDataFile f2;
  f2.path = kvFile2->getPath();

  std::vector<std::unique_ptr<SortedRunReader>> readers;
  readers.push_back(std::make_unique<SortedRunReader>(
      std::vector<PaimonDataFile>{f1},
      dwio::common::FileFormat::DWRF,
      kvSchema,
      pool()));
  readers.push_back(std::make_unique<SortedRunReader>(
      std::vector<PaimonDataFile>{f2},
      dwio::common::FileFormat::DWRF,
      kvSchema,
      pool()));

  auto comparator = std::make_shared<PaimonKeyComparator>(1, 1);
  auto mergeFunction = std::make_unique<PaimonDeduplicateMerge>(2);

  PaimonSortMergeReader mergeReader(
      std::move(readers), comparator, std::move(mergeFunction), kvSchema, pool());

  auto result = mergeReader.next(1024);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1); // Only key=2 survives.

  auto keyCol = result->childAt(0)->asFlatVector<int64_t>();
  auto nameCol = result->childAt(4)->asFlatVector<StringView>();
  EXPECT_EQ(keyCol->valueAt(0), 2);
  EXPECT_EQ(nameCol->valueAt(0), StringView("val2"));
}

TEST_F(PaimonMergeTest, sortMergeReaderMultiRunSameKey) {
  // Three sorted runs, all with key=1 at different sequence numbers.
  // Run 1: key=1 seq=1 INSERT "v1"
  // Run 2: key=1 seq=2 INSERT "v2"
  // Run 3: key=1 seq=3 INSERT "v3"
  //
  // Expected: key=1 -> "v3" (highest seq)
  auto kvFile1 = writeKvFile({1}, {1}, {0}, {1}, {"v1"});
  auto kvFile2 = writeKvFile({1}, {2}, {0}, {1}, {"v2"});
  auto kvFile3 = writeKvFile({1}, {3}, {0}, {1}, {"v3"});

  auto kvSchema = ROW(
      {"_KEY_id", "_SEQUENCE_NUMBER", "_VALUE_KIND", "id", "name"},
      {BIGINT(), BIGINT(), TINYINT(), BIGINT(), VARCHAR()});

  PaimonDataFile f1, f2, f3;
  f1.path = kvFile1->getPath();
  f2.path = kvFile2->getPath();
  f3.path = kvFile3->getPath();

  std::vector<std::unique_ptr<SortedRunReader>> readers;
  readers.push_back(std::make_unique<SortedRunReader>(
      std::vector<PaimonDataFile>{f1},
      dwio::common::FileFormat::DWRF,
      kvSchema, pool()));
  readers.push_back(std::make_unique<SortedRunReader>(
      std::vector<PaimonDataFile>{f2},
      dwio::common::FileFormat::DWRF,
      kvSchema, pool()));
  readers.push_back(std::make_unique<SortedRunReader>(
      std::vector<PaimonDataFile>{f3},
      dwio::common::FileFormat::DWRF,
      kvSchema, pool()));

  auto comparator = std::make_shared<PaimonKeyComparator>(1, 1);
  auto mergeFunction = std::make_unique<PaimonDeduplicateMerge>(2);

  PaimonSortMergeReader mergeReader(
      std::move(readers), comparator, std::move(mergeFunction), kvSchema, pool());

  auto result = mergeReader.next(1024);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  auto nameCol = result->childAt(4)->asFlatVector<StringView>();
  EXPECT_EQ(nameCol->valueAt(0), StringView("v3"));
}

TEST_F(PaimonMergeTest, sortMergeReaderAllDeleted) {
  // All keys are deleted after merge.
  auto kvFile1 = writeKvFile({1, 2}, {1, 1}, {0, 0}, {1, 2}, {"a", "b"});
  auto kvFile2 = writeKvFile({1, 2}, {2, 2}, {3, 3}, {1, 2}, {"x", "y"});

  auto kvSchema = ROW(
      {"_KEY_id", "_SEQUENCE_NUMBER", "_VALUE_KIND", "id", "name"},
      {BIGINT(), BIGINT(), TINYINT(), BIGINT(), VARCHAR()});

  PaimonDataFile f1, f2;
  f1.path = kvFile1->getPath();
  f2.path = kvFile2->getPath();

  std::vector<std::unique_ptr<SortedRunReader>> readers;
  readers.push_back(std::make_unique<SortedRunReader>(
      std::vector<PaimonDataFile>{f1},
      dwio::common::FileFormat::DWRF,
      kvSchema, pool()));
  readers.push_back(std::make_unique<SortedRunReader>(
      std::vector<PaimonDataFile>{f2},
      dwio::common::FileFormat::DWRF,
      kvSchema, pool()));

  auto comparator = std::make_shared<PaimonKeyComparator>(1, 1);
  auto mergeFunction = std::make_unique<PaimonDeduplicateMerge>(2);

  PaimonSortMergeReader mergeReader(
      std::move(readers), comparator, std::move(mergeFunction), kvSchema, pool());

  auto result = mergeReader.next(1024);
  EXPECT_EQ(result, nullptr); // All rows deleted.
}
