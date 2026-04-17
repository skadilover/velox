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

#include "velox/connectors/hive/paimon/PaimonConnectorSplit.h"
#include "velox/connectors/hive/paimon/merge/PaimonIntervalPartition.h"
#include "velox/connectors/hive/paimon/merge/PaimonSortMergeReader.h"

namespace facebook::velox::connector::hive::paimon {

/// Orchestrates merge-on-read for primary-key tables with rawConvertible=false.
///
/// Pipeline:
///   1. Build KV schema from file schema + primary keys
///   2. IntervalPartition: group files into sections with sorted runs
///   3. For each section: create SortMergeReader (k-way merge + dedup)
///   4. Project output: strip system columns, extract user columns
///
/// Sections are processed sequentially (their key ranges don't overlap).
class PaimonMergeReader {
 public:
  PaimonMergeReader(
      std::shared_ptr<const PaimonConnectorSplit> paimonSplit,
      const RowTypePtr& outputType,
      memory::MemoryPool* pool);

  /// Get the next batch of merged and projected rows.
  /// Returns nullptr when all sections are exhausted.
  RowVectorPtr next(uint64_t batchSize);

  uint64_t getCompletedRows() const {
    return completedRows_;
  }

 private:
  /// Discover file schema and build the KV read schema.
  void buildKvSchema();

  /// Run IntervalPartition on the data files.
  void initializeSections();

  /// Create a SortMergeReader for the given section.
  bool openSection(size_t sectionIdx);

  /// Extract user columns from a KV batch and fill partition values.
  RowVectorPtr projectOutputColumns(const RowVectorPtr& kvBatch);

  std::shared_ptr<const PaimonConnectorSplit> paimonSplit_;
  RowTypePtr outputType_;
  memory::MemoryPool* pool_;

  // KV schema and column indices.
  RowTypePtr kvSchema_;
  size_t numKeyColumns_{0};
  size_t seqColumnIndex_{0};
  size_t valueKindColumnIndex_{0};

  // Maps output column index -> KV schema column index.
  // -1 indicates a partition key column (not read from files).
  std::vector<int32_t> outputColumnToKvIndex_;

  // Sections from IntervalPartition.
  std::vector<Section> sections_;
  size_t currentSectionIdx_{0};
  std::unique_ptr<PaimonSortMergeReader> currentMergeReader_;

  uint64_t completedRows_{0};
};

} // namespace facebook::velox::connector::hive::paimon
