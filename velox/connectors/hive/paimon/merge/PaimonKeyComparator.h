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

#include "velox/common/base/CompareFlags.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::connector::hive::paimon {

/// Comparator for Paimon primary key columns within RowVector batches.
///
/// Used by PaimonSortMergeReader to maintain sorted order during k-way merge.
/// Compares rows by their key columns (the first numKeyColumns columns of the
/// KV schema), optionally followed by _SEQUENCE_NUMBER for total ordering.
///
/// The comparison order matches Paimon's sort contract:
///   1. Primary key columns: ASC, nulls-first (column-by-column)
///   2. _SEQUENCE_NUMBER: ASC (for total ordering within same key)
class PaimonKeyComparator {
 public:
  PaimonKeyComparator(size_t numKeyColumns, size_t seqColumnIndex);

  /// Compare only the primary key columns of two rows.
  /// Returns <0 if rowA < rowB, 0 if equal, >0 if rowA > rowB.
  int32_t compareKeys(
      const RowVectorPtr& batchA,
      vector_size_t idxA,
      const RowVectorPtr& batchB,
      vector_size_t idxB) const;

  /// Compare primary key columns + _SEQUENCE_NUMBER for total ordering.
  int32_t compareKeysAndSequence(
      const RowVectorPtr& batchA,
      vector_size_t idxA,
      const RowVectorPtr& batchB,
      vector_size_t idxB) const;

  /// Compare two binary-encoded key statistics (from PaimonDataFile
  /// minKey/maxKey). Uses lexicographic byte comparison.
  static int32_t compareKeyBytes(
      const std::string& keyA,
      const std::string& keyB);

  size_t numKeyColumns() const {
    return numKeyColumns_;
  }

 private:
  size_t numKeyColumns_;
  size_t seqColumnIndex_;
  CompareFlags flags_;
};

} // namespace facebook::velox::connector::hive::paimon
