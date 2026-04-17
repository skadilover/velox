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

#include "velox/connectors/hive/paimon/PaimonRowKind.h"
#include "velox/common/base/Exceptions.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::connector::hive::paimon {

/// Interface for Paimon merge functions that combine rows with the same key.
///
/// During merge-on-read, when multiple sorted runs contain rows with the same
/// primary key, a MergeFunction determines which row(s) to output.
///
/// Rows are added in sequence_number ASC order (guaranteed by the min-heap
/// merge), so the last add() call receives the newest version.
class PaimonMergeFunction {
 public:
  virtual ~PaimonMergeFunction() = default;

  /// Reset state for a new key group.
  virtual void reset() = 0;

  /// Add a row to the current key group.
  /// @param kvBatch The KV-schema batch containing the row.
  /// @param rowIndex Index of the row within the batch.
  virtual void add(const RowVectorPtr& kvBatch, vector_size_t rowIndex) = 0;

  /// Whether a valid result exists after processing the key group.
  /// For Deduplicate, returns false if the winning row is DELETE/RETRACT.
  virtual bool hasResult() const = 0;

  /// Get the result row reference (batch + index).
  /// Only valid when hasResult() returns true.
  virtual std::pair<RowVectorPtr, vector_size_t> getResult() const = 0;
};

/// Deduplicate merge function: keeps the row with the highest sequence number.
///
/// Since rows arrive in sequence ASC order (guaranteed by the min-heap),
/// the last add() call receives the newest row, so we simply overwrite.
///
/// After merge, if the winning row has _VALUE_KIND = DELETE or UPDATE_BEFORE,
/// hasResult() returns false (the row is filtered out).
class PaimonDeduplicateMerge : public PaimonMergeFunction {
 public:
  explicit PaimonDeduplicateMerge(size_t valueKindColumnIndex)
      : valueKindColumnIndex_(valueKindColumnIndex) {}

  void reset() override {
    hasData_ = false;
    latestBatch_ = nullptr;
  }

  void add(const RowVectorPtr& kvBatch, vector_size_t rowIndex) override {
    latestBatch_ = kvBatch;
    latestIndex_ = rowIndex;
    hasData_ = true;
  }

  bool hasResult() const override {
    if (!hasData_) {
      return false;
    }
    // Filter out DELETE and UPDATE_BEFORE rows.
    auto kindVector = latestBatch_->childAt(valueKindColumnIndex_)
                          ->asFlatVector<int8_t>();
    VELOX_CHECK_NOT_NULL(
        kindVector,
        "_VALUE_KIND column is not a FlatVector<int8_t>");
    auto kind = static_cast<PaimonRowKind>(kindVector->valueAt(latestIndex_));
    return kind == PaimonRowKind::kInsert ||
        kind == PaimonRowKind::kUpdateAfter;
  }

  std::pair<RowVectorPtr, vector_size_t> getResult() const override {
    return {latestBatch_, latestIndex_};
  }

 private:
  size_t valueKindColumnIndex_;
  RowVectorPtr latestBatch_;
  vector_size_t latestIndex_{0};
  bool hasData_{false};
};

} // namespace facebook::velox::connector::hive::paimon
