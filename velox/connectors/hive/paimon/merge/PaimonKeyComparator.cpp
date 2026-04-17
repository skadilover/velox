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

#include "velox/connectors/hive/paimon/merge/PaimonKeyComparator.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::connector::hive::paimon {

PaimonKeyComparator::PaimonKeyComparator(
    size_t numKeyColumns,
    size_t seqColumnIndex)
    : numKeyColumns_(numKeyColumns), seqColumnIndex_(seqColumnIndex) {
  VELOX_CHECK_GT(numKeyColumns_, 0, "Must have at least one key column");
  flags_.nullsFirst = true;
  flags_.ascending = true;
  flags_.nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue;
}

int32_t PaimonKeyComparator::compareKeys(
    const RowVectorPtr& batchA,
    vector_size_t idxA,
    const RowVectorPtr& batchB,
    vector_size_t idxB) const {
  for (size_t col = 0; col < numKeyColumns_; ++col) {
    auto result = batchA->childAt(col)->compare(
        batchB->childAt(col).get(), idxA, idxB, flags_);
    VELOX_CHECK(
        result.has_value(),
        "Key column comparison returned nullopt at column {}", col);
    if (result.value() != 0) {
      return result.value();
    }
  }
  return 0;
}

int32_t PaimonKeyComparator::compareKeysAndSequence(
    const RowVectorPtr& batchA,
    vector_size_t idxA,
    const RowVectorPtr& batchB,
    vector_size_t idxB) const {
  auto keyResult = compareKeys(batchA, idxA, batchB, idxB);
  if (keyResult != 0) {
    return keyResult;
  }
  auto seqResult = batchA->childAt(seqColumnIndex_)
                       ->compare(
                           batchB->childAt(seqColumnIndex_).get(),
                           idxA,
                           idxB,
                           flags_);
  VELOX_CHECK(
      seqResult.has_value(),
      "Sequence number comparison returned nullopt");
  return seqResult.value();
}

/*static*/ int32_t PaimonKeyComparator::compareKeyBytes(
    const std::string& keyA,
    const std::string& keyB) {
  int cmp = keyA.compare(keyB);
  if (cmp < 0) {
    return -1;
  } else if (cmp > 0) {
    return 1;
  }
  return 0;
}

} // namespace facebook::velox::connector::hive::paimon
