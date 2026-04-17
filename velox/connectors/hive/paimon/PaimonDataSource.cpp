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
#include "velox/connectors/hive/paimon/PaimonDataSource.h"

#include "velox/common/Casts.h"
#include "velox/connectors/hive/paimon/PaimonConnectorSplit.h"
#include "velox/connectors/hive/paimon/PaimonSplitReader.h"
#include "velox/connectors/hive/paimon/merge/PaimonMergeReader.h"

namespace facebook::velox::connector::hive::paimon {

PaimonDataSource::PaimonDataSource(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const ColumnHandleMap& assignments,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* ioExecutor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<PaimonConfig>& paimonConfig)
    : FileDataSource(
          outputType,
          tableHandle,
          assignments,
          fileHandleFactory,
          ioExecutor,
          connectorQueryCtx,
          paimonConfig),
      mergeOutputType_(outputType) {}

PaimonDataSource::~PaimonDataSource() = default;

void PaimonDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  paimonSplit_ = checkedPointerCast<PaimonConnectorSplit>(split);

  if (!paimonSplit_->rawConvertible()) {
    // Merge path: create a PaimonMergeReader that handles IntervalPartition,
    // k-way merge, and deduplication.
    mergeReader_ = std::make_unique<PaimonMergeReader>(
        paimonSplit_, mergeOutputType_, pool_);
    return;
  }

  // Raw path: create a FileConnectorSplit for the first data file and delegate
  // to FileDataSource::addSplit(), which calls createSplitReader() to create
  // a PaimonSplitReader that handles all files internally.
  const auto& firstFile = paimonSplit_->dataFiles().front();
  auto firstFileSplit = std::make_shared<FileConnectorSplit>(
      paimonSplit_->connectorId,
      firstFile.path,
      paimonSplit_->fileFormat(),
      /*_start=*/0,
      /*_length=*/std::numeric_limits<uint64_t>::max(),
      /*splitWeight=*/0,
      /*cacheable=*/true,
      /*_properties=*/std::nullopt,
      paimonSplit_->partitionKeys());

  FileDataSource::addSplit(std::move(firstFileSplit));
}

std::optional<RowVectorPtr> PaimonDataSource::next(
    uint64_t size,
    velox::ContinueFuture& future) {
  if (mergeReader_) {
    auto result = mergeReader_->next(size);
    if (!result || result->size() == 0) {
      return nullptr;
    }
    mergeCompletedRows_ += result->size();
    return result;
  }
  return FileDataSource::next(size, future);
}

uint64_t PaimonDataSource::getCompletedRows() {
  if (mergeReader_) {
    return mergeCompletedRows_;
  }
  return FileDataSource::getCompletedRows();
}

std::unique_ptr<FileSplitReader> PaimonDataSource::createSplitReader() {
  return std::make_unique<PaimonSplitReader>(
      split_,
      paimonSplit_,
      tableHandle_,
      &partitionKeys_,
      connectorQueryCtx_,
      fileConfig_,
      readerOutputType_,
      ioStatistics_,
      ioStats_,
      fileHandleFactory_,
      ioExecutor_,
      scanSpec_);
}

} // namespace facebook::velox::connector::hive::paimon
