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

#include "velox/connectors/hive/paimon/merge/PaimonMergeReader.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/InputStream.h"
#include "velox/dwio/common/ReaderFactory.h"

namespace facebook::velox::connector::hive::paimon {

PaimonMergeReader::PaimonMergeReader(
    std::shared_ptr<const PaimonConnectorSplit> paimonSplit,
    const RowTypePtr& outputType,
    memory::MemoryPool* pool)
    : paimonSplit_(std::move(paimonSplit)),
      outputType_(outputType),
      pool_(pool) {
  VELOX_CHECK(
      !paimonSplit_->dataFiles().empty(),
      "PK merge split must have data files");
  VELOX_CHECK(
      !paimonSplit_->primaryKeys().empty(),
      "PK merge split must have primary keys");

  buildKvSchema();
  initializeSections();

  if (!sections_.empty()) {
    openSection(0);
  }
}

void PaimonMergeReader::buildKvSchema() {
  // Peek at the first file's schema to discover _KEY_* column types.
  const auto& firstFile = paimonSplit_->dataFiles().front();
  auto fs = filesystems::getFileSystem(firstFile.path, nullptr);
  auto readFile = fs->openFileForRead(firstFile.path);

  dwio::common::ReaderOptions readerOptions(pool_);
  readerOptions.setFileFormat(paimonSplit_->fileFormat());

  auto inputStream =
      std::make_unique<dwio::common::ReadFileInputStream>(std::move(readFile));
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::move(inputStream), *pool_);

  auto reader = dwio::common::getReaderFactory(paimonSplit_->fileFormat())
                    ->createReader(std::move(input), readerOptions);
  auto fileSchema = reader->rowType();

  // Build KV schema columns.
  std::vector<std::string> names;
  std::vector<TypePtr> types;

  // 1. _KEY_* columns for each primary key.
  const auto& primaryKeys = paimonSplit_->primaryKeys();
  for (const auto& pk : primaryKeys) {
    std::string keyColName = "_KEY_" + pk;
    if (fileSchema->containsChild(keyColName)) {
      names.push_back(keyColName);
      types.push_back(fileSchema->findChild(keyColName));
    } else {
      // Fallback: try to get type from outputType or file schema.
      auto outIdx = outputType_->getChildIdxIfExists(pk);
      if (outIdx.has_value()) {
        names.push_back(keyColName);
        types.push_back(outputType_->childAt(*outIdx));
      } else {
        VELOX_CHECK(
            fileSchema->containsChild(pk),
            "Cannot determine type for primary key column '{}' "
            "(not found as _KEY_{} or {} in file schema, "
            "and not in output type)",
            pk,
            pk,
            pk);
        names.push_back(keyColName);
        types.push_back(fileSchema->findChild(pk));
      }
    }
  }
  numKeyColumns_ = names.size();

  // 2. _SEQUENCE_NUMBER (BIGINT).
  seqColumnIndex_ = names.size();
  names.push_back("_SEQUENCE_NUMBER");
  types.push_back(BIGINT());

  // 3. _VALUE_KIND (TINYINT).
  valueKindColumnIndex_ = names.size();
  names.push_back("_VALUE_KIND");
  types.push_back(TINYINT());

  // 4. User output columns (only regular columns, not partition keys).
  const auto& splitPartitionKeys = paimonSplit_->partitionKeys();
  outputColumnToKvIndex_.resize(outputType_->size(), -1);
  for (size_t i = 0; i < outputType_->size(); i++) {
    const auto& colName = outputType_->nameOf(i);
    // Check if this is a partition key column.
    if (splitPartitionKeys.count(colName) > 0) {
      // Partition key — will be filled from split metadata, not read from file.
      outputColumnToKvIndex_[i] = -1;
    } else {
      outputColumnToKvIndex_[i] = static_cast<int32_t>(names.size());
      names.push_back(colName);
      types.push_back(outputType_->childAt(i));
    }
  }

  kvSchema_ = ROW(std::move(names), std::move(types));
}

void PaimonMergeReader::initializeSections() {
  sections_ = PaimonIntervalPartition::partition(paimonSplit_->dataFiles());
}

bool PaimonMergeReader::openSection(size_t sectionIdx) {
  if (sectionIdx >= sections_.size()) {
    currentMergeReader_.reset();
    return false;
  }

  const auto& section = sections_[sectionIdx];

  // Create a SortedRunReader for each sorted run in the section.
  std::vector<std::unique_ptr<SortedRunReader>> runReaders;
  runReaders.reserve(section.sortedRuns.size());

  for (const auto& run : section.sortedRuns) {
    runReaders.push_back(std::make_unique<SortedRunReader>(
        run.files, paimonSplit_->fileFormat(), kvSchema_, pool_));
  }

  // Create the comparator.
  auto comparator =
      std::make_shared<PaimonKeyComparator>(numKeyColumns_, seqColumnIndex_);

  // Create the merge function based on merge engine.
  std::unique_ptr<PaimonMergeFunction> mergeFunction;
  if (paimonSplit_->mergeEngine() == "deduplicate") {
    mergeFunction =
        std::make_unique<PaimonDeduplicateMerge>(valueKindColumnIndex_);
  } else {
    VELOX_NYI(
        "Merge engine '{}' is not yet supported. "
        "Only 'deduplicate' is currently implemented.",
        paimonSplit_->mergeEngine());
  }

  currentMergeReader_ = std::make_unique<PaimonSortMergeReader>(
      std::move(runReaders),
      std::move(comparator),
      std::move(mergeFunction),
      kvSchema_,
      pool_);

  return true;
}

RowVectorPtr PaimonMergeReader::next(uint64_t batchSize) {
  while (currentSectionIdx_ < sections_.size()) {
    if (!currentMergeReader_) {
      if (!openSection(currentSectionIdx_)) {
        break;
      }
    }

    auto kvBatch = currentMergeReader_->next(batchSize);
    if (kvBatch) {
      auto projected = projectOutputColumns(kvBatch);
      completedRows_ += projected->size();
      return projected;
    }

    // Current section exhausted, move to next.
    currentMergeReader_.reset();
    currentSectionIdx_++;
  }
  return nullptr;
}

RowVectorPtr PaimonMergeReader::projectOutputColumns(
    const RowVectorPtr& kvBatch) {
  auto numRows = kvBatch->size();
  std::vector<VectorPtr> children(outputType_->size());

  const auto& splitPartitionKeys = paimonSplit_->partitionKeys();

  for (size_t i = 0; i < outputType_->size(); i++) {
    if (outputColumnToKvIndex_[i] >= 0) {
      // Regular column — copy from KV batch.
      children[i] = kvBatch->childAt(outputColumnToKvIndex_[i]);
    } else {
      // Partition key — fill from split metadata.
      const auto& colName = outputType_->nameOf(i);
      auto it = splitPartitionKeys.find(colName);
      if (it != splitPartitionKeys.end() && it->second.has_value()) {
        // Create a constant vector with the partition value, cast to the
        // correct output type. Partition values are serialized as strings
        // by the coordinator, so create via VARCHAR variant and let
        // createConstant handle the type.
        auto outputColType = outputType_->childAt(i);
        children[i] = BaseVector::createConstant(
            outputColType,
            variant::create<TypeKind::VARCHAR>(StringView(it->second.value())),
            numRows,
            pool_);
      } else {
        children[i] = BaseVector::createNullConstant(
            outputType_->childAt(i), numRows, pool_);
      }
    }
  }

  return std::make_shared<RowVector>(
      pool_, outputType_, BufferPtr(), numRows, std::move(children));
}

} // namespace facebook::velox::connector::hive::paimon
