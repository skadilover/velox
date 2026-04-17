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

#include "velox/connectors/hive/paimon/merge/PaimonSortMergeReader.h"

#include <algorithm>
#include <unordered_map>

#include "velox/common/base/Exceptions.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/InputStream.h"
#include "velox/dwio/common/ReaderFactory.h"

namespace facebook::velox::connector::hive::paimon {

// ============================================================================
// SortedRunReader
// ============================================================================

SortedRunReader::SortedRunReader(
    std::vector<PaimonDataFile> files,
    dwio::common::FileFormat format,
    const RowTypePtr& kvSchema,
    memory::MemoryPool* pool)
    : files_(std::move(files)),
      format_(format),
      kvSchema_(kvSchema),
      pool_(pool) {
  VELOX_CHECK(!files_.empty(), "SortedRunReader requires at least one file");
  openFile(0);
  readNextBatch();
}

void SortedRunReader::openFile(size_t idx) {
  currentReader_.reset();
  currentRowReader_.reset();

  if (idx >= files_.size()) {
    return;
  }

  const auto& filePath = files_[idx].path;
  auto fs = filesystems::getFileSystem(filePath, nullptr);
  auto readFile = fs->openFileForRead(filePath);

  dwio::common::ReaderOptions readerOptions(pool_);
  readerOptions.setFileFormat(format_);

  auto inputStream =
      std::make_unique<dwio::common::ReadFileInputStream>(std::move(readFile));
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::move(inputStream), *pool_);

  currentReader_ = dwio::common::getReaderFactory(format_)->createReader(
      std::move(input), readerOptions);

  // Filter read columns to those in the KV schema that exist in the file.
  auto fileType = currentReader_->rowType();
  std::vector<std::string> validColumns;
  for (size_t i = 0; i < kvSchema_->size(); i++) {
    const auto& colName = kvSchema_->nameOf(i);
    if (fileType->containsChild(colName)) {
      validColumns.push_back(colName);
    }
  }

  dwio::common::RowReaderOptions rowReaderOptions;
  if (!validColumns.empty()) {
    rowReaderOptions.select(
        std::make_shared<dwio::common::ColumnSelector>(
            fileType, validColumns));
  }

  currentRowReader_ = currentReader_->createRowReader(rowReaderOptions);
}

bool SortedRunReader::readNextBatch() {
  while (currentRowReader_) {
    VectorPtr output;
    auto rowsRead = currentRowReader_->next(1024, output);
    if (rowsRead > 0 && output) {
      auto fileBatch = std::dynamic_pointer_cast<RowVector>(output);
      VELOX_CHECK_NOT_NULL(fileBatch);
      currentBatch_ = reorderToKvSchema(fileBatch);
      currentRow_ = 0;
      return true;
    }
    // Current file exhausted, try next file.
    currentReader_.reset();
    currentRowReader_.reset();
    currentFileIdx_++;
    if (currentFileIdx_ < files_.size()) {
      openFile(currentFileIdx_);
    }
  }
  currentBatch_ = nullptr;
  return false;
}

RowVectorPtr SortedRunReader::reorderToKvSchema(
    const RowVectorPtr& fileBatch) {
  auto& inputType = fileBatch->type()->asRow();
  std::vector<VectorPtr> children(kvSchema_->size());
  for (size_t i = 0; i < kvSchema_->size(); i++) {
    auto idx = inputType.getChildIdxIfExists(kvSchema_->nameOf(i));
    if (idx.has_value()) {
      children[i] = fileBatch->childAt(*idx);
    } else {
      // Column not in file (schema evolution) - fill with nulls.
      children[i] = BaseVector::createNullConstant(
          kvSchema_->childAt(i), fileBatch->size(), pool_);
    }
  }
  return std::make_shared<RowVector>(
      pool_, kvSchema_, BufferPtr(), fileBatch->size(), std::move(children));
}

bool SortedRunReader::advance() {
  if (!currentBatch_) {
    return false;
  }
  currentRow_++;
  if (currentRow_ < currentBatch_->size()) {
    return true;
  }
  // Current batch exhausted, read next batch.
  return readNextBatch();
}

// ============================================================================
// PaimonSortMergeReader
// ============================================================================

PaimonSortMergeReader::PaimonSortMergeReader(
    std::vector<std::unique_ptr<SortedRunReader>> runReaders,
    std::shared_ptr<PaimonKeyComparator> comparator,
    std::unique_ptr<PaimonMergeFunction> mergeFunction,
    const RowTypePtr& kvSchema,
    memory::MemoryPool* pool)
    : runReaders_(std::move(runReaders)),
      comparator_(std::move(comparator)),
      mergeFunction_(std::move(mergeFunction)),
      kvSchema_(kvSchema),
      pool_(pool) {}

void PaimonSortMergeReader::initializeHeap() {
  heap_.clear();
  for (size_t i = 0; i < runReaders_.size(); i++) {
    if (runReaders_[i]->hasData()) {
      heap_.push_back({i});
    }
  }
  auto cmp = [this](const HeapEntry& a, const HeapEntry& b) {
    return heapCompare(a, b);
  };
  std::make_heap(heap_.begin(), heap_.end(), cmp);
  initialized_ = true;
}

bool PaimonSortMergeReader::heapCompare(
    const HeapEntry& a,
    const HeapEntry& b) const {
  // std::*_heap is a max-heap by default.
  // Return true if a > b to get min-heap behavior (smallest at top).
  return comparator_->compareKeysAndSequence(
             runReaders_[a.runIndex]->currentBatch(),
             runReaders_[a.runIndex]->currentRow(),
             runReaders_[b.runIndex]->currentBatch(),
             runReaders_[b.runIndex]->currentRow()) > 0;
}

RowVectorPtr PaimonSortMergeReader::buildOutputBatch(
    const std::vector<RowRef>& rows) {
  auto numRows = static_cast<vector_size_t>(rows.size());
  if (numRows == 0) {
    return nullptr;
  }

  // Group output rows by their source batch for efficient batch-level copy.
  // This reduces O(rows * cols) individual copy calls to
  // O(distinctBatches * cols) batch copy calls.
  std::unordered_map<
      const RowVector*,
      std::vector<std::pair<vector_size_t, vector_size_t>>>
      batchGroups; // batch -> [(targetIndex, sourceIndex)]
  for (vector_size_t i = 0; i < numRows; i++) {
    batchGroups[rows[i].batch.get()].emplace_back(i, rows[i].row);
  }

  // Pre-allocate reusable buffers for batch copy operations.
  SelectivityVector targetRows(numRows, false);
  std::vector<vector_size_t> toSourceRow(numRows);

  std::vector<VectorPtr> columns(kvSchema_->size());
  for (size_t col = 0; col < kvSchema_->size(); col++) {
    auto type = kvSchema_->childAt(col);
    auto column = BaseVector::create(type, numRows, pool_);

    for (const auto& [batchPtr, mappings] : batchGroups) {
      auto* sourceCol = batchPtr->childAt(col).get();
      targetRows.clearAll();
      for (const auto& [targetIdx, sourceIdx] : mappings) {
        targetRows.setValid(targetIdx, true);
        toSourceRow[targetIdx] = sourceIdx;
      }
      targetRows.updateBounds();
      column->copy(sourceCol, targetRows, toSourceRow.data());
    }

    columns[col] = std::move(column);
  }
  return std::make_shared<RowVector>(
      pool_, kvSchema_, BufferPtr(), numRows, std::move(columns));
}

RowVectorPtr PaimonSortMergeReader::next(uint64_t batchSize) {
  if (!initialized_) {
    initializeHeap();
  }

  if (heap_.empty()) {
    return nullptr;
  }

  std::vector<RowRef> outputRows;
  outputRows.reserve(batchSize);

  // Pre-allocate polled vector to avoid repeated small allocations.
  std::vector<size_t> polled;

  auto heapCmp = [this](const HeapEntry& a, const HeapEntry& b) {
    return heapCompare(a, b);
  };

  while (!heap_.empty() &&
         outputRows.size() < static_cast<size_t>(batchSize)) {
    // Pop the minimum entry (smallest key+seq).
    std::pop_heap(heap_.begin(), heap_.end(), heapCmp);
    auto minEntry = heap_.back();
    heap_.pop_back();

    auto& minReader = runReaders_[minEntry.runIndex];

    // Save the current key reference for grouping.
    auto keyBatch = minReader->currentBatch();
    auto keyRow = minReader->currentRow();

    // Start a new key group.
    mergeFunction_->reset();
    mergeFunction_->add(keyBatch, keyRow);

    // Track entries to advance later.
    polled.clear();
    polled.push_back(minEntry.runIndex);

    // Pop all entries with the same key (key-only comparison).
    while (!heap_.empty()) {
      auto& topReader = runReaders_[heap_[0].runIndex];

      if (comparator_->compareKeys(
              keyBatch,
              keyRow,
              topReader->currentBatch(),
              topReader->currentRow()) != 0) {
        break; // Different key, stop grouping.
      }

      // Same key: pop and add to merge function.
      std::pop_heap(heap_.begin(), heap_.end(), heapCmp);
      auto entry = heap_.back();
      heap_.pop_back();

      auto& entryReader = runReaders_[entry.runIndex];
      mergeFunction_->add(
          entryReader->currentBatch(), entryReader->currentRow());
      polled.push_back(entry.runIndex);
    }

    // Get merge result.
    if (mergeFunction_->hasResult()) {
      auto [resultBatch, resultRow] = mergeFunction_->getResult();
      outputRows.push_back({resultBatch, resultRow});
    }

    // Advance all polled entries and re-insert if they have more data.
    for (auto runIdx : polled) {
      auto& reader = runReaders_[runIdx];
      reader->advance();
      if (reader->hasData()) {
        heap_.push_back({runIdx});
        std::push_heap(heap_.begin(), heap_.end(), heapCmp);
      }
    }
  }

  if (outputRows.empty()) {
    return nullptr;
  }

  auto result = buildOutputBatch(outputRows);
  completedRows_ += result->size();
  return result;
}

} // namespace facebook::velox::connector::hive::paimon
