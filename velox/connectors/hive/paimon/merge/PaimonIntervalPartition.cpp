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

#include "velox/connectors/hive/paimon/merge/PaimonIntervalPartition.h"
#include "velox/connectors/hive/paimon/merge/PaimonKeyComparator.h"

#include <algorithm>
#include <numeric>
#include <queue>

namespace facebook::velox::connector::hive::paimon {

namespace {

bool allFilesHaveKeyStats(const std::vector<PaimonDataFile>& files) {
  for (const auto& f : files) {
    if (!f.minKey.has_value() || !f.maxKey.has_value()) {
      return false;
    }
  }
  return true;
}

std::vector<Section> fallbackPartition(
    const std::vector<PaimonDataFile>& files) {
  Section section;
  for (const auto& file : files) {
    SortedRun run;
    run.files.push_back(file);
    section.sortedRuns.push_back(std::move(run));
  }
  return {std::move(section)};
}

} // namespace

/*static*/ std::vector<Section> PaimonIntervalPartition::partition(
    const std::vector<PaimonDataFile>& files) {
  if (files.empty()) {
    return {};
  }

  if (files.size() == 1) {
    Section section;
    SortedRun run;
    run.files.push_back(files[0]);
    section.sortedRuns.push_back(std::move(run));
    return {std::move(section)};
  }

  if (!allFilesHaveKeyStats(files)) {
    return fallbackPartition(files);
  }

  // Sort files by (minKey, maxKey) ascending.
  std::vector<size_t> indices(files.size());
  std::iota(indices.begin(), indices.end(), 0);
  std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
    int cmp = PaimonKeyComparator::compareKeyBytes(
        *files[a].minKey, *files[b].minKey);
    if (cmp != 0) {
      return cmp < 0;
    }
    return PaimonKeyComparator::compareKeyBytes(
               *files[a].maxKey, *files[b].maxKey) < 0;
  });

  // Phase 1: Section detection using sweep line.
  std::vector<std::vector<size_t>> sectionFileIndices;
  sectionFileIndices.emplace_back();
  std::string currentBound = *files[indices[0]].maxKey;

  sectionFileIndices.back().push_back(indices[0]);

  for (size_t i = 1; i < indices.size(); ++i) {
    size_t fileIdx = indices[i];
    const auto& file = files[fileIdx];

    if (PaimonKeyComparator::compareKeyBytes(*file.minKey, currentBound) > 0) {
      sectionFileIndices.emplace_back();
      currentBound = *file.maxKey;
    } else {
      if (PaimonKeyComparator::compareKeyBytes(*file.maxKey, currentBound) >
          0) {
        currentBound = *file.maxKey;
      }
    }
    sectionFileIndices.back().push_back(fileIdx);
  }

  // Phase 2: Greedy sorted run assignment within each section.
  std::vector<Section> sections;
  sections.reserve(sectionFileIndices.size());

  for (auto& fileIndicesInSection : sectionFileIndices) {
    Section section;

    using HeapEntry = std::pair<std::string, size_t>;
    auto heapCmp = [](const HeapEntry& a, const HeapEntry& b) {
      return PaimonKeyComparator::compareKeyBytes(a.first, b.first) > 0;
    };
    std::priority_queue<HeapEntry, std::vector<HeapEntry>, decltype(heapCmp)>
        heap(heapCmp);

    for (size_t fileIdx : fileIndicesInSection) {
      const auto& file = files[fileIdx];

      if (!heap.empty()) {
        auto top = heap.top();
        if (PaimonKeyComparator::compareKeyBytes(top.first, *file.minKey) < 0) {
          heap.pop();
          section.sortedRuns[top.second].files.push_back(file);
          heap.push({*file.maxKey, top.second});
          continue;
        }
      }

      size_t runIdx = section.sortedRuns.size();
      SortedRun run;
      run.files.push_back(file);
      section.sortedRuns.push_back(std::move(run));
      heap.push({*file.maxKey, runIdx});
    }

    sections.push_back(std::move(section));
  }

  return sections;
}

} // namespace facebook::velox::connector::hive::paimon
