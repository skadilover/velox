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

#include <string>
#include <vector>
#include "velox/connectors/hive/paimon/PaimonDataFileMeta.h"

namespace facebook::velox::connector::hive::paimon {

/// A sorted run is a list of data files whose key ranges do not overlap.
struct SortedRun {
  std::vector<PaimonDataFile> files;
};

/// A section is a group of sorted runs whose combined key range does not
/// overlap with other sections.
struct Section {
  std::vector<SortedRun> sortedRuns;
};

/// Partitions a list of data files into non-overlapping sections, each
/// containing the minimum number of sorted runs needed.
///
/// Algorithm (two phases):
///   Phase 1 — Section Detection (sweep line):
///     Files sorted by (minKey, maxKey). A sweep line tracks max maxKey.
///     When a new file's minKey exceeds the bound, a new section starts.
///   Phase 2 — Sorted Run Assignment (greedy interval coloring):
///     Within each section, files are assigned to runs using a min-heap.
class PaimonIntervalPartition {
 public:
  static std::vector<Section> partition(
      const std::vector<PaimonDataFile>& files);
};

} // namespace facebook::velox::connector::hive::paimon
