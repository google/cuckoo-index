# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----------------------------------------------------------------------------
# File: leveldb.BUILD
# -----------------------------------------------------------------------------

load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

# Original headers at include/leveldb/...
original_headers = glob(["include/leveldb/*.h"])

# Headers exported to include/... and to leveldb/...
headers_in_include = ["include/" + x.split("/")[-1] for x in original_headers]
headers_in_leveldb = ["leveldb/" + x.split("/")[-1] for x in original_headers]

genrule(
  name = "relocate_headers_to_include",
  srcs = original_headers,
  outs = headers_in_include,
  cmd = "cp $(SRCS) -t $(@D)/include",
)

genrule(
  name = "relocate_headers_to_leveldb",
  srcs = original_headers,
  outs = headers_in_leveldb,
  cmd = "cp $(SRCS) -t $(@D)/leveldb",
)

filegroup(
    name = "util_sources_group",
    srcs = [
        "util/arena.cc",
        "util/arena.h",
        "util/bloom.cc",
        "util/coding.cc",
        "util/coding.h",
        "util/crc32c.cc",
        "util/crc32c.h",
        "util/env.cc",
        "util/filter_policy.cc",
        "util/hash.cc",
        "util/hash.h",
        "util/logging.cc",
        "util/logging.h",
        "util/status.cc",
        "util/env_posix_test_helper.h",
        "util/posix_logger.h",
    ],
)

filegroup(
    name = "util_headers_group",
    srcs = [
        "port/port.h",
        "port/port_stdcxx.h",
        "port/thread_annotations.h",
        "include/export.h",
        "include/filter_policy.h",
        "include/slice.h",
        "leveldb/env.h",
        "leveldb/export.h",
        "leveldb/filter_policy.h",
        "leveldb/slice.h",
        "leveldb/status.h",
    ],
)

cc_library(
    name = "util",
    # TODO: Change to env_windows.cc for windows.
    srcs = [":util_sources_group", "util/env_posix.cc"],
    hdrs = [":util_headers_group"],
    defines = ["LEVELDB_PLATFORM_POSIX", "LEVELDB_IS_BIG_ENDIAN=false"],
)
