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

workspace(name = "com_github_google_cuckoo_index")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# abseil-cpp.
http_archive(
    name = "com_google_absl",
    sha256 = "d3311ead20ffce78c7fde96df803b73d0de8d992d46bdf36753954bd2d459f31",
    strip_prefix = "abseil-cpp-df3ea785d8c30a9503321a3d35ee7d35808f190d",
    urls = ["https://github.com/abseil/abseil-cpp/archive/df3ea785d8c30a9503321a3d35ee7d35808f190d.zip"],
)

# Google Test.
http_archive(
    name = "com_google_googletest",
    sha256 = "7c7709af5d0c3c2514674261f9fc321b3f1099a2c57f13d0e56187d193c07e81",
    strip_prefix = "googletest-10b1902d893ea8cc43c69541d70868f91af3646b",
    urls = ["https://github.com/google/googletest/archive/10b1902d893ea8cc43c69541d70868f91af3646b.zip"],
)

# Google Benchmark.
http_archive(
    name = "com_google_benchmark",
    sha256 = "c3673a6c8c9233e88d885c61a4f152ae585247901c6e221b19e4cfe5415f743f",
    strip_prefix = "benchmark-8982e1ee6aef31e48170400b7d1dc9969b156e5e",
    urls = ["https://github.com/google/benchmark/archive/8982e1ee6aef31e48170400b7d1dc9969b156e5e.zip"],
)

# C++ rules for Bazel.
http_archive(
    name = "rules_cc",
    sha256 = "954b7a3efc8752da957ae193a13b9133da227bdacf5ceb111f2e11264f7e8c95",
    strip_prefix = "rules_cc-9e10b8a6db775b1ecd358d8ddd3dab379a2c29a5",
    urls = ["https://github.com/bazelbuild/rules_cc/archive/9e10b8a6db775b1ecd358d8ddd3dab379a2c29a5.zip"],
)

# Build rules for Boost.
# Apache License 2.0 for the rules.
# Boost Software License for boost (similar to MIT or BSD).
git_repository(
    name = "com_github_nelhage_rules_boost",
    commit = "353a58c5d231293795e7b63c2c21467922153add",
    remote = "https://github.com/nelhage/rules_boost",
    shallow_since = "1580416893 -0800",
)

load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")

boost_deps()

# Protocol buffers.
http_archive(
    name = "com_google_protobuf",
    sha256 = "c5fd8f99f0d30c6f9f050bf008e021ccc70d7645ac1f64679c6038e07583b2f3",
    strip_prefix = "protobuf-d0bfd5221182da1a7cc280f3337b5e41a89539cf",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/d0bfd5221182da1a7cc280f3337b5e41a89539cf.zip"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "rules_proto",
    sha256 = "602e7161d9195e50246177e7c55b2f39950a9cf7366f74ed5f22fd45750cd208",
    strip_prefix = "rules_proto-97d8af4dc474595af3900dd85cb3a29ad28cc313",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz",
        "https://github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz",
    ],
)

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()

http_archive(
    name = "rules_cc",
    sha256 = "35f2fb4ea0b3e61ad64a369de284e4fbbdcdba71836a5555abb5e194cf119509",
    strip_prefix = "rules_cc-624b5d59dfb45672d4239422fa1e3de1822ee110",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_cc/archive/624b5d59dfb45672d4239422fa1e3de1822ee110.tar.gz",
        "https://github.com/bazelbuild/rules_cc/archive/624b5d59dfb45672d4239422fa1e3de1822ee110.tar.gz",
    ],
)

load("@rules_cc//cc:repositories.bzl", "rules_cc_dependencies")

rules_cc_dependencies()

# Roaring.
# Apache License 2.0
http_archive(
    name = "CRoaring",
    build_file = "@//:croaring.BUILD",
    sha256 = "b26a1878c1016495c758e98b1ec62ed36bb401afd0d0f5f84f37615a724d2b1d",
    strip_prefix = "CRoaringUnityBuild-c1d1a754faa6451436efaffa3fe449edc7710b65",
    urls = ["https://github.com/lemire/CRoaringUnityBuild/archive/c1d1a754faa6451436efaffa3fe449edc7710b65.zip"],
)

# CSV parser.
# MIT License
http_archive(
    name = "csv-parser",
    build_file = "@//:csv-parser.BUILD",
    sha256 = "550681980b7012dd9ef64dc46ff24044444c4f219b34b96f15fdc7bbe3f1fdc6",
    strip_prefix = "csv-parser-6fb1f43ad43fc7962baa3b0fe524b282a56ae4b0",
    urls = ["https://github.com/vincentlaucsb/csv-parser/archive/6fb1f43ad43fc7962baa3b0fe524b282a56ae4b0.zip"],
)

# XOR filter
# Apache License 2.0
http_archive(
    name = "xor_singleheader",
    build_file = "@//:xor_singleheader.BUILD",
    sha256 = "c58d0d21404c11ccf509e9435693102ca5806ea75321d39afb894314a882f3a6",
    strip_prefix = "xor_singleheader-6cea6a4dcf2f18a0e3b9b9e0b94d6012b804ffa1",
    urls = ["https://github.com/FastFilter/xor_singleheader/archive/6cea6a4dcf2f18a0e3b9b9e0b94d6012b804ffa1.zip"],
)
