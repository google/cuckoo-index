// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// File: byte_coding_test.cc
// -----------------------------------------------------------------------------
//
// Tests for the vector-coders.

#include "common/byte_coding.h"

#include <limits>
#include <numeric>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "gtest/gtest.h"

namespace ci {
namespace {

// Size to use for vectors to be filled in tests.
constexpr size_t kVectorSize = 1000;

class CodersTest : public testing::Test {
 protected:
  void SetUp() override {
    vec_.clear();
    buf_.set_pos(0);
  }

  // Checks all PutPrimitive and GetPrimitive variants on std::vector and
  // ByteBuffer.
  template <typename T>
  void CheckPutAndGetPrimitive(const T& value) {
    // *** std::vector ***
    const size_t old_size = vec_.size();
    vec_.resize(vec_.size() + sizeof(T));
    size_t pos = old_size;
    PutPrimitive<T>(value, absl::Span<char>(vec_), &pos);
    EXPECT_EQ(pos, vec_.size());
    pos = old_size;
    EXPECT_EQ(value, GetPrimitive<T>(vec_, &pos));
    EXPECT_EQ(pos, vec_.size());

    // *** ByteBuffer ***
    const size_t old_pos = buf_.pos();
    PutPrimitive<T>(value, &buf_);
    size_t end_pos = buf_.pos();
    EXPECT_EQ(end_pos - old_pos, sizeof(T));
    buf_.set_pos(old_pos);
    EXPECT_EQ(value, GetPrimitive<T>(&buf_));
    EXPECT_EQ(buf_.pos(), end_pos);
  }

  template <typename T>
  void CheckPrimitives(absl::Span<const T> values) {
    for (const T& value : values) CheckPutAndGetPrimitive(value);
  }

  std::vector<char> vec_;
  ByteBuffer buf_;
};

TEST_F(CodersTest, Checkint32) {
  CheckPrimitives<int32_t>({-17, -1, 0, 1, 17, 42,
                            std::numeric_limits<int32_t>::min(),
                            std::numeric_limits<int32_t>::max()});
}

TEST_F(CodersTest, CheckUint32) {
  CheckPrimitives<uint32_t>(
      {0, 1, 17, 42, std::numeric_limits<uint32_t>::max()});
}

TEST_F(CodersTest, Checkint64) {
  const std::vector<int64_t> int64_ts({-17, -1, 0, 1, 17, 42,
                                       std::numeric_limits<int64_t>::min(),
                                       std::numeric_limits<int64_t>::max()});
  CheckPrimitives<int64_t>(int64_ts);
}

TEST_F(CodersTest, CheckUint64) {
  const std::vector<uint64_t> uint64_ts(
      {0, 1, 17, 42, std::numeric_limits<int64_t>::max()});
  CheckPrimitives<uint64_t>(uint64_ts);
}

TEST_F(CodersTest, CheckFloat) {
  using nl = std::numeric_limits<float>;

  CheckPrimitives<float>({-17, -1, 0, 1, 17, 42, nl::min(), -nl::min(),
                          nl::max(), -nl::max(), nl::infinity(),
                          -nl::infinity()});
}

TEST_F(CodersTest, CheckDouble) {
  using nl = std::numeric_limits<double>;

  const std::vector<double> doubles({-17, -1, 0, 1, 17, 42, nl::min(),
                                     -nl::min(), nl::max(), -nl::max(),
                                     nl::infinity(), -nl::infinity()});
  CheckPrimitives<double>(doubles);
}

TEST_F(CodersTest, CheckString) {
  const std::string arr[] = {
      "", "James", "Dean", "Humphrey Bogart", std::string("\xFF\0\xFF", 3), ""};
  size_t pos = 0;
  vec_.resize(kVectorSize);
  for (const std::string& value : arr) {
    PutString(value, &buf_);
    PutString(value, absl::Span<char>(vec_), &pos);
  }
  pos = 0;
  buf_.set_pos(0);
  for (const std::string& value : arr) {
    EXPECT_EQ(value, GetString(&buf_));
    EXPECT_EQ(value, GetString(vec_, &pos));
  }
}

}  // namespace
}  // namespace ci
