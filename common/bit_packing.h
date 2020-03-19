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
// File: bit_packing.h
// -----------------------------------------------------------------------------
//
// Utilities for bit-packing. In our project bit-packing simply means using the
// least amount of bits or bytes necessary to store a value.

#ifndef CUCKOO_INDEX_COMMON_BIT_PACKING_H_
#define CUCKOO_INDEX_COMMON_BIT_PACKING_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <type_traits>

#include "absl/base/internal/endian.h"
#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "common/byte_coding.h"

// In our bit-packing code we assume a 64 bit architecture.
static_assert(sizeof(void*) == 8, "Must be on a 64 bit platform");

namespace ci {

inline uint32_t BitsRequired(uint32_t x) {
  return (31 ^ __builtin_clz(x | 0x1)) + 1;
}

inline uint32_t BitsRequired64(uint64_t x) {
  return (63 ^ __builtin_clzll(x | 0x1)) + 1;
}

// Returns the number of bits needed for the given uint32_t / uint64_t value.
// Assumes that we need 0 bits to encode val = 0.
template <typename T>
int BitWidth(T val);

template <>
inline int BitWidth<uint32_t>(uint32_t val) {
  if (ABSL_PREDICT_FALSE(val == 0))
    return 0;
  else
    return BitsRequired(val);
}

template <>
inline int BitWidth<uint64_t>(uint64_t val) {
  if (ABSL_PREDICT_FALSE(val == 0))
    return 0;
  else
    return BitsRequired64(val);
}

// Returns the maximum number of bits needed to bit-pack the values in the
// uint32_t/uint64_t-array of the given size. Returns 0 if all entries in the
// array are equal to 0 or if size = 0.
template <typename T>
inline int MaxBitWidth(absl::Span<const T> array) {
  if (array.empty()) return 0;
  return BitWidth<T>(*std::max_element(array.begin(), array.end()));
}

// Returns the number of bytes required to encode num_bits using bit-packing.
// Examples: for num_bits = 0: returns 0, for num_bits = 1-8: returns 1.
// Typical usage: BitPackingBytesRequired(size * bit_width).
inline size_t BitPackingBytesRequired(size_t num_bits) {
  assert(num_bits >= 0);
  assert(num_bits <= std::numeric_limits<uint64_t>::max() - 7);
  return (num_bits + 7) >> 3;
}

namespace internal {

// For bit-widths > 58 we potentially need to read / write an extra 64 bit word.
// Otherwise, a single 64 bit word and shifting the contents always works. Note:
// We start at a byte-offset of (bit-offset >> 3) and then shift by at most by 7
// bits. Let's look at the resulting cases by bit-width:
// * bit-width <= 57: In the extreme case the payload starts at the 7th bit of
//   the first byte => we read 1 bit in the first byte and <= 56 bits in the
//   remaining 7 bytes.
// * bit-width = 58: we only read/write at even bit-offsets. Therefore, in the
//   extreme case the payload starts at the 6th bit of the first byte => we read
//   2 bits in the first byte and 56 bits in the remaining 7 bytes.
// * bit-width > 58: In the extreme case the payload starts at the 7th bit of
//   the first byte => we read 1 bit in the first byte then need to read > 56
//   bits, i.e., more than just 7 bytes.
constexpr int kMaxSingleWordBitWidth = 58;
constexpr int kSlopBytes = 8;

// Returns a mask with the lowest num_bits set. Works only for num_bits < 64.
inline uint64_t FastBitMask(int num_bits) {
  // Note num_bits = 64 cannot be computed with a single shift, since 1ULL << 64
  // is not defined and in practice results in 1ULL.
  return (1ULL << num_bits) - 1ULL;
}

// Stores `val` with the given `bit_width` at the offset `shift` in `word`.
// `shift` will be updated for the next value to set. If it overflows to the
// next byte or beyond, the current `word` is written to `data` and `word`
// updated accordingly. T must be either uint32_t or uint64_t. Special care is
// taken for the uint64_t bit case where the high-bits of `val` may have
// overflown.
//
// This is in essence the converse of the loop-unrolling code for GetBatch(..)
// in `internal::Unroll` below (notable difference: here uint64_t is supported).
// The writing is not as performance-critical => no unrolling for now.
template <typename T>
inline void StoreValue(const uint64_t val, const int bit_width, uint64_t* word,
                       size_t* shift, char** data) {
  static_assert(
      std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value,
      "Only uint32_t and uint64_t supported");
  assert(BitWidth(val) <= bit_width);
  assert(*shift < 8);
  assert((*word & (~FastBitMask(static_cast<int>(*shift)))) == 0);

  *word |= val << *shift;
  *shift += bit_width;

  // Store the word buffered so far at `data`. For `shift` >= 8, at least a full
  // byte was written. We can advance `data` by the number of bytes written,
  // remove the written bytes out of `word` (shift right) and update `shift`
  // accordingly.
  absl::little_endian::Store64(*data, *word);
  // Compute the number of bits filled as "full bytes" in `word` so far.
  // We shift `word` to the right by this amount below.
  const size_t right_shift = *shift & ~0x7ULL;
  *shift &= 0x7ULL;
  // Use `std::is_same` to ensure code below is simplified for uint32_t.
  const bool is_64_bit_shift =
      std::is_same<T, uint64_t>::value && right_shift == 64;
  *word = is_64_bit_shift ? 0 : *word >> right_shift;
  *data += right_shift >> 3;
  // For uint64_t we may need ensure that the first `shift` bits in `word` are
  // set if `right_shift` = 64 and `shift` > 0 (i.e., high bits overflowed!).
  if (ABSL_PREDICT_FALSE(is_64_bit_shift && *shift > 0)) {
    assert(*word == 0);
    // Set the first `shift` bits of `word` to the overflown bits.
    *word = val >> (bit_width - *shift);
  }
  // Check that no bits with position >= `shift` are set.
  assert((*word & ~FastBitMask(static_cast<int>(*shift))) == 0);
}

}  // namespace internal

// Adds the given `array` of type `T` (must be uint32_t or uint64_t) in fixed
// `bit_width` encoding to `buffer`.
template <typename T>
void StoreBitPacked(absl::Span<const T> array, const int bit_width,
                    ByteBuffer* buffer) {
  if (bit_width == 0) return;
  const size_t num_bytes = BitPackingBytesRequired(bit_width * array.size());
  // Check that at least a byte would be written.
  // Note that for empty arrays the bit_width is zero.
  assert(num_bytes > 0);

  // Prepare the buffer. Ensure that we have extra slop-bytes in the end.
  // We may write up to 7 bytes past the last one, see StoreValue(..).
  size_t new_pos = buffer->pos() + num_bytes;
  buffer->EnsureCapacity(new_pos + internal::kSlopBytes);

  // The position from which on to store the bit-packed values.
  char* data = buffer->data() + buffer->pos();
  // The amount by which the next `val` should be shifted.
  size_t shift = 0;
  // Buffer for the current word, filled low-to-high bits and written to `data`
  // with every call to StoreValue(..) below (except possibly the last call).
  uint64_t word = 0;
  for (const T val : array)
    internal::StoreValue<T>(val, bit_width, &word, &shift, &data);
  // Make sure any pending changes to `word` are stored at `data`.
  absl::little_endian::Store64(data, word);
  buffer->set_pos(new_pos);
}

// Adds the necessary slop-bytes for correctly supporting BitPacked ints.
// Note that these bytes do not need to be added after each bit-packed array
// They only need to be added once, usually to the end of the buffer.
inline void PutSlopBytes(ByteBuffer* buffer) {
  PutPrimitive<uint64_t>(0, buffer);
}

// Lightweight class to read values from an array that was previously
// bit-packed with StoreBitPacked<T>(..). This uses the same unsafe semantics as
// it may access up to 8 bytes beyond the last bit set. Make sure to leave room
// for these "slop bytes" when writing arrays with StoreBitPacked<T>(..). Use
// PutSlopBytes(..).
// T must be either uint32_t or uint64_t.
template <typename T>
class BitPackedReader {
  static_assert(std::is_same<T, uint32_t>::value ||
                    std::is_same<T, uint64_t>::value,
                "Only uint32_t and uint64_t supported");

 public:
  // Does *not* take ownership of the array, i.e., its lifetime must be longer
  // than the lifetime of this reader.
  BitPackedReader(int bit_width, const char* data)
      : bit_width_(bit_width), data_(data) {}

  // Empty BitPackedReader.
  BitPackedReader() : bit_width_(0), data_(nullptr) {}

  // Allow copying, this is meant to be handed into methods.
  BitPackedReader(const BitPackedReader&) = default;
  BitPackedReader& operator=(const BitPackedReader&) = default;

  // Read the value at the given index.
  T Get(size_t index) const {
    const size_t bit0_offset = index * bit_width_;
    const size_t byte0_offset = bit0_offset >> 3;
    const char* byte0 = data_ + byte0_offset;
    // The index of the 0th bit within byte0.
    const int start = bit0_offset & 0x7;
    uint64_t val = absl::little_endian::Load64(byte0) >> start;

    // For uint64_t we might overflow into the next word. Add the is_same check
    // so the compiler can get rid of this if-statement entirely for uint32_t.
    if (std::is_same<T, uint64_t>::value &&
        ABSL_PREDICT_FALSE(bit_width_ > internal::kMaxSingleWordBitWidth)) {
      const int next_word_bits = start + bit_width_ - 64;
      if (next_word_bits > 0)
        val |= absl::little_endian::Load64(byte0 + 8)
               << (bit_width_ - next_word_bits);
      return bit_width_ == 64 ? val : val & internal::FastBitMask(bit_width_);
    }
    // The following also works for bit_width_ == 0, since "... & 0" will be
    // applied in that case. This avoids a check for bit_width_ == 0 which is
    // more costly. Note: since nothing at all was written in this case, we read
    // exactly 8 bytes beyond the end here. This is why we always add 8 slop
    // bytes (and not 7 as elsewhere).
    return static_cast<T>(val & internal::FastBitMask(bit_width_));
  }

  // Unpacks 'size' values starting at index 0. For each value calls
  // 'add_value(size_t i, T value)'; appending the values one-by-one for
  // increasing 'i'. Note: currently only implemented for T = uint32_t.
  //
  // Note: we could consider getting rid of the lambda and instead always
  // fill an array. This would simplify the code and lead to less inlining.
  // With the lambda we can avoid extra copies and iterations over the data
  // though, e.g., one can simply add a constant in the lambda.
  template <typename AddValueLambda>
  void GetBatch(size_t size, const AddValueLambda& add_value)
      // Force inlining here, since the 'add_value' lambda otherwise wouldn't be
      // inlined into the inner loop. This is crucial for performance.
      __attribute__((always_inline));

  std::string DebugString(size_t size) const {
    std::string s;
    absl::StrAppend(&s, "size: ", size);
    absl::StrAppend(&s, ", bit-width: ", bit_width_);
    absl::StrAppend(&s,
                    ", bytes: ", BitPackingBytesRequired(size * bit_width_));
    return s;
  }

 private:
  // Implementation of GetBatch(..) for a constant kBitWidth (must be the same
  // as bit_width_). Permits optimized unrolling.
  template <typename AddValueLambda, int kBitWidth>
  void GetBatchImpl(size_t size, const AddValueLambda& add_value)
      __attribute__((always_inline));

  int bit_width_;
  const char* data_;
};

namespace internal {

constexpr size_t kUnrollCount = 32;

// The struct Unroll is used to do loop-urolling to fetch a batch of 32 bit
// packed values with 'kBitWidth'. It is heavily inspired by the FastPFor
// library written by Daniel Lemire. We tried to simplify the approach slightly,
// but the key ideas are the same.
//
// The method Unroll<kBitWidth, KShift, kMaxIndex, kIndex>::Get(..) is unrolled
// until kIndex = kMaxIndex (must be <= 32) is reached. In each call the entry
// at '*data' is shifted by 'kShift' and stored by calling 'add_value(kIndex,
// value)'. If 'kShift' has reached the next 32 bit word, 'data' is advanced.
//
// Compared to the standard loop over BitPackedReader<32>::Get(..), a couple of
// computations and conditionals are saved per step. The resulting code is
// about 2 - 2.7x faster.
template <size_t kBitWidth, size_t kShift, size_t kMaxIndex, size_t kIndex>
struct Unroll {
  template <typename AddValueLambda>
  static inline void Get(const uint32_t** data,
                         const AddValueLambda& add_value) {
    // Shift and mask the current bit-packed value, append it with 'add_value'.
    // Fetch a full 64 bit word from *data, this way we can use a single shift.
    // Note that kShift + kBitWidth may be > 32, but is always <= 64.
    const uint64_t word =
        absl::little_endian::Load64(reinterpret_cast<const char*>(*data));
    constexpr uint64_t mask = (1ULL << kBitWidth) - 1;
    add_value(kIndex, static_cast<uint32_t>((word >> kShift) & mask));

    // Advance data if we reached into the next 4 byte word.
    if (kShift + kBitWidth >= 32) ++(*data);

    // "Recurse" into Get(..) with an increased 'kIndex' and modified 'kShift'.
    const size_t kNextShift = (kShift + kBitWidth) % 32;
    Unroll<kBitWidth, kNextShift, kMaxIndex, kIndex + 1>::Get(data, add_value);
  }
};

// Terminal case when kMaxIndex is reached => do nothing.
template <size_t kBitWidth, size_t kShift, size_t kMaxIndex>
struct Unroll<kBitWidth, kShift, kMaxIndex, kMaxIndex> {
  template <typename AddValueLambda>
  static inline void Get(const uint32_t** data,
                         const AddValueLambda& add_value) {}
};

}  // namespace internal

template <typename T>
template <typename AddValueLambda>
inline void BitPackedReader<T>::GetBatch(size_t size,
                                         const AddValueLambda& add_value) {
  // "Transform" the member field 'bit_width_' into a constexpr.
  switch (bit_width_) {
    case 0:
      GetBatchImpl<AddValueLambda, 0>(size, add_value);
      return;
    case 1:
      GetBatchImpl<AddValueLambda, 1>(size, add_value);
      return;
    case 2:
      GetBatchImpl<AddValueLambda, 2>(size, add_value);
      return;
    case 3:
      GetBatchImpl<AddValueLambda, 3>(size, add_value);
      return;
    case 4:
      GetBatchImpl<AddValueLambda, 4>(size, add_value);
      return;
    case 5:
      GetBatchImpl<AddValueLambda, 5>(size, add_value);
      return;
    case 6:
      GetBatchImpl<AddValueLambda, 6>(size, add_value);
      return;
    case 7:
      GetBatchImpl<AddValueLambda, 7>(size, add_value);
      return;
    case 8:
      GetBatchImpl<AddValueLambda, 8>(size, add_value);
      return;
    case 9:
      GetBatchImpl<AddValueLambda, 9>(size, add_value);
      return;
    case 10:
      GetBatchImpl<AddValueLambda, 10>(size, add_value);
      return;
    case 11:
      GetBatchImpl<AddValueLambda, 11>(size, add_value);
      return;
    case 12:
      GetBatchImpl<AddValueLambda, 12>(size, add_value);
      return;
    case 13:
      GetBatchImpl<AddValueLambda, 13>(size, add_value);
      return;
    case 14:
      GetBatchImpl<AddValueLambda, 14>(size, add_value);
      return;
    case 15:
      GetBatchImpl<AddValueLambda, 15>(size, add_value);
      return;
    case 16:
      GetBatchImpl<AddValueLambda, 16>(size, add_value);
      return;
    case 17:
      GetBatchImpl<AddValueLambda, 17>(size, add_value);
      return;
    case 18:
      GetBatchImpl<AddValueLambda, 18>(size, add_value);
      return;
    case 19:
      GetBatchImpl<AddValueLambda, 19>(size, add_value);
      return;
    case 20:
      GetBatchImpl<AddValueLambda, 20>(size, add_value);
      return;
    case 21:
      GetBatchImpl<AddValueLambda, 21>(size, add_value);
      return;
    case 22:
      GetBatchImpl<AddValueLambda, 22>(size, add_value);
      return;
    case 23:
      GetBatchImpl<AddValueLambda, 23>(size, add_value);
      return;
    case 24:
      GetBatchImpl<AddValueLambda, 24>(size, add_value);
      return;
    case 25:
      GetBatchImpl<AddValueLambda, 25>(size, add_value);
      return;
    case 26:
      GetBatchImpl<AddValueLambda, 26>(size, add_value);
      return;
    case 27:
      GetBatchImpl<AddValueLambda, 27>(size, add_value);
      return;
    case 28:
      GetBatchImpl<AddValueLambda, 28>(size, add_value);
      return;
    case 29:
      GetBatchImpl<AddValueLambda, 29>(size, add_value);
      return;
    case 30:
      GetBatchImpl<AddValueLambda, 30>(size, add_value);
      return;
    case 31:
      GetBatchImpl<AddValueLambda, 31>(size, add_value);
      return;
    case 32:
      GetBatchImpl<AddValueLambda, 32>(size, add_value);
      return;
  }
  std::cerr << "Unexpected bit-width: " << bit_width_ << std::endl;
  std::exit(1);
}

template <typename T>
template <typename AddValueLambda, int kBitWidth>
inline void BitPackedReader<T>::GetBatchImpl(size_t size,
                                             const AddValueLambda& add_value) {
  static_assert(std::is_same<T, uint32_t>::value, "Unexpected type");
  assert(kBitWidth == bit_width_);
  const uint32_t* data = reinterpret_cast<const uint32_t*>(data_);

  // Retrieve the batch of values by repeatedly calling Unroll::Get(..).
  size_t offset = 0;
  while (offset + internal::kUnrollCount <= size) {
    internal::Unroll<kBitWidth, 0, internal::kUnrollCount, 0>::Get(
        &data, [&](size_t i, uint32_t value) { add_value(offset + i, value); });
    offset += internal::kUnrollCount;
  }
  // Take care of the remaining 'size - offset' values.
  while (offset < size) {
    // TODO: possibly extract out a method GetImpl(..) that takes the
    // bit_width as a parameter, this way we can hand in the constant kBitWidth.
    add_value(offset, Get(offset));
    ++offset;
  }

  // If we get rid of the "massive" inlining due to the lambda, we could
  // expand out the following more efficient variant for the remainder.
  // (With CONVERT_0_TO_32_TO_CONST being a macro for the switch-statement.)
  // CONVERT_0_TO_32_TO_CONST(size - offset,
  //     (internal::Unroll<kBitWidth, 0, kConstVal, 0>::Get(data, ...)));
}

}  // namespace ci
#endif  // CUCKOO_INDEX_COMMON_BIT_PACKING_H_
