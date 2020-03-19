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
// File: byte_coding.h
// -----------------------------------------------------------------------------
//
// A ByteBuffer class which is a small wrapper around a growable byte array.
// Together with the host of methods to add / read types to ByteBuffers
// (and byte-arrays, vector<char>) useful for encoding special (in particular
// compact) data-structures.

#ifndef CUCKOO_INDEX_COMMON_BYTE_CODING_H_
#define CUCKOO_INDEX_COMMON_BYTE_CODING_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "google/protobuf/io/coded_stream.h"

// All the methods below assume that we're on a little-endian platform => fail
// compilation if this is not the case (use the corresponding proto2 macro).

#if !ABSL_IS_LITTLE_ENDIAN
#error "The byte_coding.h methods work only on little-endian architectures."
#endif

namespace ci {

// Max number of bytes required to write varints.
constexpr uint32_t kVarint32MaxBytes = 5;
constexpr uint32_t kVarint64MaxBytes = 9;

// Trait which checks whether T is a supported "fixed-length raw-value type",
// i.e., whether T is one of int64_t, uint64_t or double.
template <typename T>
struct is_fixed_length_raw_value_type {
  static constexpr bool value = std::is_same<T, int64_t>::value ||
                                std::is_same<T, uint64_t>::value ||
                                std::is_same<T, double>::value;
};

// Holder class which tracks an internal buffer of chars. Its EnsureCapacity(..)
// method is more efficient than vector::resize(..), since it doesn't
// initialize the underlying content and (more importantly) since it is inlined.
class ByteBuffer {
 public:
  static constexpr size_t kDefaultCapacity = 32;

  explicit ByteBuffer(size_t capacity = kDefaultCapacity)
      : data_(new char[capacity]), capacity_(capacity), pos_(0) {}
  // Takes ownership of the data.
  ByteBuffer(char* data, size_t capacity)
      : data_(data), capacity_(capacity), pos_(0) {}

  // Not copyable or movable.
  ByteBuffer(const ByteBuffer&) = delete;
  ByteBuffer& operator=(const ByteBuffer&) = delete;

  ~ByteBuffer() { delete[] data_; }

  // Direct access to the underlying array.
  // May be invalidated on calls to EnsureCapacity(..).
  char* data() { return data_; }
  const char* data() const { return data_; }

  // Releases & returns the internal array, the caller takes ownership.
  // The ByteBuffer is reset with a new, empty array.
  char* release() {
    char* tmp = data_;
    capacity_ = kDefaultCapacity;
    pos_ = 0;
    data_ = new char[capacity_];
    return tmp;
  }

  size_t capacity() const { return capacity_; }

  absl::Span<char> as_span() { return absl::Span<char>(data_, capacity_); }
  absl::Span<const char> as_span() const {
    return absl::Span<const char>(data_, capacity_);
  }

  // For convenience when adding data to this buffer, it provides a current
  // position in the underlying array. This is also meant to be used as lvalue.
  // Before writing to pos, ensure that the capacity is large enough.
  size_t& pos() { return pos_; }
  const size_t& pos() const { return pos_; }
  void set_pos(size_t pos) { pos_ = pos; }

  // Makes sure that the underlying array has at least the given capacity.
  // At least doubles the capacity on resizes. Does not initialize the extra
  // memory allocated (but does copy all existing data).
  void EnsureCapacity(size_t required_capacity) {
    if (ABSL_PREDICT_FALSE(required_capacity > capacity_)) {
      const size_t new_capacity = std::max(2 * capacity_, required_capacity);
      char* new_data = new char[new_capacity];
      memcpy(new_data, data_, capacity_);
      delete[] data_;
      data_ = new_data;
      capacity_ = new_capacity;
    }
  }

 private:
  char* data_;
  size_t capacity_;
  size_t pos_;
};

// TODO: in the Put*(..) methods which take 'data' as an output
// parameter, 'data' could be renamed to 'dst' or so to make it clearer what
// it is about.

// Casts the given value to char* and adds it at the given pos in the given
// array. Overwrites anything that may have been there before. Assumes that
// there is enough space in the array. Sets pos to the new position (right
// after the written value).
template <typename T>
inline void PutPrimitive(T value, absl::Span<char> data, size_t* pos) {
  assert(*pos + sizeof(T) <= data.size());
  // Use memcpy(..) to avoid potential problems when casting doubles. See
  // bit_cast(..) in casts.h for a detailed explanation of why this is
  // preferable to using a reinterpret_cast. Benchmarks look fine for this.
  memcpy(data.data() + *pos, &value, sizeof(T));
  *pos += sizeof(T);
}
template <typename T>
inline void PutPrimitive(T value, ByteBuffer* buf) {
  buf->EnsureCapacity(buf->pos() + sizeof(T));
  PutPrimitive<T>(value, buf->as_span(), &buf->pos());
}

// Interprets the bytes in 'data' from position 'pos' on a type T and returns
// the value at that position, increases pos accordingly.
template <typename T>
inline T GetPrimitive(absl::Span<const char> data, size_t* pos) {
  assert(*pos + sizeof(T) <= data.size());
  T result;
  memcpy(&result, data.data() + *pos, sizeof(T));
  *pos += sizeof(T);
  return result;
}
template <typename T>
inline T GetPrimitive(ByteBuffer* buf) {
  return GetPrimitive<T>(buf->as_span(), &buf->pos());
}

// Stores the given uint32_t in the prefix-varint encoding at data + *pos.
// There must be at least `kVarint32MaxBytes` bytes from pos on (within the
// given size).
inline void PutVarint32(uint32_t value, absl::Span<char> data, size_t* pos) {
  assert(*pos + kVarint32MaxBytes <= data.size());
  unsigned char* data_ptr = reinterpret_cast<unsigned char*>(data.data());
  *pos = google::protobuf::io::CodedOutputStream::WriteVarint32ToArray(
             value, data_ptr + *pos) -
         data_ptr;
}

inline void PutVarint32(uint32_t value, ByteBuffer* buf) {
  buf->EnsureCapacity(buf->pos() + kVarint32MaxBytes);
  PutVarint32(value, buf->as_span(), &buf->pos());
}

// Reads a prefix-varint encoded uint32_t from data + *pos.
inline uint32_t GetVarint32(absl::Span<const char> data, size_t* pos) {
  uint32_t result;
  google::protobuf::io::CodedInputStream input_stream(
      reinterpret_cast<const unsigned char*>(data.data()) + *pos,
      kVarint32MaxBytes);
  input_stream.ReadVarint32(&result);
  *pos += input_stream.CurrentPosition();
  return result;
}

inline uint32_t GetVarint32(ByteBuffer* buf) {
  return GetVarint32(buf->as_span(), &buf->pos());
}

// Stores the given uint64_t in the varint encoding at data + *pos.
// There must be at least `kVarint64MaxBytes` bytes from pos on (within the
// given size).
inline void PutVarint64(uint64_t value, absl::Span<char> data, size_t* pos) {
  assert(*pos + kVarint64MaxBytes <= data.size());
  unsigned char* data_ptr = reinterpret_cast<unsigned char*>(data.data());
  *pos = google::protobuf::io::CodedOutputStream::WriteVarint32ToArray(
             value, data_ptr + *pos) -
         data_ptr;
}

inline void PutVarint64(uint64_t value, ByteBuffer* buf) {
  buf->EnsureCapacity(buf->pos() + kVarint64MaxBytes);
  PutVarint64(value, buf->as_span(), &buf->pos());
}

// Reads a prefix-varint encoded uint64_t from data + *pos.
inline uint64_t GetVarint64(absl::Span<const char> data, size_t* pos) {
  uint64_t result;
  google::protobuf::io::CodedInputStream input_stream(
      reinterpret_cast<const unsigned char*>(data.data()) + *pos,
      kVarint64MaxBytes);
  input_stream.ReadVarint64(&result);
  *pos += input_stream.CurrentPosition();
  return result;
}

inline uint64_t GetVarint64(ByteBuffer* buf) {
  return GetVarint64(buf->as_span(), &buf->pos());
}

// Puts 'length' bytes from 'bytes' into 'data' at position 'pos'.
inline void PutBytes(const char* bytes, size_t length, absl::Span<char> data,
                     size_t* pos) {
  assert(*pos + length <= data.size());
  memcpy(data.data() + *pos, bytes, length);
  *pos += length;
}
inline void PutBytes(const char* bytes, size_t length, ByteBuffer* buf) {
  buf->EnsureCapacity(buf->pos() + length);
  PutBytes(bytes, length, buf->as_span(), &buf->pos());
}

// Stores the length of str in prefix-varint32_t encoding, followed by
// the actual bytes of the string. Can cope with 0 bytes inside the string.
inline void PutString(absl::string_view str, absl::Span<char> data,
                      size_t* pos) {
  PutVarint64(str.size(), data, pos);
  PutBytes(str.data(), str.size(), data, pos);
}
inline void PutString(absl::string_view str, ByteBuffer* buf) {
  // Use the Put*(..) methods on ByteBuffers, since they conveniently
  // ensure that the ByteBuffer has enough capacity.

  PutVarint64(str.size(), buf);
  PutBytes(str.data(), str.size(), buf);
}

// Reads a string from the given 'pos'. Must have been written by PutString(..):
// first the length as prefix-varint32_t, then the actual bytes.
// Note: the string_view points to the sub-string in 'data', i.e., 'data' must
// have a longer lifetime than the returned string_view.
inline absl::string_view GetString(absl::Span<const char> data, size_t* pos) {
  uint64_t length = GetVarint64(data, pos);
  assert(*pos + length <= data.size());
  const char* start = data.data() + *pos;
  *pos += length;
  return absl::string_view(start, length);
}
inline absl::string_view GetString(ByteBuffer* buf) {
  return GetString(buf->as_span(), &buf->pos());
}

}  // namespace ci
#endif  // CUCKOO_INDEX_COMMON_BYTE_CODING_H_
