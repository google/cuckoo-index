#include <cstdint>

#include "absl/container/flat_hash_map.h"
#include "absl/time/time.h"

namespace ci {

enum class Counter {
  Kicking,
  Encoding,
};

// A simple profiler that can collect stats. Use `ScopedProfile` for registering
// counters to profile.
//
// Is thread-safe, because there can be only a single instance per thread.
class Profiler {
 public:
  // Retrieves a `Profiler` instance for the current thread.
  static Profiler& GetThreadInstance();

  Profiler(const Profiler&) = delete;
  Profiler& operator=(const Profiler&) = delete;
  virtual ~Profiler() {}

  int64_t GetValue(Counter counter) const {
    const auto value_it = counters_.find(counter);
    return value_it != counters_.end() ? value_it->second : 0;
  }

  void Reset() { counters_.clear(); }

 private:
  friend class ScopedProfile;

  Profiler() {}

  // Starts profiling for the given counter.
  void Start(Counter counter) {
    counters_[counter] -= absl::GetCurrentTimeNanos();
  }

  // Stops profiling for the given counter.
  void Stop(Counter counter) {
    counters_[counter] += absl::GetCurrentTimeNanos();
  }

  absl::flat_hash_map<Counter, int64_t> counters_;
};

// Instantiate a local variable with this class to profile the local scope.
// Example:
//   void MyClass::MyMethod() {
//     ScopedProfile t(Counters::MyClass_MyMethod);
//     .... // Do expensive stuff.
//   }
class ScopedProfile {
 public:
  // ScopedProfile is neither copyable nor moveable.
  ScopedProfile(const Profiler&) = delete;
  ScopedProfile& operator=(const Profiler&) = delete;

  explicit ScopedProfile(Counter counter) : counter_(counter) {
    Profiler::GetThreadInstance().Start(counter);
  }

  ~ScopedProfile() { Profiler::GetThreadInstance().Stop(counter_); }

 private:
  Counter counter_;
};

}  // namespace ci
