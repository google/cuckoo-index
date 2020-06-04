#include "common/profiling.h"

namespace ci {

Profiler& Profiler::GetThreadInstance() {
  thread_local static Profiler static_profiler;
  return static_profiler;
}

}  // namespace ci
