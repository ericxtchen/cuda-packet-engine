#define _GNU_SOURCE
#include "cpe/affinity.hpp"

#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include <fstream>
#include <set>
#include <string>

std::vector<int> distinct_physical_cores() {
  std::vector<int> cores;
  std::set<int> seen_physical_core_ids;

  long n = sysconf(_SC_NPROCESSORS_ONLN);
  for (long cpu = 0; cpu < n; ++cpu) {
    std::string path = "/sys/devices/system/cpu/cpu" + std::to_string(cpu) +
                       "/topology/core_id";
    std::ifstream f(path);
    int physical_core_id = -1;
    if (f >> physical_core_id) {
      // First logical CPU seen for a given physical core wins
      if (seen_physical_core_ids.insert(physical_core_id).second) {
        cores.push_back(static_cast<int>(cpu));
      }
    }
  }

  if (cores.empty()) {
    unsigned int count = std::thread::hardware_concurrency();
    for (unsigned int i = 0; i < (count ? count : 1); ++i)
      cores.push_back(i);
  }
  return cores;
}

void pin_thread_to_core(std::thread &t, int core_id) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
}