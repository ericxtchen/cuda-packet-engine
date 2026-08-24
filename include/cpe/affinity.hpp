#ifndef CPE_AFFINITY_H
#define CPE_AFFINITY_H

#include <thread>
#include <vector>

// One logical CPU id per distinct physical core (hyperthread siblings
// collapsed to their first logical CPU), in ascending order.
std::vector<int> distinct_physical_cores();

// Pins an already-created std::thread to one logical CPU.
void pin_thread_to_core(std::thread &t, int core_id);

#endif