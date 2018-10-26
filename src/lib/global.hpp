#pragma once

#include <chrono>
#include <map>

#include "utils/singleton.hpp"

namespace opossum {

struct OperatorTimes {
  std::chrono::microseconds preparation_time;
  std::chrono::microseconds execution_time;
};

enum class OperatorType;

struct Global : public Singleton<Global> {
  bool jit = false;
  bool lazy_load = false;
  bool jit_validate = false;
  bool deep_copy_exists = false;
  bool interpret = false;
  std::map<OperatorType, OperatorTimes> times;

 private:
  Global() = default;

  friend class Singleton;
};

}  // namespace opossum
