#pragma once

#include <memory>
#include <string>
#include "scheduler/abstract_task.hpp"

namespace opossum {

class SQLPipeline;

class PipelineCreationTask : public AbstractTask {
 public:
  PipelineCreationTask() = delete;
  explicit PipelineCreationTask(const std::string& sql);

  std::unique_ptr<SQLPipeline> get_sql_pipeline();

 protected:
  void _on_execute();

 private:
  std::string _sql;
  std::unique_ptr<SQLPipeline> _sql_pipeline;
};
}  // namespace opossum
