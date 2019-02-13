#pragma once

#include <memory>
#include <string>
#include "scheduler/abstract_task.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

class SQLPipeline;

class PipelineExecutionTask : public AbstractTask {
 public:
  PipelineExecutionTask() = delete;
  explicit PipelineExecutionTask(SQLPipelineBuilder builder);

  std::unique_ptr<SQLPipeline> get_sql_pipeline();

 protected:
  void _on_execute();

 private:
  SQLPipelineBuilder _builder;
  std::unique_ptr<SQLPipeline> _sql_pipeline;
};
}  // namespace opossum
