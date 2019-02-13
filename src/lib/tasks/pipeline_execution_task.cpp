#include "pipeline_execution_task.hpp"

#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

PipelineExecutionTask::PipelineExecutionTask(SQLPipelineBuilder builder) : _builder(std::move(builder)) {}

std::unique_ptr<SQLPipeline> PipelineExecutionTask::get_sql_pipeline() { return std::move(_sql_pipeline); }

void PipelineExecutionTask::_on_execute() {
  _sql_pipeline = std::make_unique<SQLPipeline>(_builder.create_pipeline());
  _sql_pipeline->get_result_tables();
}
}  // namespace opossum
