#include "pipeline_creation_task.hpp"

#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

PipelineCreationTask::PipelineCreationTask(const std::string& sql) : _sql(sql) {}

std::unique_ptr<SQLPipeline> PipelineCreationTask::get_sql_pipeline() { return std::move(_sql_pipeline); }

void PipelineCreationTask::_on_execute() {
  _sql_pipeline = std::make_unique<SQLPipeline>(SQLPipelineBuilder{_sql}.create_pipeline());
  _sql_pipeline->get_result_tables();
}
}  // namespace opossum
