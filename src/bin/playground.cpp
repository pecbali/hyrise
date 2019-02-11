#include <iostream>

#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tasks/pipeline_creation_task.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

using namespace opossum;  // NOLINT

void approach1() {
  std::string query = "Select 1, 2;";
  std::unique_ptr<SQLPipeline> pipeline;
  std::shared_ptr<JobTask> task = SQLPipelineBuilder{query}.create_and_execute_pipeline(pipeline);

  std::vector<std::shared_ptr<JobTask>> task_list = {task};

  CurrentScheduler::schedule_and_wait_for_tasks(task_list);

  Assert(pipeline->requires_execution() == false, "pipeline is supposed to be executed at this point");

  auto table = pipeline->get_result_table();

  Assert(table->row_count() == 1, "row_count is wrong");
  Assert(table->column_count() == 2, "column_count is wrong");
}

void approach2() {
  std::string query = "Select 1, 2;";

  auto pipeline_task = std::make_shared<PipelineCreationTask>(query);
  std::vector<std::shared_ptr<AbstractTask>> task_list = {pipeline_task};

  CurrentScheduler::schedule_and_wait_for_tasks(task_list);

  auto pipeline = pipeline_task->get_sql_pipeline();

  Assert(pipeline->requires_execution() == false, "pipeline is supposed to be executed at this point");

  auto table = pipeline->get_result_table();

  Assert(table->row_count() == 1, "row_count is wrong");
  Assert(table->column_count() == 2, "column_count is wrong");
}

int main() {
  approach1();
  approach2();
}
