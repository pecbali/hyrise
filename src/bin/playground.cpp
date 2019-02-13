#include <iostream>

#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tasks/pipeline_execution_task.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

using namespace opossum;  // NOLINT

void execute_pipeline() {
  std::string query = "Select 1, 2;";

  auto builder = SQLPipelineBuilder{query};
  auto pipeline_task = std::make_shared<PipelineExecutionTask>(std::move(builder));

  std::vector<std::shared_ptr<AbstractTask>> task_list = {pipeline_task};

  CurrentScheduler::schedule_and_wait_for_tasks(task_list);

  auto pipeline = pipeline_task->get_sql_pipeline();

  Assert(pipeline->requires_execution() == false, "pipeline is supposed to be executed at this point");

  auto table = pipeline->get_result_table();

  Assert(table->row_count() == 1, "row_count is wrong");
  Assert(table->column_count() == 2, "column_count is wrong");
}

int main() {
  CurrentScheduler::set(std::make_shared<opossum::NodeQueueScheduler>());
  execute_pipeline();
  CurrentScheduler::get()->finish();
}
