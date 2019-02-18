#include <iostream>

#include <memory>
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/abstract_task.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tasks/pipeline_execution_task.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

using namespace opossum;  // NOLINT

static constexpr auto query =
    "SELECT l_returnflag, l_linestatus, SUM(l_quantity) as sum_qty, SUM(l_extendedprice) as sum_base_price, "
    "SUM(l_extendedprice*(1-l_discount)) as sum_disc_price, "
    "SUM(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, AVG(l_quantity) as avg_qty, "
    "AVG(l_extendedprice) as avg_price, AVG(l_discount) as avg_disc, COUNT(*) as count_order "
    "FROM lineitem "
    "WHERE l_shipdate <= '1998-12-01' "
    "GROUP BY l_returnflag, l_linestatus "
    "ORDER BY l_returnflag, l_linestatus;";


void new_approach() {
  auto task_list = std::vector<std::shared_ptr<AbstractTask>>();

  for (auto i = 0; i < 1000; i++) {
    auto builder = SQLPipelineBuilder{query}.disable_mvcc();
    auto pipeline_task = std::make_shared<PipelineExecutionTask>(std::move(builder));
    task_list.emplace_back(pipeline_task);
  }

  std::cout << task_list.size() << std::endl;
  CurrentScheduler::schedule_and_wait_for_tasks(task_list);

  auto pipeline = std::dynamic_pointer_cast<PipelineExecutionTask>(task_list.back())->get_sql_pipeline();
  Assert(pipeline->requires_execution() == false, "pipeline is supposed to be executed at this point");
}

void old_approach() {
  auto task_list = std::vector<std::shared_ptr<AbstractTask>>();

  for (auto i = 0; i < 1000; i++) {
    auto pipeline_builder = SQLPipelineBuilder{query}.disable_mvcc();
    auto pipeline = pipeline_builder.create_pipeline();

    auto tasks_per_statement = pipeline.get_tasks();

    for (auto tasks : tasks_per_statement) {
      CurrentScheduler::schedule_tasks(tasks);
      task_list.insert(task_list.end(), tasks.begin(), tasks.end());
    }
  }

  CurrentScheduler::wait_for_tasks(task_list);
}

int main() {
  TpchTableGenerator{0.01f, 100'000}.generate_and_store();
  CurrentScheduler::set(std::make_shared<opossum::NodeQueueScheduler>());
  // old_approach();
  new_approach();
  CurrentScheduler::get()->finish();
}
