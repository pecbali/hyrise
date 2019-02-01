#include "common_subplan_rule.hpp"

#include <functional>
#include <unordered_set>

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

using namespace opossum::expression_functional;  // NOLINT

namespace {
  struct myhash
  {
    std::size_t operator()(const std::shared_ptr<AbstractLQPNode>& k) const
    {
      return 0;
    }
  };

  struct myequals
{
public:
  bool operator() (std::shared_ptr<AbstractLQPNode> const& t1, std::shared_ptr<AbstractLQPNode> const& t2) const
  {
    return *t1 == *t2;
  }
};

void apply_to_impl(const std::shared_ptr<AbstractLQPNode>& node, std::unordered_set<std::shared_ptr<AbstractLQPNode>, myhash, myequals>& nodes, LQPNodeMapping& mapping) {
  // Iterate over all expressions of a lqp node
  for (const auto& expression : node->node_expressions) {
    // Recursively iterate over each nested expression
    visit_expression(expression, [&](const auto& sub_expression) {
      // Apply rule for every subquery
      if (const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression)) {
        apply_to_impl(subquery_expression->lqp, nodes, mapping);
        return ExpressionVisitation::DoNotVisitArguments;
      }
      return ExpressionVisitation::VisitArguments;
    });
  }

  for (auto input_side : {LQPInputSide::Left, LQPInputSide::Right}) {
    auto input = node->input(input_side);
    if (!input) continue;
    auto it = nodes.find(input);
    if (it != nodes.end()) {
      for (auto m : lqp_create_node_mapping(input, *it)) {
        mapping.insert(m);
      }
      node->set_input(input_side, *it);
      continue;
    } else {
      nodes.emplace(node);
      apply_to_impl(input, nodes, mapping);
    }
  }
}

}

std::string CommonSubplanRule::name() const { return "Common Subplan Rule"; }

void CommonSubplanRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  std::unordered_set<std::shared_ptr<AbstractLQPNode>, myhash, myequals> nodes;
  LQPNodeMapping mapping;
  apply_to_impl(node, nodes, mapping);


  visit_lqp(node, [&](auto& deeper_node) {
    for (auto& expression : deeper_node->node_expressions) {
      expression_adapt_to_different_lqp(expression, mapping);
    }
    return LQPVisitation::VisitInputs;
  });

}

}  // namespace opossum
