#include "jit_compute.hpp"

#include "jit_expression.hpp"

namespace opossum {

JitCompute::JitCompute(const std::shared_ptr<const JitExpression>& expression) : _expression{expression} {}

std::string JitCompute::description() const {
  return "[Compute] x" + std::to_string(_expression->result().tuple_index()) + " = " + _expression->to_string();
}

std::shared_ptr<const JitExpression> JitCompute::expression() const { return _expression; }

void JitCompute::_consume(JitRuntimeContext& context) const {
  _expression->compute(context);
  _emit(context);
}

}  // namespace opossum
