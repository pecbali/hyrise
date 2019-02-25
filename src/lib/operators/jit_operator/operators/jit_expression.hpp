#pragma once

#include <boost/preprocessor/seq.hpp>
#include <boost/preprocessor/seq/enum.hpp>
#include <boost/preprocessor/seq/for_each.hpp>

#include "all_type_variant.hpp"

#include "operators/jit_operator/jit_types.hpp"

namespace opossum {

#define JIT_VARIANT_MEMBER(r, d, type) BOOST_PP_TUPLE_ELEM(3, 0, type) BOOST_PP_TUPLE_ELEM(3, 1, type);

struct JitVariant {
  template <typename ValueType>
  void set(const ValueType& value);

  template <typename ValueType>
  // Function inlined to reduce specialization overhead
  __attribute__((always_inline)) ValueType get() const;

  BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_MEMBER, _, JIT_DATA_TYPE_INFO)
  bool is_null = false;
};

/* JitExpression represents a SQL expression - this includes arithmetic and logical expressions as well as comparisons.
 * Each JitExpression works on JitTupleValues and is structured as a binary tree. All leaves of that tree reference a tuple
 * value in the JitRuntimeContext and are of type JitExpressionType::Column - independent of whether these values actually
 * came from a column, are literal values or placeholders.
 * Each JitExpression can compute its value and stores it in its assigned result JitTupleValue. JitExpressions are also
 * able to compute the data type of the expression they represent.
 *
 * Using AbstractExpression as a base class for JitExpressions seems like a logical choice. However, AbstractExpression
 * adds a lot of bloat during code specialization. We thus decided against deriving from it here.
 */
class JitExpression {
 public:
  // Create a column JitExpression which returns the corresponding column value from the runtime tuple
  explicit JitExpression(const JitTupleValue& tuple_value);
  // Create a value JitExpression which returns the stored variant value
  explicit JitExpression(const JitTupleValue& tuple_value, const AllTypeVariant& variant);
  // Create a unary JitExpression
  JitExpression(const std::shared_ptr<const JitExpression>& child, const JitExpressionType expression_type,
                const size_t result_tuple_index);
  // Create a binary JitExpression
  JitExpression(const std::shared_ptr<const JitExpression>& left_child, const JitExpressionType expression_type,
                const std::shared_ptr<const JitExpression>& right_child, const size_t result_tuple_index);

  std::string to_string() const;

  JitExpressionType expression_type() const { return _expression_type; }
  std::shared_ptr<const JitExpression> left_child() const { return _left_child; }
  std::shared_ptr<const JitExpression> right_child() const { return _right_child; }
  const JitTupleValue& result() const { return _result_value; }

  // The compute() and compute<ResultValueType>() functions trigger the (recursive) computation of the value
  // represented by this expression.

  /* The compute() function does not return the result, but stores it in the _result_value tuple value.
   * The function MUST be called before the result value in the runtime tuple can safely be accessed through the
   * _result_value.
   * The _result_value itself, however, can safely be passed around before (e.g. by calling the result() function),
   * since it only abstractly represents the result slot in the runtime tuple.
   */
  void compute(JitRuntimeContext& context) const;

  /* The compute<ResultValueType>() function directly returns the result and does not store it in the runtime tuple. The
   * ResultValueType function template parameter specifies the returned type of the result.
   */
  template <typename ResultValueType>
  std::optional<ResultValueType> compute(JitRuntimeContext& context) const;

 private:
  std::pair<const DataType, const bool> _compute_result_type();

  const std::shared_ptr<const JitExpression> _left_child;
  const std::shared_ptr<const JitExpression> _right_child;
  const JitExpressionType _expression_type;
  const JitTupleValue _result_value;

  // A custom JitVariant struct is used to store literal values as std::variant or AllTypeVariant cannot be specialized.
  JitVariant _variant;
};

// cleanup
#undef JIT_VARIANT_MEMBER

}  // namespace opossum
