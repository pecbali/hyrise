#pragma once

#include <boost/preprocessor/seq/for_each.hpp>

#include <chrono>
#include <unordered_map>

#include "all_type_variant.hpp"
#include "storage/base_value_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_iterables/base_segment_iterators.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

// Functions using strings are not optimized to support code specialization on Linux.
// This specialization issue came up with pr #933 (https://github.com/hyrise/hyrise/pull/933).
// The flag __attribute__((optnone)) ensures that clang does not optimize these functions.

// We need a boolean data type in the JitOperatorWrapper, but don't want to add it to
// DATA_TYPE_INFO to avoid costly template instantiations.
// See "all_type_variant.hpp" for details.
#define JIT_DATA_TYPE_INFO ((bool, Bool, "bool")) DATA_TYPE_INFO

// clang-format off
#define DATA_TYPE_INFO_WO_STRING        \
  ((int32_t,     Int,        "int"))    \
  ((int64_t,     Long,       "long"))   \
  ((float,       Float,      "float"))  \
  ((double,      Double,     "double"))  \
  ((bool, Bool, "bool"))

#define DATA_TYPE_INFO_INT        \
  ((int32_t,     Int,        "int"))

#define DATA_TYPE_INFO_WO_INT        \
  ((int32_t,     Int,        "int"))    \
  ((int64_t,     Long,       "long"))   \
  ((float,       Float,      "float"))  \
  ((double,      Double,     "double"))  \
  ((std::string, String,     "string"))  \
  ((bool, Bool, "bool"))
// Type          Enum Value   String
// clang-format on

#define JIT_VARIANT_VECTOR_MEMBER(r, d, type) \
  std::vector<BOOST_PP_TUPLE_ELEM(3, 0, type)> BOOST_PP_TUPLE_ELEM(3, 1, type);

#define JIT_VARIANT_VECTOR_GET(r, d, type)                                           \
  template <typename T = BOOST_PP_TUPLE_ELEM(3, 0, type), typename = typename std::enable_if_t<std::is_same_v<T, BOOST_PP_TUPLE_ELEM(3, 0, type)>>>        \
  __attribute__((always_inline)) BOOST_PP_TUPLE_ELEM(3, 0, type) get(const size_t index) const { \
    return BOOST_PP_TUPLE_ELEM(3, 1, type)[index];                                   \
  }

#define JIT_VARIANT_VECTOR_SET(r, d, type)                                                                     \
  template <typename T = BOOST_PP_TUPLE_ELEM(3, 0, type), typename = typename std::enable_if_t<std::is_same_v<T, BOOST_PP_TUPLE_ELEM(3, 0, type)>>>        \
  __attribute__((always_inline)) void set(const size_t index, const BOOST_PP_TUPLE_ELEM(3, 0, type) & value) { \
    BOOST_PP_TUPLE_ELEM(3, 1, type)[index] = value;                                                            \
  }

#define JIT_MEASURE 0

/*
#undef JIT_LAZY_LOAD
#define JIT_LAZY_LOAD 1
#undef JIT_OLD_LAZY_LOAD
#define JIT_OLD_LAZY_LOAD 0
*/

  #ifndef JIT_LOGICAL_PRUNING
#define JIT_LOGICAL_PRUNING 1
#endif

// Expression uses int32_t to store booleans (see src/lib/expression/evaluation/expression_evaluator.hpp)
using Bool = int32_t;
static constexpr auto DataTypeBool = DataType::Int;
using JitValueID = int32_t;
static constexpr auto DataTypeValueID = DataType::Int;


template <typename ValueType>
using Value = std::optional<ValueType>;
/*
struct Value : public std::optional<ValueType> {
  using value_type = ValueType;
  Value() : std::optional<ValueType>{std::nullopt} {}
  Value(const bool _is_null, const ValueType _value) : std::optional<ValueType>{!_is_null ? std::optional<ValueType>{_value} : std::nullopt} {}
  // bool is_null;
  // ValueType value;
  __attribute__((always_inline))
  bool is_null() const {
    return !std::optional<ValueType>::has_value();
  }
};
*/

/* A brief overview of the type system and the way values are handled in the JitOperatorWrapper:
 *
 * The JitOperatorWrapper performs most of its operations on variant values, since this allows writing generic operators with
 * an unlimited number of variably-typed operands (without the need for excessive templating).
 * While this sounds costly at first, the jit engine knows the actual type of each value in advance and replaces generic
 * operations with specialized versions for the concrete data types.
 *
 * This entails two important restrictions:
 * 1) There can be no temporary (=local) variant values. This is because we support std::string as a data type.
 *    Strings are not POD types and they thus require memory allocations, destructors and exception
 *    handling. The specialization engine is not able to completely optimize this overhead away, even if no strings are
 *    processed in an operation.
 * 2) The data type of each value needs to be stored separately from the value itself. This is because we want the
 *    specialization engine to know about data type, but not concrete values.
 *
 * Both restrictions are enforced using the same mechanism:
 * All values (and other data that must not be available to the specialization engine) are encapsulated in the
 * JitRuntimeContext. This context is only passed to the operator after code specialization has taken place.
 * The JitRuntimeContext contains a vector of variant values (called tuple) which the JitOperatorWrapper can access through
 * JitTupleValue instances.
 * The runtime tuple is created and destroyed outside the JitOperatorWrapper's scope, so no memory management is
 * required from within the operator.
 * Each JitTupleValue encapsulates information about how to access a single value in the runtime tuple. However, the
 * JitTupleValues are part of the JitOperatorWrapper and must thus not store a reference to the JitRuntimeContext. So while
 * they "know" how to access values in the runtime tuple, they do not have the means to do so.
 * Only by passing the runtime context to a JitTupleValue allows the value to be accessed.
 */

/* The JitVariantVector can be used in two ways:
 * 1) As a vector of variant values.
 *    Imagine you want to store a database row (aka tuple) of some table. Since each column of the table could have a
 *    different type, a std::vector<Variant> seems like a good choice. To store N values, the vector is resized to
 *    contain N slots and each value (representing one column) can be accessed by its index from [0, N).
 *    We do something slightly different here: Instead of one vector (where each vector element can hold any of a number
 *    of types), we create one strongly typed vector per data type. All of these vectors have size N, so each value
 *    (representing one column of the tuple) has a slot in each vector.
 *    Accessing the value at position P as an "int" will return the element at position P in the integer vector.
 *    There is no automatic type conversions happening here. Storing a value as "int" and reading it later as "double"
 *    won't work, since this accesses different memory locations in different vectors.
 *    This is not a problem, however, since the type of each value does not change throughout query execution.
 *
 * 2) As a variant vector.
 *    This data structure can also be used to store a vector of values of some unknown, but fixed type.
 *    Say you want to build an aggregate operator and need to store a column of aggregate values. All values
 *    produced will have the same type, but there is no way of knowing that type in advance.
 *    By adding a templated "grow_by_one" function to the implementation below, we can add an arbitrary number of
 *    elements to the std::vector of that data type. All other vectors will remain empty.
 *    This interpretation of the variant vector is used for the JitAggregate operator.
 */
class JitVariantVector {
 public:
  enum class InitialValue { Zero, MinValue, MaxValue };

  void resize(const size_t new_size);

  template <typename T, typename = typename std::enable_if_t<!std::is_scalar_v<T>>>
  __attribute__((optnone)) std::string get(const size_t index) const;
  // template <typename T, typename = typename std::enable_if_t<std::is_scalar_v<T>>>
  // __attribute__((always_inline))
  // T get(const size_t index) const;
  template <typename T, typename = typename std::enable_if_t<!std::is_scalar_v<T>>>
  __attribute__((optnone)) void set(const size_t index, const std::string& value);
  // template <typename T, typename = typename std::enable_if_t<std::is_scalar_v<T> && !std::is_same_v<T, int32_t>>>
  // __attribute__((always_inline))
  // void set(const size_t index, const T& value);
  /*
  template <typename T = int32_t, typename = typename std::enable_if_t<std::is_same_v<T, int32_t>>>
  __attribute__((always_inline))
  void set(const size_t index, const int32_t& value) {
    Int[index] = value;
  }
   */
  bool is_null(const size_t index) { return _is_null[index]; }
  void set_is_null(const size_t index, const bool is_null) { _is_null[index] = is_null; }

  // Adds an element to the internal vector for the specified data type.
  // The initial value can be set to Zero, MaxValue, or MinValue in a data type independent way.
  // The implementation will then construct the concrete initial value for the correct data type using
  // std::numeric_limits.
  template <typename T>
  size_t grow_by_one(const InitialValue initial_value);

  // Returns the internal vector for the specified data type.
  template <typename T>
  std::vector<T>& get_vector();

  // Returns the internal _is_null vector.
  std::vector<bool>& get_is_null_vector();

  BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_GET, _, DATA_TYPE_INFO_WO_STRING)
  BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_SET, _, DATA_TYPE_INFO_WO_STRING)

 private:
  BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_MEMBER, _, JIT_DATA_TYPE_INFO)
  std::vector<bool> _is_null;
};

class BaseJitSegmentReader;
class BaseJitSegmentWriter;

// The JitAggregate operator (and possibly future hashing based operators) require an efficient way to hash tuples
// across multiple columns (i.e., the key-type of the hashmap spans multiple columns).
// Since the number / data types of the columns are not known at compile time, we use a regular
// hashmap in combination with some JitVariantVectors to build the foundation for more flexible hashing.
// See the JitAggregate operator (jit_aggregate.hpp) for details.
// The runtime hashmap is part of the JitRuntimeContext to keep mutable state from the operators.
struct JitRuntimeHashmap {
  std::unordered_map<uint64_t, std::vector<size_t>> indices;
  std::vector<JitVariantVector> columns;
};

enum JitOperatorType {
  Read = 0,
  Write = 1,
  Aggregate = 2,
  Filter = 3,
  Compute = 4,
  Validate = 5,
  Limit = 6,
  ReadValue = 7,
  WriteOffset = 8,
  Size = 9
};

// The structure encapsulates all data available to the JitOperatorWrapper at runtime,
// but NOT during code specialization.
struct JitRuntimeContext {
  uint32_t chunk_size;
  ChunkOffset chunk_offset;
  JitVariantVector tuple;
  std::vector<std::shared_ptr<BaseJitSegmentReader>> inputs;
  std::vector<std::shared_ptr<BaseJitSegmentWriter>> outputs;
  JitRuntimeHashmap hashmap;
  Segments out_chunk;

  // Query transaction data required by JitValidate
  TransactionID transaction_id;
  CommitID snapshot_commit_id;

  // MVCC data from the current input chunk required by JitValidate
  // If the input table is a data table, its MVCC data is used.
  std::shared_ptr<const MvccData> mvcc_data;
  // The MVCC data is locked with a SharedScopedLockingPtr. The SharedScopedLockingPtr is stored within a unique ptr as
  // a SharedScopedLockingPtr has not the required copy assignment operator due to its reference data member.
  // The SharedScopedLockingPtr is only used to lock and not to access MVCC as this construct requires two ptr
  // dereferencings instead of one to access the MVCC data.
  std::unique_ptr<SharedScopedLockingPtr<const MvccData>> mvcc_data_lock;
  // The transaction ids are materialized as specialization cannot handle the atomics holding the transaction ids.
  pmr_vector<TransactionID> row_tids;

  // If the input table is a reference table, the position list of the first segment and the reference table are used to
  // lookup the corresponding mvcc data for each row.
  std::shared_ptr<const Table> referenced_table;
  std::shared_ptr<const PosList> pos_list;

  size_t limit_rows;
  ChunkID chunk_id;
  std::shared_ptr<PosList> output_pos_list;  // std::shared_ptr<PosList>  -  std::vector<RowID>
  pmr_vector<TransactionID> transaction_ids;
#if JIT_MEASURE
  std::chrono::nanoseconds times[JitOperatorType::Size];
  std::chrono::time_point<std::chrono::high_resolution_clock> begin_operator;
#endif
};

// The JitTupleValue represents a value in the runtime tuple.
// The JitTupleValue has information about the data type and index of the value it represents, but it does NOT have
// a reference to the runtime tuple with the actual values.
// However, this is enough for the jit engine to optimize any operation involving the value.
// It only knows how to access the value from the runtime context.
class JitTupleValue {
 public:
  JitTupleValue(const DataType data_type, const bool is_nullable, const size_t tuple_index);
  JitTupleValue(const std::pair<const DataType, const bool> data_type, const size_t tuple_index);

  DataType data_type() const;
  bool is_nullable() const;
  size_t tuple_index() const;

  template <typename T>
  __attribute__((always_inline))
  T get(JitRuntimeContext& context) const {
    return context.tuple.get<T>(_tuple_index);
  }

  template <typename T>
  __attribute__((always_inline))
  void set(const T value, JitRuntimeContext& context) const {
    context.tuple.set<T>(_tuple_index, value);
  }

  bool is_null(JitRuntimeContext& context) const {
    return _is_nullable && context.tuple.is_null(_tuple_index);
  }
  void set_is_null(const bool is_null, JitRuntimeContext& context) const {
    context.tuple.set_is_null(_tuple_index, is_null);
  }

  // Compares two JitTupleValue instances for equality. This method does NOT compare actual concrete values but only the
  // configuration (data type, nullability, tuple index) of the tuple values. I.e., two equal JitTupleValues refer to
  // the same value in a given JitRuntimeContext.
  bool operator==(const JitTupleValue& other) const;

  void set_type(const DataType data_type, const bool is_nullable) const {
    const_cast<DataType&>(_data_type) = data_type;
    const_cast<bool&>(_is_nullable) = is_nullable;
  }

 private:
  const DataType _data_type;
  const bool _is_nullable;
  const size_t _tuple_index;
};

// The JitHashmapValue represents a value in the runtime hashmap.
// The JitHashmapValue has information about the data type and index of the value it represents, but it does NOT have
// a reference to the runtime hashmap with the actual values.
// However, this is enough for the jit engine to optimize any operation involving the value.
// It only knows how to access the value from the runtime context.
// Compared to JitTupleValues, the hashmap values offer an additional dimension: The value is configured with a
// column_index, which references a column in the runtime hashmap. However, this column is not a single value but a
// vector. Whenever the JitHashmapValue is used in a computation, an additional row_index is required to specify the
// value inside the vector that should be used for the computation.
// Example: A JitHashmapValue may refer to an aggregate that is computed in the JitAggregate operator. The
// JitHashmapValue stores information about the data type, nullability of the aggregate, and the index of the column in
// the runtime hashmap that stores the computed aggregates.
// However, the JitAggregate operator computes multiple aggregate values - one for each group of tuples.
// The JitHashmapValue represents all these aggregates (i.e., the entire vector of aggregates) at once.
// The additional row_index is needed to access one specific aggregate (e.g., when updating aggregates during tuple
// processing).
class JitHashmapValue {
 public:
  JitHashmapValue(const DataType data_type, const bool is_nullable, const size_t column_index);

  DataType data_type() const;
  bool is_nullable() const;
  size_t column_index() const;

  template <typename T, typename = typename std::enable_if_t<!std::is_scalar_v<T>>>
  __attribute__((optnone)) std::string get(const size_t index, JitRuntimeContext& context) const {
    return context.hashmap.columns[_column_index].get<std::string>(index);
  }
  template <typename T, typename = typename std::enable_if_t<std::is_scalar_v<T>>>
  T get(const size_t index, JitRuntimeContext& context) const {
    return context.hashmap.columns[_column_index].get<T>(index);
  }
  template <typename T, typename = typename std::enable_if_t<!std::is_scalar_v<T>>>
  __attribute__((optnone)) void set(const std::string& value, const size_t index, JitRuntimeContext& context) const {
    context.hashmap.columns[_column_index].set<std::string>(index, value);
  }
  template <typename T, typename = typename std::enable_if_t<std::is_scalar_v<T>>>
  void set(const T value, const size_t index, JitRuntimeContext& context) const {
    context.hashmap.columns[_column_index].set<T>(index, value);
  }

  bool is_null(const size_t index, JitRuntimeContext& context) const;
  void set_is_null(const bool is_null, const size_t index, JitRuntimeContext& context) const;

 private:
  const DataType _data_type;
  const bool _is_nullable;
  const size_t _column_index;
};

enum class JitExpressionType {
  Addition,
  Column,
  Subtraction,
  Multiplication,
  Division,
  Modulo,
  Power,
  Equals,
  NotEquals,
  GreaterThan,
  GreaterThanEquals,
  LessThan,
  LessThanEquals,
  Between,
  Like,
  NotLike,
  And,
  Or,
  Not,
  IsNull,
  IsNotNull,
  In
};

bool jit_expression_is_binary(const JitExpressionType expression_type);

// cleanup
#undef JIT_VARIANT_VECTOR_GET
#undef JIT_VARIANT_VECTOR_SET
#undef JIT_VARIANT_VECTOR_MEMBER
#undef JIT_VARIANT_VECTOR_RESIZE

}  // namespace opossum
