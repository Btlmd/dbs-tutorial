//
// Created by lambda on 22-11-30.
//

#ifndef DBS_TUTORIAL_WHERECONDITIONS_H
#define DBS_TUTORIAL_WHERECONDITIONS_H

#include <defines.h>
#include <record/Field.h>
#include <record/Record.h>
#include <node/OpNode.h>
#include <exception/OperationException.h>

#include <memory>
#include <utility>
#include <algorithm>
#include <regex>

#include <boost/algorithm/string.hpp>

/**
 * Algebraic Compare Operators
 */
class Cmp {
public:
    virtual bool operator()(const std::shared_ptr<Field> &lhs, const std::shared_ptr<Field> &rhs) const = 0;

    static void flip(std::shared_ptr<Cmp> &ptr);
};

typedef std::vector<std::shared_ptr<Cmp>> CmpList;

class EqCmp : public Cmp {
public:
    bool operator()(const std::shared_ptr<Field> &lhs, const std::shared_ptr<Field> &rhs) const override {
        return *lhs == *rhs;
    }
};

class LeCmp : public Cmp {
public:
    bool operator()(const std::shared_ptr<Field> &lhs, const std::shared_ptr<Field> &rhs) const override {
        return *lhs < *rhs;
    }
};

class GeCmp : public Cmp {
public:
    bool operator()(const std::shared_ptr<Field> &lhs, const std::shared_ptr<Field> &rhs) const override {
        return *lhs > *rhs;
    }
};

class LeqCmp : public Cmp {
public:
    bool operator()(const std::shared_ptr<Field> &lhs, const std::shared_ptr<Field> &rhs) const override {
        return *lhs <= *rhs;
    }
};

class GeqCmp : public Cmp {
public:
    bool operator()(const std::shared_ptr<Field> &lhs, const std::shared_ptr<Field> &rhs) const override {
        return *lhs >= *rhs;
    }
};

class NeqCmp : public Cmp {
public:
    bool operator()(const std::shared_ptr<Field> &lhs, const std::shared_ptr<Field> &rhs) const override {
        return *lhs != *rhs;
    }
};

inline void Cmp::flip(std::shared_ptr<Cmp> &ptr) {
    if (std::dynamic_pointer_cast<LeCmp>(ptr)) {
        ptr = std::make_shared<GeCmp>();
        return;
    }
    if (std::dynamic_pointer_cast<GeCmp>(ptr)) {
        ptr = std::make_shared<LeCmp>();
        return;
    }
    if (std::dynamic_pointer_cast<LeqCmp>(ptr)) {
        ptr = std::make_shared<GeCmp>();
        return;
    }
    if (std::dynamic_pointer_cast<GeqCmp>(ptr)) {
        ptr = std::make_shared<LeCmp>();
        return;
    }
}

enum class ConditionType {
    FILTER = 0,
    JOIN = 1
};

/**
 * Represent any condition in a where clause
 */
class Condition {
public:
    ConditionType type;

    explicit Condition(ConditionType type) : type{type} {}
};

/**
 * Record Filter Condition
 */
class FilterCondition : public Condition {
public:
    TableID table_id;

    explicit FilterCondition(TableID table_id) : Condition{ConditionType::FILTER}, table_id{table_id} {}

    virtual bool operator()(const std::shared_ptr<Record> &record) const = 0;

    virtual ~FilterCondition() = default;
};

typedef std::vector<std::shared_ptr<FilterCondition>> FilterConditionList;

/**
 * Compare field with a given value
 */
class ValueCompareCondition : public FilterCondition {
public:
    const std::shared_ptr<Field> rhs;
    const FieldID field_position;
    const std::shared_ptr<Cmp> comparer;

    ValueCompareCondition(std::shared_ptr<Field> rhs, TableID table_id, FieldID pos, const std::shared_ptr<Cmp> &cmp) :
            FilterCondition{table_id}, rhs{std::move(rhs)}, field_position{pos}, comparer{std::move(cmp)} {}

    bool operator()(const std::shared_ptr<Record> &record) const override {
        return (*comparer)(record->fields[field_position], rhs);
    }
};

typedef std::vector<std::shared_ptr<Field>> ValueList;

/**
 * Check whether the field value match any of the given list of values
 */
class ValueInListCondition : public FilterCondition {
public:
    const ValueList value_list;
    const FieldID field_position;

    ValueInListCondition(ValueList value_list, TableID table_id, FieldID pos) :
            FilterCondition{table_id}, value_list{std::move(value_list)}, field_position{pos} {}

    bool operator()(const std::shared_ptr<Record> &record) const override {
        if (value_list.size() == 0) {
            return false;
        }
        EqCmp comparer;
        const auto &record_field{record->fields[field_position]};
        return std::ranges::any_of(value_list.cbegin(), value_list.cend(),
                                   [&record_field, &comparer](const auto &f) { return comparer(record_field, f); });
    }
};

/**
 * Compare two fields in a record
 */
class FieldCmpCondition : public FilterCondition {
public:
    const FieldID lhs_pos;
    const FieldID rhs_pos;
    const std::shared_ptr<Cmp> comparer;

    FieldCmpCondition(TableID table_id, FieldID lhs_pos, FieldID rhs_pos, std::shared_ptr<Cmp> cmp) :
            FilterCondition{table_id}, lhs_pos{lhs_pos}, rhs_pos{rhs_pos}, comparer{std::move(cmp)} {}

    bool operator()(const std::shared_ptr<Record> &record) const override {
        return (*comparer)(record->fields[lhs_pos], record->fields[rhs_pos]);
    }
};

class NullCompCondition : public FilterCondition {
public:
    const bool filter_not_null;  // when true, filter not null records
    const FieldID field_position;

    NullCompCondition(TableID table_id, FieldID pos, bool filter_not_null) :
            FilterCondition{table_id}, field_position{pos}, filter_not_null{filter_not_null} {}

    bool operator()(const std::shared_ptr<Record> &record) const override {
        return filter_not_null ^ record->fields[field_position]->is_null;
    }
};

class LikeCondition : public FilterCondition {
public:
    std::regex pattern;
    const FieldID field_position;

    LikeCondition(const std::string &sql_pattern, TableID table_id, FieldID pos) : FilterCondition{table_id},
                                                                                   field_position{pos} {
        /**
         * Escape the user input
         * Reference: https://stackoverflow.com/questions/40195412/c11-regex-search-for-exact-string-escape
         * TODO: is this escape correct?
         */
        std::regex special_char{R"([-[\]{}()*+?.,\^$|#\s])"};
        std::string input_pattern{std::regex_replace(sql_pattern, special_char, R"(\$&)")};

        boost::replace_all(input_pattern, "%", ".*");
        boost::replace_all(input_pattern, "_", ".");
        input_pattern = "^" + input_pattern + "$";

        Trace("LikeCondition constructed as " << input_pattern);

        pattern = input_pattern;
    }

    bool operator()(const std::shared_ptr<Record> &record) const override {
        auto string_field{std::dynamic_pointer_cast<String>(record->fields[field_position])};
        return std::regex_match(string_field->data, pattern);
    }
};

/**
 * Apply a series of conjunct filter conditions on the record
 */
class AndCondition : public FilterCondition {
public:
    FilterConditionList conditions;

    explicit AndCondition(FilterConditionList conditions, TableID table_id) :
            FilterCondition{table_id}, conditions{std::move(conditions)} {
        assert(std::ranges::all_of(this->conditions.begin(), this->conditions.end(),
                                   [table_id](const auto &cond) { return cond->table_id == table_id; }));
    }

    bool operator()(const std::shared_ptr<Record> &record) const override {
        return std::ranges::all_of(conditions.begin(), conditions.end(),
                                   [&record](const auto &cond) { return (*cond)(record); });
    }

    [[nodiscard]] std::size_t Size() const {
        return conditions.size();
    }

    void AddCondition(std::shared_ptr<FilterCondition> cond) {
        conditions.push_back(std::move(cond));
    }
};

typedef std::pair<std::vector<std::string>, std::shared_ptr<OpNode>> SelectPlan;

inline ValueList ToValueList(const SelectPlan &select_plan) {
    if (select_plan.first.size() != 1) {
        throw OperationError{"Operand should contain only 1 column"};
    }
    auto records = select_plan.second->All();

    ValueList value_list;
    for (const auto &record: records) {
        assert(record->fields.size() == 1);
        value_list.push_back(std::move(record->fields[0]));
    }
    return std::move(value_list);
}

/**
 * In subquery condition
 */
class InSubQueryCondition : public ValueInListCondition {
public:
    InSubQueryCondition(const SelectPlan &select_plan, TableID table_id, FieldID pos) :
            ValueInListCondition{ToValueList(select_plan), table_id, pos} {}
};

/**
 * compare with a subquery condition
 */
class CompareSubQueryCondition : public FilterCondition {
public:
    FieldID field_position;
    std::shared_ptr<Field> value;
    std::shared_ptr<Cmp> cmp;

    CompareSubQueryCondition(const SelectPlan &select_plan, TableID table_id, FieldID pos, std::shared_ptr<Cmp> cmp) :
            FilterCondition{table_id}, field_position{pos}, cmp{std::move(cmp)} {
        auto value_list{ToValueList(select_plan)};
        if (value_list.size() > 1) {
            throw OperationError{"Subquery returns more than 1 row"};
        }
        if (value_list.size() == 1) {
            value = value_list[0];
        } else {
            value = nullptr;
        }
    }

    bool operator()(const std::shared_ptr<Record> &record) const override {
        if (value == nullptr) {
            return false;
        }
        const auto &record_field{record->fields[field_position]};
        return (*cmp)(record_field, value);
    }

};

typedef std::pair<TableID, TableID> JoinPair;
typedef std::tuple<FieldID, FieldID, std::shared_ptr<Cmp>> JoinCond;

/**
 * Ensure an ascending pair
 * @param original
 * @return
 */
inline JoinPair make_ordered(const JoinPair &original) {
    assert(original.first != original.second);
    if (original.first < original.second) {
        return original;
    }
    return std::make_pair(original.second, original.first);
}

/**
 * Record Join Condition, with conjunct comparers (since disjunction is not yet supported in the grammar)
 */
class JoinCondition : public Condition {
public:
    JoinPair tables;
    std::vector<JoinCond> conditions;

    explicit JoinCondition(std::vector<JoinCond> conditions, JoinPair tables) :
            Condition{ConditionType::JOIN}, conditions{std::move(conditions)},
            tables{std::move(tables)} {
        DebugLog << "Join Condition on tables (" << this->tables.first << ", "<< this->tables.second << ")";
        for (const auto&[l, r, cond]: this->conditions) {
            DebugLog << " - (" << l << ", " << r << ")";
        }
    }

    bool operator()(const std::shared_ptr<Record> &lhs, const std::shared_ptr<Record> &rhs) const {
        return std::ranges::all_of(conditions.begin(), conditions.end(), [&lhs, &rhs](const auto &cond) {
            return (*std::get<2>(cond))(lhs->fields[std::get<0>(cond)], rhs->fields[std::get<1>(cond)]);
        });  // note that `all_of` return true for an empty container
    }

    void Swap() {
        for (auto &[lhs, rhs, cond_p]: conditions) {
            std::swap(lhs, rhs);
            Cmp::flip(cond_p);
        }
        std::swap(tables.first, tables.second);
    }

    void MatchSeq(const JoinPair &other) {
        assert(make_ordered(other) == make_ordered(tables));
        if (other.first != tables.first) {
            Swap();
        }
    }

    static void Merge(std::shared_ptr<JoinCondition> &dst, const std::shared_ptr<JoinCondition> &other) {
        if (dst != other) {
//            assert(dst->tables == other->tables);
            std::copy(other->conditions.cbegin(), other->conditions.cend(), std::back_inserter(dst->conditions));
        }
    }

    static std::shared_ptr<JoinCondition>
    Merge(const std::vector<std::shared_ptr<JoinCondition>> &conditions, const JoinPair& tables) {
        std::vector<JoinCond> accu;
        for (const auto &cond: conditions) {
            std::copy(cond->conditions.begin(), cond->conditions.end(), std::back_inserter(accu));
        }
        return std::make_shared<JoinCondition>(std::move(accu), tables);
    }
};

/**
 * Make a condition shared_ptr
 * @tparam T
 * @tparam Args
 * @param args
 * @return
 */
template<typename T, typename... Args>
inline std::shared_ptr<Condition>
make_cond(Args &&... args) {
    return std::static_pointer_cast<Condition>(std::make_shared<T>(std::forward<Args>(args)...));
}

#endif //DBS_TUTORIAL_WHERECONDITIONS_H
