//
// Created by lambda on 22-11-20.
//

#include "Field.h"
#include <index/IndexField.h>
#include <system/WhereConditions.h>

std::pair<int, std::pair<std::shared_ptr<IndexField>, std::shared_ptr<IndexField>>> IndexKey::FilterCondition(const std::shared_ptr<AndCondition>& and_cond) {
    if (and_cond == nullptr) {
        return {0, {}};
    }

    std::unordered_map<FieldID, std::vector<std::shared_ptr<Field>>> index_fields_leq;  // we left a vector for each index
    std::unordered_map<FieldID, std::vector<std::shared_ptr<Field>>>index_fields_geq;  // we left a vector for each index
    index_fields_leq.clear(); index_fields_geq.clear();


    // We consider these two conds
    std::shared_ptr<ValueCompareCondition> value_cond;
    std::shared_ptr<NullCompCondition> null_cond;

    // For value_cond, we consider following CMPs
    std::shared_ptr<EqCmp> eq_cmp;
    std::shared_ptr<LeCmp> le_cmp;  // maybe there is a typo, i think he means lt and gt, lol
    std::shared_ptr<GeCmp> ge_cmp;
    std::shared_ptr<LeqCmp> leq_cmp;
    std::shared_ptr<GeqCmp> geq_cmp;
    // Yes, we only ignore the NeqCmp

    std::shared_ptr<Int> int_val;

    for (auto &cond : and_cond->conditions) {
        if (value_cond = std::dynamic_pointer_cast<ValueCompareCondition>(cond)) {
            auto field_id = value_cond->field_position;
            FieldID exist_pos = Find(field_id);
            if (exist_pos < 0) {
                continue;
            }

            int_val = std::dynamic_pointer_cast<Int>(value_cond->rhs);
            if (int_val == nullptr || int_val->is_null) {
                continue;
            }

            if ((eq_cmp = std::dynamic_pointer_cast<EqCmp>(value_cond->comparer))) {
                index_fields_leq[exist_pos].push_back(value_cond->rhs);
                index_fields_geq[exist_pos].push_back(value_cond->rhs);
            }
            else if ((le_cmp = std::dynamic_pointer_cast<LeCmp>(value_cond->comparer))) {
                index_fields_leq[exist_pos].push_back(std::make_shared<Int>(int_val->value - 1));
                index_fields_geq[exist_pos].push_back(std::make_shared<Int>(std::numeric_limits<int>::min()));
            }
            else if ((ge_cmp = std::dynamic_pointer_cast<GeCmp>(value_cond->comparer))) {
                index_fields_leq[exist_pos].push_back(std::make_shared<Int>(std::numeric_limits<int>::max()));
                index_fields_geq[exist_pos].push_back(std::make_shared<Int>(int_val->value + 1));
            }
            else if ((leq_cmp = std::dynamic_pointer_cast<LeqCmp>(value_cond->comparer))) {
                index_fields_leq[exist_pos].push_back(value_cond->rhs);
                index_fields_geq[exist_pos].push_back(std::make_shared<Int>(std::numeric_limits<int>::min()));
            }
            else if ((geq_cmp = std::dynamic_pointer_cast<GeqCmp>(value_cond->comparer))) {
                index_fields_leq[exist_pos].push_back(std::make_shared<Int>(std::numeric_limits<int>::max()));
                index_fields_geq[exist_pos].push_back(value_cond->rhs);
            }

        } else if ((null_cond = std::dynamic_pointer_cast<NullCompCondition>(cond))) {
            auto field_id = null_cond->field_position;
            FieldID exist_pos = Find(field_id);
            if (exist_pos < 0) {
                continue;
            }

            // Only INT type columns can be added index. Skip check.
            if (null_cond->filter_not_null) {
                index_fields_leq[exist_pos].push_back(std::make_shared<Int>(std::numeric_limits<int>::max()));
                index_fields_geq[exist_pos].push_back(std::make_shared<Int>(std::numeric_limits<int>::min()));
            } else {
                auto _max_record = std::make_shared<Int>(std::numeric_limits<int>::max());
                _max_record->is_null = true;
                index_fields_leq[exist_pos].push_back(_max_record);

                auto _min_record = std::make_shared<Int>(std::numeric_limits<int>::min());
                _min_record->is_null = true;
                index_fields_geq[exist_pos].push_back(_min_record);
            }

        }
    }

    // Now we decide if we can use this index, and if we can, we return the index fields [key_start, key_end]
    if (index_fields_leq[0].size() == 0) {
        return {0, {}};
    }

    // Else we can use this index.
    int max_len = 0;
    for (int i = 0; i < field_count; ++i) {
        if (index_fields_leq[i].size() != 0) {
            max_len++;
        } else {
            break;
        }
    }

    // Construct [key_start, key_end]
    std::vector<std::shared_ptr<IndexField>> index_fields;
    std::vector<std::shared_ptr<IndexField>> key_starts, key_ends;
    for (int dim = 0; dim < max_len; ++dim) {
        index_fields.clear();
        for (auto &field : index_fields_leq[dim]) {
            index_fields.push_back(IndexINT::FromDataField({field}));
        }
        auto key_end = *std::min_element(index_fields.begin(), index_fields.end(),
                                         [&](const std::shared_ptr<IndexField>& a, const std::shared_ptr<IndexField>& b) {
                                             return (*a) < (*b);
                                         });
        key_ends.push_back(key_end);

        index_fields.clear();
        for (auto &field : index_fields_geq[dim]) {
            index_fields.push_back(IndexINT::FromDataField({field}));
        }
        auto key_start = *std::max_element(index_fields.begin(), index_fields.end(),
                                           [&](const std::shared_ptr<IndexField>& a, const std::shared_ptr<IndexField>& b) {
                                               return (*a) < (*b);
                                           });
        key_starts.push_back(key_start);
    }
    for (int dim = max_len; dim < field_count; ++dim) {
        key_starts.push_back(std::make_shared<IndexINT>(std::numeric_limits<int>::min(), true));
        key_ends.push_back(std::make_shared<IndexINT>(std::numeric_limits<int>::max(), false));
    }

    if (field_count == 1) {
        return {1, {key_starts[0], key_ends[0]}};
    } else if (field_count == 2) {
        auto key_start = std::make_shared<IndexINT2>(
            std::reinterpret_pointer_cast<IndexINT>(key_starts[0])->value,
            std::reinterpret_pointer_cast<IndexINT>(key_starts[0])->is_null,
            std::reinterpret_pointer_cast<IndexINT>(key_starts[1])->value,
            std::reinterpret_pointer_cast<IndexINT>(key_starts[1])->is_null
        );
        auto key_end = std::make_shared<IndexINT2>(
            std::reinterpret_pointer_cast<IndexINT>(key_ends[0])->value,
            std::reinterpret_pointer_cast<IndexINT>(key_ends[0])->is_null,
            std::reinterpret_pointer_cast<IndexINT>(key_ends[1])->value,
            std::reinterpret_pointer_cast<IndexINT>(key_ends[1])->is_null
        );
        return {2, {key_start, key_end}};
    } else {
        assert(false);
        return {0, {}};
    }

}

std::hash<std::string> String::hash;