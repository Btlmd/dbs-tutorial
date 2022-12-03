//
// Created by lambda on 22-12-2.
//

#ifndef DBS_TUTORIAL_AGGREGATENODE_H
#define DBS_TUTORIAL_AGGREGATENODE_H

#include <node/OpNode.h>
#include <system/Column.h>
#include <memory>
#include <boost/algorithm/string.hpp>

class AggregateNode : public OpNode {
public:
    std::vector<std::shared_ptr<Column>> aggregators;

    std::vector<FieldID> target;

    FieldID group_by_col;

    bool aggregated;

    AggregateNode(std::shared_ptr<OpNode> downstream, std::vector<std::shared_ptr<Column>> aggregators,
                  FieldID group_by_col, std::vector<FieldID> target) :
            OpNode{{std::move(downstream)}}, aggregators{std::move(aggregators)},
            group_by_col{group_by_col}, aggregated{false}, target{std::move(target)} {}

    /**
     * Returns true if there is at least one not NULL value
     * @return
     */
    [[nodiscard]] bool SomeToAggregate(RecordList::iterator begin, RecordList::iterator end, FieldID i) const {
        for (auto ptr{begin}; ptr != end; ++ptr) {
            if ((*ptr)->fields[target[i]] && !(*ptr)->fields[target[i]]->is_null) {
                return true;
            }
        }
        return false;
    }

    void Reset() override {
        aggregated = false;
    }

    [[nodiscard]] bool Over() const override {
        return aggregated;
    }

    RecordList Next() override {
        aggregated = true;

        // sort downstream records
        RecordList records{children[0]->All()};
        auto record_len{static_cast<FieldID>(aggregators.size())};
        if (group_by_col >= 0) {
            // sort by the last field
            std::sort(records.begin(), records.end(),
                      [this](const std::shared_ptr<Record> &lhs, const std::shared_ptr<Record> &rhs) -> bool {
                          // make NULL the smallest
                          auto l_f{lhs->fields[group_by_col]};
                          auto r_f{rhs->fields[group_by_col]};
                          if (l_f->is_null && r_f->is_null) {
                              return false;
                          }
                          if (l_f->is_null && !r_f->is_null) {
                              return true;
                          }
                          if (!l_f->is_null && r_f->is_null) {
                              return false;
                          }
                          return *l_f < *r_f;
                      });
        }

//#ifdef DEBUG
//        {
//            TraceLog << "Aggregation: sorted records";
//            for (const auto &r: records) {
//                TraceLog << boost::algorithm::join(r->ToString(), ", ");
//            }
//
//        };
//#endif

        RecordList ret;

        auto curr{records.begin()}, boarder{records.begin()}, ptr{records.begin()};
        while (curr != records.end()) {
            while (
                    boarder != records.end() &&
                    (
                            group_by_col < 0  // not group by
                            ||
                            (  // equal value, including both null
                                    *(*boarder)->fields[group_by_col] == *(*curr)->fields[group_by_col] ||
                                    ((*boarder)->fields[group_by_col]->is_null &&
                                     (*curr)->fields[group_by_col]->is_null)
                            )

                    )
                    ) {
                ++boarder;
//                TraceLog << "Aggregate ptr step";
            }

            // aggregate in the segment
            std::vector<std::shared_ptr<Field>> ag;
            for (FieldID i{0}; i < aggregators.size(); ++i) {
                auto &col{aggregators[i]};
                if (col->type == ColumnType::COUNT_REC) {
                    ag.push_back(std::make_shared<Int>(std::distance(curr, boarder)));
                    continue;
                }

                auto some{SomeToAggregate(curr, boarder, i)};
                auto typed{Field::MakeNull(col->field_meta->type, col->field_meta->max_size)};

                switch (col->type) {
                    case ColumnType::BASIC:
                        ag.push_back((*curr)->fields[target[i]]);
                        break;
                    case ColumnType::COUNT:
                        ag.push_back(std::make_shared<Int>(
                                std::count_if(curr, boarder, [this, i](const std::shared_ptr<Record> &lhs) -> bool {
                                    return !lhs->fields[target[i]]->is_null;
                                })));
                        break;
                    case ColumnType::MAX:
                        for (ptr = curr; ptr != boarder; ++ptr) {
                            if (typed->is_null || *(*ptr)->fields[target[i]] > *typed) {
                                typed = (*ptr)->fields[target[i]];
                            }
                        }
                        ag.push_back(typed);
                        break;
                    case ColumnType::MIN:
                        for (ptr = curr; ptr != boarder; ++ptr) {
                            if (typed->is_null || *(*ptr)->fields[target[i]] < *typed) {
                                typed = (*ptr)->fields[target[i]];
                            }
                        }
                        ag.push_back(typed);
                        break;
                    case ColumnType::AVG:
                        if (!some) {
                            ag.push_back(typed);
                        } else if (col->field_meta->type != FieldType::INT &&
                                   col->field_meta->type != FieldType::FLOAT) {
                            ag.push_back(std::make_shared<Int>(0));
                        } else {
                            double sum{0};
                            int count{0};
                            for (ptr = curr; ptr != boarder; ++ptr) {
                                if ((*ptr)->fields[target[i]]->is_null) {
                                    continue;
                                }
                                if (col->field_meta->type == FieldType::INT) {
                                    sum += std::dynamic_pointer_cast<Int>((*ptr)->fields[target[i]])->value;
                                } else if (col->field_meta->type == FieldType::FLOAT) {
                                    sum += std::dynamic_pointer_cast<Float>((*ptr)->fields[target[i]])->value;
                                } else {
                                    assert(false);
                                }
                                ++count;
                            }
                            assert(count != 0);
                            ag.push_back(std::make_shared<Float>(sum / count));
                        }
                        break;
                    case ColumnType::SUM:
                        if (!some) {
                            ag.push_back(typed);
                        } else if (col->field_meta->type != FieldType::INT &&
                                   col->field_meta->type != FieldType::FLOAT) {
                            ag.push_back(std::make_shared<Int>(0));
                        } else {
                            if (col->field_meta->type == FieldType::INT) {
                                int sum{0};
                                for (ptr = curr; ptr != boarder; ++ptr) {
                                    if ((*ptr)->fields[target[i]]->is_null) {
                                        continue;
                                    }
                                    sum += std::dynamic_pointer_cast<Int>((*ptr)->fields[target[i]])->value;
                                }
                                ag.push_back(std::make_shared<Int>(sum));
                            } else if (col->field_meta->type == FieldType::FLOAT) {
                                double sum{0};
                                for (ptr = curr; ptr != boarder; ++ptr) {
                                    if ((*ptr)->fields[target[i]]->is_null) {
                                        continue;
                                    }
                                    sum += std::dynamic_pointer_cast<Float>((*ptr)->fields[target[i]])->value;
                                }
                                ag.push_back(std::make_shared<Float>(sum));
                            } else {
                                assert(false);
                            }
                        }
                        break;
                    case ColumnType::COUNT_REC:
                        assert(false); // already been taken care of
                }
            }
            ret.push_back(std::make_shared<Record>(ag));
            curr = boarder;
        }
        return std::move(ret);
    }

};


#endif //DBS_TUTORIAL_AGGREGATENODE_H
