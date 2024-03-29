//
// Created by lambda on 22-12-22.
//

#ifndef DBS_TUTORIAL_HASHJOINNODE_H
#define DBS_TUTORIAL_HASHJOINNODE_H

#include <memory>
#include <unordered_map>
#include <utility>

#include <record/Field.h>
#include <node/OpNode.h>
#include <system/WhereConditions.h>
#include <fmt/format.h>

typedef std::unordered_multimap<std::shared_ptr<Field>, std::shared_ptr<Record>, FieldHash, FieldEqual> FieldHashTable;

class HashJoinNode : public OpNode {
public:
    std::shared_ptr<JoinCondition> hash_cond;
    std::shared_ptr<JoinCondition> other_cond;
    FieldHashTable ht;
    bool called;
    FieldID left_pos;
    FieldID right_pos;

    static bool Acceptable(const std::shared_ptr<JoinCondition> &cond) {
        return cond->conditions.size() == 1 && dynamic_pointer_cast<EqCmp>(std::get<2>(cond->conditions[0]));
    }

    void Init() {
        called = true;
        auto [l_pos, r_pos, _] {hash_cond->conditions[0]};
        left_pos = l_pos;
        right_pos = r_pos;

        ht.clear();
        while (!children[1]->Over()) {
            for (const auto &r: children[1]->Next()) {
                ht.insert({r->fields[right_pos], r});
            }
        }
    }

    HashJoinNode(std::shared_ptr<OpNode> lhs, std::shared_ptr<OpNode> rhs, std::shared_ptr<JoinCondition> hash_cond, std::shared_ptr<JoinCondition> other_cond) :
            OpNode{{std::move(lhs), std::move(rhs)}}, hash_cond{std::move(hash_cond)}, other_cond{std::move(other_cond)}, called{false} {
        assert(Acceptable(this->hash_cond));
    }

    [[nodiscard]] bool Over() const override {
        return children[0]->Over();
    }

    void Reset() override {
        OpNode::Reset();
        called = false;
    }

    RecordList Next() override {
        if (!called) {
            Init();
        }
        RecordList ret;
        for (const auto &r: children[0]->Next()) {
            auto [it, end] {ht.equal_range(r->fields[left_pos])};
            while (it != end) {
                if(other_cond == nullptr || (*other_cond)(r, it->second)) {
                    auto joined_record{std::make_shared<Record>()};
                    std::move(r->fields.cbegin(), r->fields.cend(),
                              std::back_inserter(joined_record->fields));
                    std::copy(it->second->fields.cbegin(), it->second->fields.cend(),
                              std::back_inserter(joined_record->fields));
                    ret.push_back(std::move(joined_record));
                }
                ++it;
            }
        }
        return ret;
    }
};

#endif //DBS_TUTORIAL_HASHJOINNODE_H
