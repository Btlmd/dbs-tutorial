//
// Created by lambda on 22-11-26.
//

#ifndef DBS_TUTORIAL_JOINNODE_H
#define DBS_TUTORIAL_JOINNODE_H

#include <node/OpNode.h>
#include <system/WhereConditions.h>
#include <fmt/format.h>

class JoinNode : public OpNode {
public:
    std::shared_ptr<JoinCondition> cond;
    RecordList rhs_records;
    bool uncalled;

    JoinNode(std::shared_ptr<OpNode> lhs, std::shared_ptr<OpNode> rhs, std::shared_ptr<JoinCondition> cond) :
            OpNode{{std::move(lhs), std::move(rhs)}}, cond{std::move(cond)}, uncalled{true} {}

    [[nodiscard]] bool Over() const override {
        return children[0]->Over();
    }

    void Reset() override {
        OpNode::Reset();
        uncalled = true;
        rhs_records.clear();
    }

    [[nodiscard]] RecordList Join(const RecordList &lhs, const RecordList &rhs) const {
        RecordList ret;
        for (const auto &lhs_record: lhs) {
            for (const auto &rhs_record: rhs) {
//                TraceLog << "Join on " << lhs_record->Repr() << " with " << rhs_record->Repr();
                if (cond == nullptr || (*cond)(lhs_record, rhs_record)) {
                    auto joined_record{std::make_shared<Record>()};
                    std::move(lhs_record->fields.cbegin(), lhs_record->fields.cend(),
                              std::back_inserter(joined_record->fields));
                    std::copy(rhs_record->fields.cbegin(), rhs_record->fields.cend(),
                              std::back_inserter(joined_record->fields));
                    ret.push_back(std::move(joined_record));
//                    TraceLog << "- true";
                } else {
//                    TraceLog << " - false";
                }
            }
        }
        return std::move(ret);
    }

    RecordList Next() override {
        if (uncalled) {
            uncalled = false;
            rhs_records = children[1]->All();
        }
        return Join(children[0]->Next(), rhs_records);
    }
};


#endif //DBS_TUTORIAL_JOINNODE_H
