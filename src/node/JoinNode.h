//
// Created by lambda on 22-11-26.
//

#ifndef DBS_TUTORIAL_JOINNODE_H
#define DBS_TUTORIAL_JOINNODE_H

#include <node/OpNode.h>
#include <system/WhereConditions.h>

class JoinNode : public OpNode {
public:
    std::shared_ptr<JoinCondition> cond;
    RecordList rhs_records;
    bool read;

    JoinNode(std::shared_ptr<OpNode> lhs, std::shared_ptr<OpNode> rhs, std::shared_ptr<JoinCondition> cond) :
            OpNode{{std::move(lhs), std::move(rhs)}}, cond{std::move(cond)}, read{false} {}

    RecordList Next() override {
        // Brute force: read all rhs records into memory
        if (!read) {
            read = true;
            rhs_records = children[1]->All();
        }


        RecordList lhs_record_slice{children[0]->Next()};
        if (lhs_record_slice.empty()) {
            return {};
        }

        RecordList ret;
        for (const auto &lhs_record: lhs_record_slice) {
            for (const auto &rhs_record: rhs_records) {
                if (cond == nullptr || (*cond)(lhs_record, rhs_record)) {
                    auto joined_record{std::make_shared<Record>()};
                    std::move(lhs_record->fields.cbegin(), lhs_record->fields.cend(),
                              std::back_inserter(joined_record->fields));
                    std::copy(rhs_record->fields.cbegin(), rhs_record->fields.cend(),
                              std::back_inserter(joined_record->fields));
                    ret.push_back(std::move(joined_record));
                }
            }
        }
        return std::move(ret);
    }
};


#endif //DBS_TUTORIAL_JOINNODE_H
