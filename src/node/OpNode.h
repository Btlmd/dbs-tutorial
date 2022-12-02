//
// Created by lambda on 22-11-26.
//

#ifndef DBS_TUTORIAL_OPNODE_H
#define DBS_TUTORIAL_OPNODE_H

#include <defines.h>
#include <record/Record.h>

#include <memory>
#include <vector>

class OpNode {
public:
    std::vector<std::shared_ptr<OpNode>> children;

    explicit OpNode(std::vector<std::shared_ptr<OpNode>> children) : children{std::move(children)} {}

    virtual ~OpNode() = default;

    virtual RecordList Next() = 0;

    virtual RecordList All() {
        RecordList records;
        while (true) {
            RecordList ret{Next()};
            if (ret.empty()) {
                break;
            }
            std::move(ret.begin(), ret.end(), std::back_inserter(records));
        }
        return std::move(records);
    }
};


#endif //DBS_TUTORIAL_OPNODE_H
