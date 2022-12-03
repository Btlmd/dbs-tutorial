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

    /**
     * Return true if no more results will be produced
     * @return
     */
    [[nodiscard]] virtual bool Over() const = 0;

    /**
     * Reset the node, so that it produces records from the very beginning
     */
    virtual void Reset() {
        for (auto &ch: children) {
            ch->Reset();
        }
    }

    /**
     * Produce a set of records
     * @return
     */
    virtual RecordList Next() = 0;

    /**
     * Produce all records
     * @return
     */
    virtual RecordList All() {
        RecordList records;
        while (!Over()) {
            RecordList ret{Next()};
            std::move(ret.begin(), ret.end(), std::back_inserter(records));
        }
        return std::move(records);
    }
};


#endif //DBS_TUTORIAL_OPNODE_H
