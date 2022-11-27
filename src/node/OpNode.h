//
// Created by lambda on 22-11-26.
//

#ifndef DBS_TUTORIAL_OPNODE_H
#define DBS_TUTORIAL_OPNODE_H

#include <defines.h>
#include <record/Record.h>

#include <vector>

class OpNode {
public:
    OpNode(std::vector<OpNode *> children);
    virtual ~OpNode() = default;
    virtual RecordList Next() = 0;
};




#endif //DBS_TUTORIAL_OPNODE_H
