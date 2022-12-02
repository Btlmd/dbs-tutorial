//
// Created by lambda on 22-11-26.
//

#ifndef DBS_TUTORIAL_PROJECTNODE_H
#define DBS_TUTORIAL_PROJECTNODE_H

#include <defines.h>
#include <node/OpNode.h>

#include <vector>

class ProjectNode: public OpNode {
public:
    std::vector<FieldID> target;
    ProjectNode(std::shared_ptr<OpNode> downstream, std::vector<FieldID> target):
        OpNode{{std::move(downstream)}}, target{std::move(target)} {}

    RecordList Next() override {
        // NOTE: `target` may contain duplicate entries
        RecordList downstream{children[0]->Next()};
        RecordList ret;
        for (const auto &record: downstream) {
            std::vector<std::shared_ptr<Field>> fields;
            for (const auto &pos: target) {
                fields.push_back(record->fields[pos]);
            }
            ret.push_back(std::make_shared<Record>(std::move(fields)));
        }
        return ret;
    }

};


#endif //DBS_TUTORIAL_PROJECTNODE_H
