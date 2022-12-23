//
// Created by lambda on 22-12-22.
//

#ifndef DBS_TUTORIAL_SORTNODE_H
#define DBS_TUTORIAL_SORTNODE_H

#include <node/OpNode.h>
#include <record/Field.h>

#include <memory>
#include <functional>

class SortNode : public OpNode {
public:
    SortNode(std::shared_ptr<OpNode> downstream, std::vector<FieldID> fields, bool desc = false) :
            OpNode({std::move(downstream)}), desc{desc}, called{false}, fields{std::move(fields)} {}

    bool Over() const override {
        return called;
    }

    void Reset() override {
        OpNode::Reset();
        called = false;
    }


    RecordList Next() override {
        called = true;
        RecordList records{children[0]->All()};

        FieldCompare comp;
        std::sort(records.begin(), records.end(),
                  [this, &comp](const std::shared_ptr<Record> &lhs, const std::shared_ptr<Record> &rhs) -> bool {
                      for (const auto i: fields) {
                          if (comp(lhs->fields[i], rhs->fields[i])) {
                              return true;  // lt
                          }
                          if (comp(rhs->fields[i], lhs->fields[i])) {
                              return false;  // gt
                          }
                      }
                      return false;  // eq
                  });
        return std::move(records);
    }

private:
    const bool desc;
    bool called;
    const std::vector<FieldID> fields;
};

#endif //DBS_TUTORIAL_SORTNODE_H
