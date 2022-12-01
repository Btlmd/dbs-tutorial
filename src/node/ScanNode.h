//
// Created by lambda on 22-11-26.
//

#ifndef DBS_TUTORIAL_SCANNODE_H
#define DBS_TUTORIAL_SCANNODE_H

#include <node/OpNode.h>
#include <io/BufferSystem.h>
#include <system/WhereConditions.h>
#include <record/DataPage.h>
#include <record/TableMeta.h>

class TrivialScanNode : public OpNode {
public:
    TrivialScanNode(BufferSystem &buffer, std::shared_ptr<const TableMeta> table_meta, std::shared_ptr<FilterCondition> condition,
                    FileID page_file) : OpNode{{}}, buffer{buffer}, condition{std::move(condition)},
                                                            current_page{-1},
                                                            data_fd{page_file}, meta{std::move(table_meta)} {}

    RecordList Next() override {
        ++current_page;
        if (current_page == meta->page_count) {
            return {};
        }
        auto page = buffer.ReadPage(data_fd, current_page);
        DataPage dp{page, *meta};
        RecordList ret;
        for (FieldID i{0}; i < dp.header.slot_count; ++i) {
            auto record{dp.GetRecord(i)};
            if (record != nullptr && (condition == nullptr || (*condition)(record))) {
                ret.push_back(record);
            }
        }
        return std::move(ret);
    }

    ~TrivialScanNode() override = default;

private:
    BufferSystem &buffer;
    const std::shared_ptr<const TableMeta> meta;
    std::shared_ptr<FilterCondition> condition;
    PageID current_page;
    FileID data_fd;
};


class IndexScanNode : public OpNode {

};


#endif //DBS_TUTORIAL_SCANNODE_H
