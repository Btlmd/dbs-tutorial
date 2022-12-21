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
#include <index/IndexFile.h>

class TrivialScanNode : public OpNode {
public:
    TrivialScanNode(BufferSystem &buffer, std::shared_ptr<const TableMeta> table_meta,
                    std::shared_ptr<FilterCondition> condition,
                    FileID page_file) : OpNode{{}}, buffer{buffer}, condition{std::move(condition)},
                                        current_page{-1},
                                        data_fd{page_file}, meta{std::move(table_meta)} {}

    void Reset() override {
        OpNode::Reset();
        current_page = -1;
    }

    [[nodiscard]] bool Over() const override {
        assert(current_page < meta->data_page_count);
        return current_page == meta->data_page_count - 1;
    }

    RecordList Next() override {
        ++current_page;
        auto page = buffer.ReadPage(data_fd, current_page);
        DataPage dp{page, *meta};
        RecordList ret;
        for (FieldID i{0}; i < dp.header.slot_count; ++i) {
            auto record{dp.Select(i)};
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
    const std::shared_ptr<FilterCondition> condition;
    PageID current_page;
    FileID data_fd;
};

/*
 ** Given [key_start, key_end], use index_file to find location at key_start first
 * Once the record > key_end, stop
 */
class IndexScanNode : public OpNode {
   public:
    IndexScanNode(BufferSystem& buffer, std::shared_ptr<const TableMeta> table_meta,
                    std::shared_ptr<FilterCondition> condition,
                    FileID page_file, IndexFile& index_file, const std::shared_ptr<IndexField>& key_start, const std::shared_ptr<IndexField>& key_end) :
                                            OpNode{{}}, buffer{buffer}, condition{std::move(condition)},
                                            data_fd{page_file}, meta{std::move(table_meta)}, index_file{index_file},
                                            key_start(key_start), key_end(key_end) {
        iter = index_file.SelectRecord(key_start);
    }

    void Reset() override {
        OpNode::Reset();
        iter = index_file.SelectRecord(key_start);
    }

    [[nodiscard]] bool Over() const override {
        return iter.first == -1;
    }

    RecordList Next() override {

        auto slot_id = iter.second;
        auto child_cnt = index_file.Page(iter.first)->ChildCount();

        RecordList ret{};
        for (; iter.second < child_cnt; ++iter.second) {
            std::shared_ptr<IndexRecordLeaf> leaf_record = index_file.Select(iter);
            assert (leaf_record != nullptr);

            if (*(leaf_record->key) > *key_end) {
                iter.first = -1;
                return ret;
            }

            auto page = buffer.ReadPage(data_fd, leaf_record->page_id);
            DataPage dp{page, *meta};

            auto record{dp.Select(leaf_record->slot_id)};
            if (record != nullptr && (condition == nullptr || (*condition)(record))) {
                ret.push_back(record);
            }
        }

        if (iter.second == child_cnt) {
            iter.first = index_file.Page(iter.first)->NextPage();
            iter.second = 0;
        }

        return std::move(ret);
    }

    ~IndexScanNode() override = default;

   private:
    BufferSystem &buffer;
    IndexFile& index_file;
    const std::shared_ptr<const TableMeta> meta;
    const std::shared_ptr<FilterCondition> condition;
    std::pair<PageID, TreeOrder> iter{std::make_pair<PageID, TreeOrder>(0, 0)};
    FileID data_fd;
    const std::shared_ptr<IndexField> key_start, key_end;

};


#endif //DBS_TUTORIAL_SCANNODE_H
