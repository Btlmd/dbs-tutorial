//
// Created by lambda on 22-11-17.
//

#ifndef DBS_TUTORIAL_DBSYSTEM_H
#define DBS_TUTORIAL_DBSYSTEM_H

#include <vector>
#include <algorithm>
#include <string>
#include <memory>
#include <unordered_map>
#include <optional>

#include <boost/bimap.hpp>

#include <defines.h>
#include <display/Result.h>
#include <io/BufferSystem.h>
#include <record/Field.h>
#include <record/TableMeta.h>
#include <record/DataPage.h>
#include <node/OpNode.h>
#include <node/ScanNode.h>
#include <system/WhereConditions.h>
#include <index/IndexFile.h>

class DBSystem {
public:
    DBSystem();

    ~DBSystem();

    std::shared_ptr<Result> CreateDatabase(const std::string &db_name);

    std::shared_ptr<Result> UseDatabase(const std::string &db_name);

    std::shared_ptr<Result> DropDatabase(const std::string &db_name);

    std::shared_ptr<Result> ShowDatabases() const;


    std::shared_ptr<Result>
    CreateTable(const std::string &table_name, const std::vector<std::shared_ptr<FieldMeta>> &field_meta,
                std::optional<RawPrimaryKey> raw_pk, const std::vector<RawForeignKey> &raw_fks);

    std::shared_ptr<Result> DropTable(const std::string &table_name);

    std::shared_ptr<Result> ShowTables() const;

    std::shared_ptr<Result> AddForeignKey(const std::string &table_name, const RawForeignKey &raw_fk);

    std::shared_ptr<Result> AddPrimaryKey(const std::string &table_name, const RawPrimaryKey &raw_pk);

    std::shared_ptr<Result> AddUnique(const std::string &table_name, const std::vector<std::string> &fields);

    std::shared_ptr<Result> DropForeignKey(const std::string &table_name, const std::string &fk_name);

    std::shared_ptr<Result> DropPrimaryKey(const std::string &table_name, const std::string &pk_name);

    std::shared_ptr<Result> AddIndex(const std::string &table_name, const std::vector<std::string> &field_name);

    std::shared_ptr<Result> AddIndex(TableID table_id, const std::vector<FieldID> &field_ids, bool is_user);

    std::shared_ptr<Result> DropIndex(const std::string &table_name, const std::vector<std::string> &field_name);

    std::shared_ptr<Result> DropIndex(TableID table_id, const std::vector<FieldID> &field_ids, bool is_user);

    static std::string GetIndexFilePath(TableID table_id, const std::vector<FieldID> &field_ids) {
        auto field_string = std::to_string(field_ids[0]);
        for (auto i = 1; i < field_ids.size(); ++i) {
            field_string += "_" + std::to_string(field_ids[i]);
        }
        return fmt::format(TABLE_INDEX_PATTERN, table_id, field_string);
    }


    /**
     * Delete records satisfying `cond`
     * @param table_id
     * @param cond
     * @return
     */
    std::shared_ptr<Result> Delete(TableID table_id, const std::shared_ptr<AndCondition> &cond);

    /**
     * Generate a `TrivialScanNode` from given table_id and filter condition
     * @param table_id
     * @param cond
     * @return
     */
    std::shared_ptr<ScanNode> GetTrivialScanNode(TableID table_id, const std::shared_ptr<FilterCondition> &cond);
    std::shared_ptr<ScanNode> GetIndexScanNode(TableID table_id, std::vector<FieldID> field_ids, const std::shared_ptr<FilterCondition> &cond,
                                             const std::shared_ptr<IndexField>& key_start, const std::shared_ptr<IndexField>& key_end);

    std::shared_ptr<ScanNode> GetScanNodeByCondition(TableID table_id, const std::shared_ptr<AndCondition> &cond);

    /**
     * Select from execution tree
     * @param plan
     * @return
     */
    std::shared_ptr<Result> Select(const std::vector<std::string> &header, const std::shared_ptr<OpNode> &plan);

    /**
     * Insert records into `table_id`
     * @param table_id
     * @param records
     * @return
     */
    std::shared_ptr<Result> Insert(TableID table_id, RecordList &records);

    std::shared_ptr<Result>
    Update(TableID table_id, const std::vector<std::pair<std::shared_ptr<FieldMeta>, std::shared_ptr<Field>>> &updates,
           const std::shared_ptr<AndCondition> &cond);

    /**
     * Validate that the record satisfies all constraints specified by `meta`
     * Caller ensures that the field type are legal
     * @param meta
     * @param record
     */
    void CheckConstraintInsert(const TableMeta &meta, const std::shared_ptr<Record> &record);

    /**
     * Validate that the update does not break any constraint
     * @param meta
     * @param record_prev
     * @param record_updated
     */
    void CheckConstraintUpdate(
            const TableMeta &meta,
            const std::shared_ptr<Record> &record_prev,
            const std::shared_ptr<Record> &record_updated,
            const std::unordered_set<FieldID> &affected
    );

    /**
     * Validate that the deletion does not break any constraint
     * @param meta
     * @param record
     */
    void CheckConstraintDelete(const TableMeta &meta, const std::shared_ptr<Record> &record);

    /**
     * Get a const pointer to TableMeta of the specific table
     * @param table_id
     * @return
     */
    std::shared_ptr<const TableMeta> GetTableMeta(TableID table_id) {
        assert(meta_map.find(table_id) != meta_map.end());
        return meta_map[table_id];
    }

    /**
     * If table exists, return table_id
     * Otherwise throw exception
     * @param table_name
     * @return
     */
    TableID GetTableID(const std::string &table_name) const {
        CheckOnUse();
        auto iter{table_name_map.left.find(table_name)};
        if (iter == table_name_map.left.end()) {
            throw OperationError{"Table `{}` does not exist", table_name};
        }
        return iter->second;
    }

    /**
     * Describe table query
     * @param table_name
     * @return
     */
    std::shared_ptr<Result> DescribeTable(const std::string &table_name);

private:
    std::set<std::string> databases;
    std::string current_database;
    TableID table_count{-1};
    FileID table_info_fd{-1};
    std::unordered_map<TableID, FileID> table_data_fd;
    std::unordered_map<TableID, FileID> table_meta_fd;
    std::map<std::pair<TableID, std::vector<FieldID>>, FileID> table_index_fd;
    std::map<std::pair<TableID, std::vector<FieldID>>, std::shared_ptr<IndexFile>> table_index_file;
    std::unordered_map<TableID, std::shared_ptr<TableMeta>> meta_map;
    boost::bimap<std::string, TableID> table_name_map;
    bool on_use{false};

    BufferSystem buffer;

    TableID NextTableID() const {
        TableID tid{0};
        while (table_name_map.right.find(tid) != table_name_map.right.end()) {
            ++tid;
        }
        return tid;
    }


    /**
     * Add foreign key to `table_id`
     * @param table_id
     * @param raw_fk
     */
    void AddForeignKey(std::shared_ptr<TableMeta> &meta, const RawForeignKey &raw_fk, bool constraint_check = true);

    /**
     * Add primary key to `table_id`
     * the raw_fk can be illegal, where an exception is thrown
     * @param table_id
     * @param raw_pk
     */
    void AddPrimaryKey(std::shared_ptr<TableMeta> &meta, const RawPrimaryKey &raw_pk, bool constraint_check = true);

    void AddUnique(std::shared_ptr<TableMeta> &meta, const std::vector<std::string> &fields);

    /**
     *
     * @param table_name
     * @return
     */
    bool CheckTableExist(const std::string &table_name) const noexcept;

    /**
     * Close the currently used database
     * If do database is currently used, throw exception
     */
    void CloseDatabase();

    /**
     * Initialize the system
     */
    void Init();

    /**
     * Find a page with `size` bytes of free space
     * If there is no such page, create a new one and return
     * @param table_id
     * @param size
     * @return
     */
    std::shared_ptr<DataPage> FindPageWithSpace(TableID table_id, RecordSize size);
    void UpdatePageFreeSpace(TableID table_id, PageID page_id, RecordSize delta_size);
    // Data Page ID -> (FSM Page ID, FSM Slot ID)

    /**
     * Throw exception if no database is selected
     */
    void CheckOnUse() const;

    void InsertRecord(TableID table_id, const std::shared_ptr<Record> &record);


    /**
     * Get IndexFile for std::vector
     * @param table_id
     * @param fields
     * @param value
     */
    std::shared_ptr<IndexFile> GetIndexFile(TableID table_id, const std::vector<FieldID> &fields) {
        auto ident{std::make_pair(table_id, fields)};
        auto it_if{table_index_file.find(ident)};
        if (it_if != table_index_file.end()) {
            return it_if->second;
        }

        auto it{table_index_fd.find(std::make_pair(table_id, fields))};
        if (it == table_index_fd.end()) {
            throw OperationError{"Internal Error! No index."};
        }
        auto ret{std::make_shared<IndexFile>(buffer, it->second)};
        table_index_file[ident] = ret;
        return ret;
    }


    /**
     * Get IndexFile for single field index
     * @param table_id
     * @param field_id
     * @param value
     */
    std::shared_ptr<IndexFile> GetIndexFile(TableID table_id, FieldID field_id) {
        std::vector<FieldID> fields{{field_id}};
        auto ident{std::make_pair(table_id, fields)};
        auto it_if{table_index_file.find(ident)};
        if (it_if != table_index_file.end()) {
            return it_if->second;
        }

        auto it{table_index_fd.find(std::make_pair(table_id, fields))};
        if (it == table_index_fd.end()) {
            throw OperationError{"Internal Error! No index on ({}, {})", table_id, field_id};
        }
        auto ret{std::make_shared<IndexFile>(buffer, it->second)};
        table_index_file[ident] = ret;
        return table_index_file[ident];
    }

    /**
     * Get IndexFile for 2-fields composite index
     * @param table_id
     * @param field_id1
     * @param field_id2
     * @return
     */
    std::shared_ptr<IndexFile> &GetIndexFile(TableID table_id, FieldID field_id1, FieldID field_id2) {
        std::vector<FieldID> fields{{field_id1, field_id2}};
        auto ident{std::make_pair(table_id, fields)};
        auto it_if{table_index_file.find(ident)};
        if (it_if != table_index_file.end()) {
            return it_if->second;
        }

        auto it{table_index_fd.find(std::make_pair(table_id, fields))};
        if (it == table_index_fd.end()) {
            throw OperationError{"Internal Error! No index on ({}, [{}, {}])", table_id, field_id1, field_id2};
        }
        auto ret{std::make_shared<IndexFile>(buffer, it->second)};
        table_index_file[ident] = ret;
        return table_index_file[ident];
    }

    /**
     * Drop all indexes of `record`
     * @param table_id
     * @param page_id
     * @param j
     * @param record
     */
    void DropRecordIndex(TableID table_id, PageID page_id, SlotID j, const std::shared_ptr<Record> record);

    /**
     * Insert index for `record`
     * @param table_id
     * @param page_id
     * @param j
     * @param record
     */
    void InsertRecordIndex(TableID table_id, PageID page_id, SlotID j, const std::shared_ptr<Record> record);

    /**
     * Update index for in-place index update, only update fields that changed
     * @param affected
     * @param table_id
     * @param page_id
     * @param j
     * @param record_prev
     * @param record_updated
     */
    void UpdateInPlaceRecordIndex(
            const std::unordered_set<FieldID> &affected,
            TableID table_id, PageID page_id, SlotID j,
            const std::shared_ptr<Record> &record_prev,
            const std::shared_ptr<Record> &record_updated
    );

    /**
     * Get the num of records in `table_id` which match in `fields` with values `field_values`
     * @param table_id
     * @param fields
     * @param field_vals
     * @return
     */
    std::size_t TupleExists(TableID table_id, const std::vector<FieldID> &fields, const std::vector<std::shared_ptr<Field>>& field_vals);

    /**
     * Get the all tables that refer to `table_id`
     * @param table_id
     * @return
     */
    std::vector<std::pair<TableID, std::shared_ptr<ForeignKey>>> GetParentTables(TableID table_id);

    /**
     * Truncate fields and call `AddIndex`
     * @tparam lim
     * @param table_id
     * @param field_ids
     * @param is_user
     */
    template<FieldID lim>
    void AddIndexLimited(TableID table_id, const std::vector<FieldID> &field_ids, bool is_user) {
        std::vector<FieldID> index_fields;
        for (FieldID i{0}; i < std::min<std::size_t>(lim, field_ids.size()); ++i) {
            index_fields.push_back(field_ids[i]);
        }
        AddIndex(table_id, index_fields, is_user);
    }

    /**
     * Truncate fields and call `DropIndex`
     * @tparam lim
     * @param table_id
     * @param field_ids
     * @param is_user
     */
    template<FieldID lim>
    void DropIndexLimited(TableID table_id, const std::vector<FieldID> &field_ids, bool is_user) {
        std::vector<FieldID> index_fields;
        for (FieldID i{0}; i < std::min<std::size_t>(lim, field_ids.size()); ++i) {
            index_fields.push_back(field_ids[i]);
        }
        DropIndex(table_id, index_fields, is_user);
    }

    /**
     * Drop specified foreign key
     * @param table_id
     * @param fk_name
     */
    void DropForeignKey(TableID table_id, const std::string &fk_name);

    /**
     * Drop primary key
     * @param table_id
     */
    void DropPrimaryKey(TableID table_id);

    /**
     * Drop specified table
     * @param table_id
     */
    void DropTable(TableID table_id);

    /**
     * Check if `fields` are composite unique; treat NULL like a normal value
     * @param table_id
     * @param fields
     * @return
     */
    std::optional<std::shared_ptr<Record>> CheckRecordsUnique(TableID table_id, const std::vector<FieldID> &fields);

    std::optional<std::shared_ptr<Record>> CheckRecordsSatisfyAllUnique(TableID table_id, const RecordList &records);

};

#endif //DBS_TUTORIAL_DBSYSTEM_H
