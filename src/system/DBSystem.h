//
// Created by lambda on 22-11-17.
//

#ifndef DBS_TUTORIAL_DBSYSTEM_H
#define DBS_TUTORIAL_DBSYSTEM_H

#include <vector>
#include <string>
#include <memory>
#include <unordered_map>

#include <boost/bimap.hpp>

#include <defines.h>
#include <display/Result.h>
#include <io/BufferSystem.h>
#include <record/Field.h>
#include <record/TableMeta.h>
#include <record/DataPage.h>

class DBSystem {
public:
    DBSystem();

    ~DBSystem();

    std::shared_ptr<Result> CreateDatabase(const std::string &db_name);

    std::shared_ptr<Result> UseDatabase(const std::string &db_name);

    std::shared_ptr<Result> DropDatabase(const std::string &db_name);

    std::shared_ptr<Result> ShowDatabases() const;


    std::shared_ptr<Result> CreateTable(const std::string &table_name, const std::vector<std::shared_ptr<FieldMeta>> &field_meta,
                        std::optional<RawPrimaryKey> raw_pk, const std::vector<RawForeignKey> &raw_fks);

    std::shared_ptr<Result> DropTable(const std::string &table_name);

    std::shared_ptr<Result> ShowTables() const;

    std::shared_ptr<Result> AddForeignKey(const std::string &table_name, const RawForeignKey &raw_fk);

    std::shared_ptr<Result> AddPrimaryKey(const std::string &table_name, const RawPrimaryKey &raw_pk);

    std::shared_ptr<Result> DropForeignKey(const std::string &table_name, const std::string fk_name);

    std::shared_ptr<Result> DropPrimaryKey(const std::string &table_name, const std::string fk_name);

    /**
     * Insert `record` into table
     * Caller has to ensures that the record is legal
     * @param table
     * @param record
     */
    void Insert(TableID table, std::shared_ptr<Record> &record);

    std::shared_ptr<Result> Delete(const std::string &table_name);

    std::shared_ptr<Result> Select(const std::string &table_name);

    /**
     * Validate that the record satisfies all constraints specified by `meta`
     * @param meta
     * @param record
     */
    void CheckConstraint(const TableMeta& meta, const std::shared_ptr<Record> &record) const;

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
        auto iter{table_name_map.left.find(table_name)};
        if (iter == table_name_map.left.end()) {
            throw OperationError{"Table `{}` does not exist", table_name};
        }
        return iter->second;
    }

    /**
     * Return the number of inserted records as a `Result`
     * @return
     */
    std::shared_ptr<Result> InsertResult();

    std::shared_ptr<Result> DescribeTable(const std::string &table_name);

private:
    std::set<std::string> databases;
    std::string current_database;
    TableID table_count{-1};
    FileID table_info_fd{-1};
    std::unordered_map<TableID, FileID> table_data_fd;
    std::unordered_map<TableID, FileID> table_meta_fd;
    std::unordered_map<TableID, FileID> table_index_fd;
    std::unordered_map<TableID, std::shared_ptr<TableMeta>> meta_map;
    boost::bimap<std::string, TableID> table_name_map;
    bool on_use{false};
    std::size_t insert_record_counter{0};

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
    void AddForeignKey(std::shared_ptr<TableMeta> &meta, const RawForeignKey &raw_fk);

    /**
     * Add primary key to `table_id`
     * the raw_fk can be illegal, where an exception is thrown
     * @param table_id
     * @param raw_pk
     */
    static void AddPrimaryKey(std::shared_ptr<TableMeta> &meta, const RawPrimaryKey &raw_pk);

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

};

#endif //DBS_TUTORIAL_DBSYSTEM_H
