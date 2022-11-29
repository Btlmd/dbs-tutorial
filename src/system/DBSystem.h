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

class DBSystem {
public:
    DBSystem();

    ~DBSystem();

    Result *CreateDatabase(const std::string &db_name);

    Result *UseDatabase(const std::string &db_name);

    Result *DropDatabase(const std::string &db_name);

    Result *ShowDatabases() const;


    Result *CreateTable(const std::string &table_name, const std::vector<FieldMeta *> &field_meta,
                        std::optional<RawPrimaryKey> raw_pk, const std::vector<RawForeignKey> &raw_fks);

    Result *DropTable(const std::string &table_name);

    Result *ShowTables() const;

    Result *AddForeignKey(const std::string &table_name, const RawForeignKey &raw_fk);

    Result *AddPrimaryKey(const std::string &table_name, const RawPrimaryKey &raw_pk);

    Result *DropForeignKey(const std::string &table_name, const std::string fk_name);

    Result *DropPrimaryKey(const std::string &table_name, const std::string fk_name);

    Result *Insert(std::string &table_name, std::vector<Field *> &fields);

    Result *Delete(std::string &table_name);

    Result *Select(std::string &table_name);

private:
    std::set<std::string> databases;
    std::string current_database;
    TableID table_count{-1};
    TableID next_table_id{-1};
    FileID table_info_fd{-1};
    std::unordered_map<TableID, FileID> table_data_fd;
    std::unordered_map<TableID, FileID> table_meta_fd;
    std::unordered_map<TableID, FileID> table_index_fd;
    std::unordered_map<TableID, TableMeta *> meta_map;
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

    void

    /**
     * Add foreign key to `table_id`
     * @param table_id
     * @param raw_fk
     */
    void AddForeignKey(TableID table_id, const RawForeignKey &raw_fk);

    /**
     * Add primary key to `table_id`
     * the raw_fk can be illegal, where an exception is thrown
     * @param table_id
     * @param raw_pk
     */
    void AddPrimaryKey(TableID table_id, const RawPrimaryKey &raw_pk);

    /**
     *
     * @param table_name
     * @return
     */
    bool CheckTableExist(const std::string &table_name) const noexcept;

    /**
     * If table exists, return table_id
     * Otherwise throw exception
     * @param table_name
     * @return
     */
    TableID GetTableID(const std::string &table_name) const;

    /**
     * Close the currently used database
     * If do database is currently used, throw exception
     */
    void CloseDatabase();

    /**
     * Initialize the system
     */
    void Init();
};

#endif //DBS_TUTORIAL_DBSYSTEM_H
