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

    Result *ShowDatabases();


    Result *CreateTable(const std::string &table_name, const std::vector<FieldMeta *> &field_names, );

    Result *DropTable(const std::string &table_name);

    Result *ShowTables();


    void CloseDatabase();

    void Init();


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

    TableID NextTableID() {
        TableID tid{0};
        while (table_name_map.right.find(tid) != table_name_map.right.end()) {
            ++tid;
        }
        return tid;
    }
};

#endif //DBS_TUTORIAL_DBSYSTEM_H
