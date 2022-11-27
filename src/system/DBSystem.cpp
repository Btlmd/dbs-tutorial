//
// Created by lambda on 22-11-17.
//

#include "DBSystem.h"

#include <filesystem>

#include <defines.h>
#include <io/FileSystem.h>
#include <exception/OperationException.h>
#include <utils/Serialization.h>
#include <record/Field.h>
#include <display/Result.h>
#include <record/TableMeta.h>

Result *DBSystem::CreateDatabase(const std::string &db_name) {
    if (databases.find(db_name) != databases.end()) {
        throw OperationError{"Can't create database '{}'; database exists", db_name};
    }

    // create database directory
    FileSystem::MakeDirectory(DB_DIR / db_name, false);

    // create table file with no tables
    FileID table_file{FileSystem::NewFile(DB_DIR / db_name / TABLE_FILE)};
    auto table_file_page{buffer.CreatePage(table_file, 0)};
    auto table_file_ptr{table_file_page->data};
    write_var(table_file_ptr, TableID(0));
    table_file_page->SetDirty();
    buffer.CloseFile(table_file);

    databases.insert(db_name);

    return new TextResult{"Database created"};
}

void DBSystem::Init() {
    // make database directory
    FileSystem::MakeDirectory(DB_DIR);

    // load database names
    for (const auto &file: std::filesystem::directory_iterator(DB_DIR)) {
        if (file.is_directory()) {
            databases.insert(std::filesystem::path(file).filename());
        }
    }
}

Result *DBSystem::UseDatabase(const std::string &db_name) {
    if (databases.find(db_name) != databases.end()) {
        throw OperationError{"Unknown database '{}'", db_name};
    }
    if (on_use) {
        CloseDatabase();
    }
    on_use = true;
    /**
     * Table File Structure
     * {table_count}
     * {next_table_id}
     * {table_id, table_table_name}
     * ...
     */

    // load tables
    table_info_fd = FileSystem::OpenFile(DB_DIR / db_name / TABLE_FILE);
    const uint8_t *table_info_ptr{buffer.ReadPage(table_info_fd, 0)->data};
    read_var(table_info_ptr, table_count);
    read_var(table_info_ptr, next_table_id);  // NOTE: Assume that next_page_id not overflow

    std::string table_name;
    TableID table_id;
    for (int i{0}; i < table_count; ++i) {
        read_var(table_info_ptr, table_id);
        read_string(table_info_ptr, table_name);
        table_name_map.insert({table_name, table_id});
        table_data_fd[table_id] = FileSystem::OpenFile(DB_DIR / db_name / fmt::format(TABLE_DATA_PATTERN, table_id));
        table_meta_fd[table_id] = FileSystem::OpenFile(DB_DIR / db_name / fmt::format(TABLE_META_PATTERN, table_id));
        table_index_fd[table_id] = FileSystem::OpenFile(DB_DIR / db_name / fmt::format(TABLE_INDEX_PATTERN, table_id));
        meta_map[table_id] = TableMeta::FromSrc(table_meta_fd[table_id], buffer);
    }
    return new TextResult{"Database changed"};
}

Result *DBSystem::DropDatabase(const std::string &db_name) {
    if (databases.find(db_name) == databases.end()) {
        throw OperationError{"Can't drop database '{}'; database doesn't exist", db_name};
    }

    if (db_name == current_database) {
        CloseDatabase();
    }

    FileSystem::RemoveDirectory(DB_DIR / db_name);
    databases.erase(db_name);

    return new TextResult{"Database dropped"};
}

void DBSystem::CloseDatabase() {
    assert(table_info_fd > 0);
    assert(current_database.length() > 0);
    assert(on_use);

    if (table_info_fd > 0) {
        buffer.CloseFile(table_info_fd);
        table_info_fd = -1;
    }

    if (table_count != 0) {
        for (int table_id{0}; table_id < table_count; table_id) {
            buffer.CloseFile(table_meta_fd[table_id]);
            buffer.CloseFile(table_data_fd[table_id]);
            buffer.CloseFile(table_index_fd[table_id]);
        }

        // reset table status
        table_name_map.clear();
        table_data_fd.clear();
        table_meta_fd.clear();
        table_index_fd.clear();
    }

    current_database = "";
    table_info_fd = -1;
    on_use = false;
}

Result *DBSystem::ShowDatabases() {
    std::vector<std::vector<std::string>> db_name_buffer;
    for (const auto &db_name: databases) {
        db_name_buffer.emplace_back(std::vector<std::string>{{db_name}});
    }
    return new TableResult{{"Database"}, db_name_buffer};
}

DBSystem::DBSystem() {
    Init();
}

DBSystem::~DBSystem() {
    if (on_use) {
        CloseDatabase();
    }
}

Result *DBSystem::CreateTable(const std::string &table_name, const std::vector<FieldMeta *> &fields) {
    if (!on_use) {
        throw OperationError{"No database selected"};
    }
    TableID table_id{NextTableID()};
    table_data_fd[table_id] = FileSystem::NewFile(
            DB_DIR / current_database / fmt::format(TABLE_DATA_PATTERN, table_id));
    table_meta_fd[table_id] = FileSystem::NewFile(
            DB_DIR / current_database / fmt::format(TABLE_META_PATTERN, table_id));
    table_index_fd[table_id] = FileSystem::NewFile(
            DB_DIR / current_database / fmt::format(TABLE_INDEX_PATTERN, table_id));
    meta_map[table_id] = TableMeta:(fields, buffer);
    return new TextResult{"Query OK"};
}

Result *DBSystem::DropTable(const std::string &table_name) {

}

Result *DBSystem::ShowTables() {
    std::vector<std::vector<std::string>> table_name_buffer;
    for (const auto &[name, tid]: table_name_map) {
        table_name_buffer.push_back({{name}});
    }
    return new TableResult{{fmt::format("Tables_in_{}", current_database)}, table_name_buffer};
}


