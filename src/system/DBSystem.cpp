//
// Created by lambda on 22-11-17.
//

#include "DBSystem.h"

#include <filesystem>
#include <optional>


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
    if (databases.find(db_name) == databases.end()) {
        throw OperationError{"Unknown database '{}'", db_name};
    }
    if (on_use) {
        CloseDatabase();
    }
    TraceLog << "Opening dataset " << db_name;
    on_use = true;
    /**
     * Table File Structure
     * {table_count}
     * {next_table_id}
     * {table_id, table_name}
     * ...
     */

    // load tables
    table_info_fd = FileSystem::OpenFile(DB_DIR / db_name / TABLE_FILE);
    const uint8_t *table_info_ptr{buffer.ReadPage(table_info_fd, 0)->data};
    read_var(table_info_ptr, table_count);

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

    current_database = db_name;
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
    assert(!current_database.empty());
    assert(on_use);

    TraceLog << "Closing database " << current_database;

    if (table_info_fd > 0) {
        buffer.CloseFile(table_info_fd);
        table_info_fd = -1;
    }

    // close file descriptors

    auto close_func = [this](const std::pair<TableID, FileID>& relation)-> void {
        buffer.CloseFile(relation.second);
    };
    std::for_each(table_meta_fd.begin(), table_meta_fd.end(), close_func);
    std::for_each(table_data_fd.begin(), table_data_fd.end(), close_func);
    std::for_each(table_index_fd.begin(), table_index_fd.begin(), close_func);

    // reset table status
    table_name_map.clear();
    table_data_fd.clear();
    table_meta_fd.clear();
    table_index_fd.clear();

    current_database = "";
    table_info_fd = -1;
    on_use = false;
}

Result *DBSystem::ShowDatabases() const {
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

Result *DBSystem::CreateTable(const std::string &table_name, const std::vector<FieldMeta *> &field_meta,
                              std::optional<RawPrimaryKey> raw_pk, const std::vector<RawForeignKey> &raw_fks) {
    if (!on_use) {
        throw OperationError{"No database selected"};
    }

    if (CheckTableExist(table_name)) {
        throw OperationError{"Table `{}` already exists", table_name};
    }

    // assign file descriptor

    TableID table_id{NextTableID()};

    /**
     * on creation, field IDs are continuous. however, after deleting fields, this is not guaranteed
     */
    meta_map[table_id] = new TableMeta{0, {field_meta}, table_meta_fd[table_id], buffer};

    // primary key and foreign keys

    if (raw_pk) {
        AddPrimaryKey(table_id, raw_pk.value());
    }

    for (const auto &fk: raw_fks) {
        AddForeignKey(table_id, fk);
    }

    // if all previous step succeed, create table in DBSystem

    table_data_fd[table_id] = FileSystem::NewFile(
            DB_DIR / current_database / fmt::format(TABLE_DATA_PATTERN, table_id));
    table_meta_fd[table_id] = FileSystem::NewFile(
            DB_DIR / current_database / fmt::format(TABLE_META_PATTERN, table_id));
    table_index_fd[table_id] = FileSystem::NewFile(
            DB_DIR / current_database / fmt::format(TABLE_INDEX_PATTERN, table_id));
    table_name_map.insert({table_name, table_id});

    return new TextResult{"Query OK"};
}

Result *DBSystem::DropTable(const std::string &table_name) {

}

Result *DBSystem::ShowTables() const {
    std::vector<std::vector<std::string>> table_name_buffer;
    for (const auto &[name, tid]: table_name_map) {
        table_name_buffer.push_back({{name}});
    }
    return new TableResult{{fmt::format("Tables_in_{}", current_database)}, table_name_buffer};
}

Result *DBSystem::AddForeignKey(const std::string &table_name, const RawForeignKey &raw_fk) {
    auto table_id{GetTableID(table_name)};
    AddForeignKey(table_id, raw_fk);
    return new TextResult{"Query OK"};
}

Result *DBSystem::AddPrimaryKey(const std::string &table_name, const RawPrimaryKey &raw_pk) {
    auto table_id{GetTableID(table_name)};
    AddPrimaryKey(table_id, raw_pk);
    return new TextResult{"Query OK"};
}

void DBSystem::AddForeignKey(TableID table, const RawForeignKey &raw_fk) {
    auto &[fk_name, reference_table_name, fk_field_names, reference_field_names]{raw_fk};
    auto meta{meta_map[table]};

    auto fk{new ForeignKey};
    if (fk_name.size() > CONSTRAINT_NAME_LEN_MAX) {
        throw OperationError{"Identifier name '{}' is too long", fk_name};
    }

    if (fk_field_names.size() != reference_field_names.size()) {
        throw OperationError{"Mismatch of reference table field count"};
    }

    auto reference_table_id{GetTableID(reference_table_name)};
    auto reference_meta{meta_map[reference_table_id]};
    fk_name.copy(fk->name, fk_name.size(), 0);
    for (int i{0}; i < fk_field_names.size(); ++i) {
        auto table_fid{meta->field_meta.ToID<OperationError>(
                fk_field_names[i],
                "Key column `{}` does not exist in table", fk_field_names[i]
        )};
        auto reference_fid{reference_meta->field_meta.ToID<OperationError>(
                reference_field_names[i],
                "Missing column `{}` for constraint `{}` in the referenced table `{}`",
                reference_field_names[i],
                fk_name,
                reference_table_name
        )};
        fk->fields[i] = table_fid;
        fk->reference_fields[i] = reference_fid;
    }
    fk->field_count = fk_field_names.size();

    /**
     * TODO: check duplications of fields?
     */

    meta->foreign_keys.push_back(fk);
}

void DBSystem::AddPrimaryKey(TableID table_id, const RawPrimaryKey &raw_pk) {
    const auto &[pk_name, pk_fields]{raw_pk};
    auto meta{meta_map[table_id]};

    if (meta->primary_key) {
        throw OperationError{"Multiple primary key defined"};
    }

    auto pk{new PrimaryKey};
    if (pk_name.size() > CONSTRAINT_NAME_LEN_MAX) {
        throw OperationError{"Identifier name '{}' is too long", pk_name};
    }
    pk_name.copy(pk->name, pk_name.size(), 0);

    pk->field_count = 0;
    for (const auto &pk_field_name: pk_fields) {
        pk->fields[pk->field_count] = meta->field_meta.ToID<OperationError>(
                pk_field_name,
                "Key column `{}` does not exist in the table", pk_field_name
        );
        ++(pk->field_count);
    }

    /**
     * TODO: unique constraint, not null constraint, index build up
     */

    meta->primary_key = pk;
}

bool DBSystem::CheckTableExist(const std::string &table_name) const noexcept {
    return table_name_map.left.find(table_name) != table_name_map.left.end();
}

TableID DBSystem::GetTableID(const std::string &table_name) const {
    auto iter{table_name_map.left.find(table_name)};
    if (iter == table_name_map.left.end()) {
        throw OperationError{"Table `{}` does not exist", table_name};
    }
    return iter->second;
}
