//
// Created by lambda on 22-11-17.
//

#include "DBSystem.h"

#include <algorithm>
#include <filesystem>
#include <optional>

#include <magic_enum.hpp>
#include <boost/algorithm/string/join.hpp>

#include <defines.h>
#include <io/FileSystem.h>
#include <exception/OperationException.h>
#include <utils/Serialization.h>
#include <record/Field.h>
#include <display/Result.h>
#include <record/TableMeta.h>

std::shared_ptr<Result> DBSystem::CreateDatabase(const std::string &db_name) {
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

    return std::shared_ptr<Result>{new TextResult{"Database created"}};
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

std::shared_ptr<Result> DBSystem::UseDatabase(const std::string &db_name) {
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
    return std::shared_ptr<Result>{new TextResult{"Database changed"}};
}

std::shared_ptr<Result> DBSystem::DropDatabase(const std::string &db_name) {
    if (databases.find(db_name) == databases.end()) {
        throw OperationError{"Can't drop database '{}'; database doesn't exist", db_name};
    }

    if (db_name == current_database) {
        CloseDatabase();
    }

    FileSystem::RemoveDirectory(DB_DIR / db_name);
    databases.erase(db_name);

    return std::shared_ptr<Result>{new TextResult{"Database dropped"}};
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

    auto close_func = [this](const std::pair<TableID, FileID> &relation) -> void {
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

std::shared_ptr<Result> DBSystem::ShowDatabases() const {
    std::vector<std::vector<std::string>> db_name_buffer;
    for (const auto &db_name: databases) {
        db_name_buffer.emplace_back(std::vector<std::string>{{db_name}});
    }
    return std::shared_ptr<Result>{new TableResult{{"Database"}, db_name_buffer}};
}

DBSystem::DBSystem() {
    Init();
}

DBSystem::~DBSystem() {
    if (on_use) {
        CloseDatabase();
    }
}

std::shared_ptr<Result>
DBSystem::CreateTable(const std::string &table_name, const std::vector<std::shared_ptr<FieldMeta>> &field_meta,
                      std::optional<RawPrimaryKey> raw_pk,
                      const std::vector<RawForeignKey> &raw_fks) {
    if (!on_use) {
        throw OperationError{"No database selected"};
    }

    if (CheckTableExist(table_name)) {
        throw OperationError{"Table `{}` already exists", table_name};
    }

    // assign file descriptor

    TableID table_id{NextTableID()};

    /**
     * Refactored: field position and field id is just the same
     * So field deletion should update all possible foreign keys in the database
     */
    auto meta{std::make_shared<TableMeta>(0, FieldMeteTable{field_meta}, table_meta_fd[table_id], buffer)};

    // primary key and foreign keys

    if (raw_pk) {
        AddPrimaryKey(meta, raw_pk.value());
    }

    for (const auto &fk: raw_fks) {
        AddForeignKey(meta, fk);
    }

    // if all previous step succeed, create table in DBSystem

    table_data_fd[table_id] = FileSystem::NewFile(
            DB_DIR / current_database / fmt::format(TABLE_DATA_PATTERN, table_id));
    table_meta_fd[table_id] = FileSystem::NewFile(
            DB_DIR / current_database / fmt::format(TABLE_META_PATTERN, table_id));
    table_index_fd[table_id] = FileSystem::NewFile(
            DB_DIR / current_database / fmt::format(TABLE_INDEX_PATTERN, table_id));
    table_name_map.insert({table_name, table_id});
    meta_map[table_id] = std::move(meta);

    return std::shared_ptr<Result>{new TextResult{"Query OK"}};
}

std::shared_ptr<Result> DBSystem::DropTable(const std::string &table_name) {

}

std::shared_ptr<Result> DBSystem::ShowTables() const {
    std::vector<std::vector<std::string>> table_name_buffer;
    for (const auto &[name, tid]: table_name_map) {
        table_name_buffer.push_back({{name}});
    }
    return std::shared_ptr<Result>{new TableResult{{fmt::format("Tables_in_{}", current_database)}, table_name_buffer}};
}

std::shared_ptr<Result> DBSystem::AddForeignKey(const std::string &table_name, const RawForeignKey &raw_fk) {
    auto table_id{GetTableID(table_name)};
    AddForeignKey(meta_map[table_id], raw_fk);
    return std::shared_ptr<Result>{new TextResult{"Query OK"}};
}

std::shared_ptr<Result> DBSystem::AddPrimaryKey(const std::string &table_name, const RawPrimaryKey &raw_pk) {
    auto table_id{GetTableID(table_name)};
    AddPrimaryKey(meta_map[table_id], raw_pk);
    return std::shared_ptr<Result>{new TextResult{"Query OK"}};
}

void DBSystem::AddForeignKey(std::shared_ptr<TableMeta> &meta, const RawForeignKey &raw_fk) {
    auto &[fk_name, reference_table_name, fk_field_names, reference_field_names]{raw_fk};

    auto fk{std::make_shared<ForeignKey>()};
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

void DBSystem::AddPrimaryKey(std::shared_ptr<TableMeta> &meta, const RawPrimaryKey &raw_pk) {
    const auto &[pk_name, pk_fields]{raw_pk};

    if (meta->primary_key != nullptr) {
        throw OperationError{"Multiple primary key defined"};
    }

    auto pk{std::make_shared<PrimaryKey>()};
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

std::shared_ptr<DataPage> DBSystem::FindPageWithSpace(TableID table_id, RecordSize size) {
    /**
     * TODO: [c7w] implement FreeSpaceMap
     * See
     *  - https://thu-db.github.io/dbs-tutorial/chapter-2/variable.html
     *  - https://github.com/postgres/postgres/blob/master/src/backend/storage/freespace/README
     */
    auto &meta{meta_map[table_id]};

    /**
     * Dummy implementation: check page by page
     */
    auto data_fd{table_data_fd[table_id]};
    for (int i{0}; i < meta->page_count; ++i) {
        auto page{buffer.ReadPage(data_fd, i)};
        auto data_page{std::make_shared<DataPage>(page, *meta)};
        if (data_page->Contains(size)) {
            return data_page;
        }
    }

    // no enough space on current pages
    auto page{buffer.CreatePage(data_fd, meta->page_count++)};
    auto data_page{std::make_shared<DataPage>(page, *meta)};
    data_page->Init();
    return data_page;
}

std::shared_ptr<Result> DBSystem::InsertResult() {
    auto res{std::shared_ptr<Result>(new TextResult{fmt::format("{} records inserted", insert_record_counter)})};
    insert_record_counter = 0;
    return res;
}

void DBSystem::CheckConstraint(const TableMeta &meta, const std::shared_ptr<Record> &record) const {
    // check not null
    for (int i{0}; i < meta.field_meta.Count(); ++i) {
        if (record->fields[i]->is_null && meta.field_meta.meta[i]->not_null) {
            throw OperationError{"Column `{}` cannot be null", meta.field_meta.meta[i]->name};
        }
    }

    // TODO: check unique

    // TODO: check primary key implies not null and unique
    if (meta.primary_key != nullptr) {
        for (int i{0}; i < meta.primary_key->field_count; ++i) {

        }
    }

    // TODO: check foreign key
    for (const auto &fk: meta.foreign_keys) {
        for (int i{0}; i < fk->field_count; ++i) {

        }
    }
}

void DBSystem::Insert(TableID table_id, std::shared_ptr<Record> &record) {
    auto page_to_insert{FindPageWithSpace(table_id, record->Size())};
    page_to_insert->Insert(record);
    ++insert_record_counter;
}

std::shared_ptr<Result> DBSystem::DescribeTable(const std::string &table_name) {
    auto table_id{GetTableID(table_name)};
    auto table_meta{meta_map[table_id]};

    std::vector<std::string> header = {"Field", "Type", "Null", "Default"};
    std::vector<std::vector<std::string>> field_detail;

    // field meta
    for (const auto &fm: table_meta->field_meta.meta) {
        std::string type_name{magic_enum::enum_name(fm->type)};
        if (fm->type == FieldType::CHAR || fm->type == FieldType::VARCHAR) {
            type_name += fmt::format("({})", fm->max_size);
        }
        field_detail.push_back({
                                       fm->name,
                                       type_name,
                                       fm->not_null ? "NO" : "YES",
                                       fm->has_default ? fm->default_value->ToString() : "NULL"
                               });
    }

    auto result{std::make_shared<TableResult>(header, field_detail)};

    // primary key constraint
    if (table_meta->primary_key != nullptr) {
        std::string pk_fields;
        for (int i{0}; i < table_meta->primary_key->field_count; ++i) {
            pk_fields += table_meta->field_meta.meta[table_meta->primary_key->fields[i]]->name;
            if (i != table_meta->primary_key->field_count - 1) {
                pk_fields += ", ";
            }
        }
        result->AddInfo(fmt::format("PRIMARY KEY ({})", pk_fields));
    }

    // unique constraints
    std::vector<std::string> unique_field_names;
    for (const auto &fm: table_meta->field_meta.meta) {
        unique_field_names.push_back(fmt::format("UNIQUE ({});", fm->name));
    }
    std::sort(unique_field_names.begin(), unique_field_names.end());
    std::for_each(unique_field_names.begin(), unique_field_names.end(),
                  [&result](const auto &_info) { return result->AddInfo(_info); });

    // foreign key constraints
    for (const auto &fk: table_meta->foreign_keys) {
        std::string fk_fields;
        for (int i{0}; i < fk->field_count; ++i) {
            fk_fields += table_meta->field_meta.meta[fk->fields[i]]->name;
            if (i != fk->field_count - 1) {
                fk_fields += ", ";
            }
        }

        auto ref_name_iter{table_name_map.right.find(fk->reference_table)};
        assert(ref_name_iter != table_name_map.right.end());
        auto fk_reference_table_name{ref_name_iter->second};
        std::string fk_reference_fields;
        for (int i{0}; i < fk->field_count; ++i) {
            fk_reference_fields += table_meta->field_meta.meta[fk->reference_fields[i]]->name;
            if (i != fk->field_count - 1) {
                fk_reference_fields += ", ";
            }
        }

        result->AddInfo(fmt::format(
                "FOREIGN KEY {}({}) REFERENCES {}({})",
                fk->name,
                fk_fields,
                fk_reference_table_name,
                fk_reference_fields
        ));
    }

}
