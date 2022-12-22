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
#include <node/ScanNode.h>


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
    Trace("Opening dataset " << db_name);
    on_use = true;
    /**
     * Table File Structure
     * {table_count}
     * {table_id, table_name}
     * ...
     */

    // load tables
    table_info_fd = FileSystem::OpenFile(DB_DIR / db_name / TABLE_FILE);
    const uint8_t *table_info_ptr{buffer.ReadPage(table_info_fd, 0)->data};

    // read into buffer data structure, since data may be swapped out by buffer system
    read_var(table_info_ptr, table_count);
    std::vector<std::string> table_names;
    std::vector<TableID> table_ids;
    for (int i{0}; i < table_count; ++i) {
        std::string table_name;
        TableID table_id;
        read_var(table_info_ptr, table_id);
        read_string(table_info_ptr, table_name);
        table_names.push_back(std::move(table_name));
        table_ids.push_back(table_id);
    }

    for (int i{0}; i < table_count; ++i) {
        auto table_id{table_ids[i]};
        auto table_name{table_names[i]};
        table_name_map.insert({table_name, table_id});
        table_data_fd[table_id] = FileSystem::OpenFile(DB_DIR / db_name / fmt::format(TABLE_DATA_PATTERN, table_id));
        table_meta_fd[table_id] = FileSystem::OpenFile(DB_DIR / db_name / fmt::format(TABLE_META_PATTERN, table_id));
        meta_map[table_id] = TableMeta::FromSrc(table_meta_fd[table_id], buffer);

        // Insert into table_index_fd
        for (auto &index: meta_map[table_id]->index_keys) {
            std::vector<FieldID> field_ids;
            for (int ii = 0; ii < index->field_count; ++ii) {
                field_ids.push_back(index->fields[ii]);
            }
            table_index_fd[std::make_pair((TableID) table_id, field_ids)] =
                    FileSystem::OpenFile(DB_DIR / db_name / GetIndexFilePath(table_id, field_ids));
        }
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

    Trace("Closing database " << current_database);

    // Release IndexFile
    table_index_file.clear();

    // Update table_info
    auto table_info_page = buffer.ReadPage(table_info_fd, 0);
    uint8_t *table_info_ptr{table_info_page->data};
    write_var(table_info_ptr, table_count);
    for (const auto &[table_name, table_id]: table_name_map) {
        write_var(table_info_ptr, table_id);
        write_string(table_info_ptr, table_name);
    }
    table_info_page->SetDirty();

    buffer.CloseFile(table_info_fd);

    for (const auto &[table_id, table_meta]: meta_map) {
        table_meta->Write();
    }

    // close file descriptors

    auto close_func = [this](const std::pair<TableID, FileID> &relation) -> void {
        buffer.CloseFile(relation.second);
    };

    auto close_func_index = [this](
            const std::pair<std::pair<TableID, std::vector<FieldID>>, FileID> &relation) -> void {
        buffer.CloseFile(relation.second);
    };

    std::for_each(table_meta_fd.begin(), table_meta_fd.end(), close_func);
    std::for_each(table_data_fd.begin(), table_data_fd.end(), close_func);
    std::for_each(table_index_fd.begin(), table_index_fd.end(), close_func_index);

    // reset table status
    table_name_map.clear();
    table_data_fd.clear();
    table_meta_fd.clear();
    table_index_fd.clear();
    meta_map.clear();

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
    CheckOnUse();

    if (CheckTableExist(table_name)) {
        throw OperationError{"Table `{}` already exists", table_name};
    }

    // assign file descriptor

    TableID table_id{NextTableID()};

    /**
     * Refactored: field position and field id is just the same
     * So field deletion should update all possible foreign keys in the database
     */
    std::string meta_path{DB_DIR / current_database / fmt::format(TABLE_META_PATTERN, table_id)};
    FileID meta_fd{FileSystem::NewFile(meta_path)};
    std::shared_ptr<TableMeta> meta{nullptr};

    try {
        meta = std::make_shared<TableMeta>(
                table_id, table_name, 0, FieldMetaTable{field_meta}, meta_fd, buffer);

        // primary key and foreign keys
        if (raw_pk) {
            AddPrimaryKey(meta, raw_pk.value());
        }
        for (const auto &fk: raw_fks) {
            AddForeignKey(meta, fk);
        }
    } catch (std::exception &e) {
        buffer.CloseFile(meta_fd);
        FileSystem::RemoveFile(meta_path);
        throw;
    }

    // if all previous step succeed, create table in DBSystem

    table_data_fd[table_id] = FileSystem::NewFile(
            DB_DIR / current_database / fmt::format(TABLE_DATA_PATTERN, table_id));
    table_meta_fd[table_id] = meta_fd;
    table_name_map.insert({table_name, table_id});
    assert(meta != nullptr);
    meta_map[table_id] = std::move(meta);
    ++table_count;

    return std::shared_ptr<Result>{new TextResult{"Query OK"}};
}

std::shared_ptr<Result> DBSystem::DropTable(const std::string &table_name) {
    auto table_id{GetTableID(table_name)};
    buffer.CloseFile(table_meta_fd[table_id]);
    buffer.CloseFile(table_data_fd[table_id]);
    FileSystem::RemoveFile(DB_DIR / current_database / fmt::format(TABLE_DATA_PATTERN, table_id));
    FileSystem::RemoveFile(DB_DIR / current_database / fmt::format(TABLE_META_PATTERN, table_id));

    // TODO: drop index

    table_data_fd.erase(table_id);
    table_meta_fd.erase(table_id);
    meta_map.erase(table_id);
    table_name_map.right.erase(table_id);
    --table_count;
    return std::make_shared<TextResult>("Query OK");
}

std::shared_ptr<Result> DBSystem::ShowTables() const {
    CheckOnUse();
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
    for (int i{0}; i < meta->data_page_count; ++i) {
        auto page{buffer.ReadPage(data_fd, i)};
        auto data_page{std::make_shared<DataPage>(page, *meta)};
        if (data_page->Contains(size)) {
            return data_page;
        }
    }

    // no enough space on current pages
    auto page{buffer.CreatePage(data_fd, meta->data_page_count++)};
    auto data_page{std::make_shared<DataPage>(page, *meta)};
    data_page->Init();
    return data_page;
}

void DBSystem::CheckConstraint(const TableMeta &meta, const std::shared_ptr<Record> &record) {
    auto field_count{meta.field_meta.Count()};
    auto table_id{meta.table_id};

    // check not null
    for (int i{0}; i < meta.field_meta.Count(); ++i) {
        if (record->fields[i]->is_null && meta.field_meta.meta[i]->not_null) {
            throw OperationError{"Column `{}` cannot be null", meta.field_meta.meta[i]->name};
        }
    }

//    // check unique
//    for (const auto &uk: meta.unique_keys) {
//        if (uk->field_count == 0 && meta.field_meta.meta[0]->type == FieldType::INT) { // use index
//            if (GetIndexFile(table_id, uk->fields[0])
//                    ->Contains(IndexINT::FromDataField({record->fields[uk->fields[0]]}))
//                    ) {
//                auto repr{record->fields[uk->fields[0]]->ToString()};
//                throw OperationError{"Unique Constraint `{}` Failed: {} already exists", uk->name, repr};
//            }
//        } else { // use trivial scan
//            auto scan{GetTrivialScanNode(meta.table_id, nullptr)};
//            while (!scan->Over()) {
//                auto records{scan->Next()};
//                for (const auto &r: records) {
//                    for (FieldID i{0}; i < uk->field_count; ++i) {
//
//                    }
//                }
//            }
//        }
//
//        for (int i{0}; i < uk->field_count; ++i) {
//            auto field_id{uk->fields[i]};
//            if (meta.field_meta.meta[field_id]->type == FieldType::INT) {
//
//            } else {
//
//            }
//        }
//    }
//
//    // TODO: check primary key implies not null and unique
//    if (meta.primary_key != nullptr) {
//        for (int i{0}; i < meta.primary_key->field_count; ++i) {
//            auto field_id{meta.primary_key->fields[i]};
//
//        }
//    }
//
//    // TODO: check foreign key
//    for (const auto &fk: meta.foreign_keys) {
//        for (int i{0}; i < fk->field_count; ++i) {
//
//        }
//    }
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

    // TODO: unique constraints
//    std::vector<std::string> unique_field_names;
//    for (const auto &fm: table_meta->field_meta.meta) {
//        if (fm->unique) {
//            unique_field_names.push_back(fmt::format("UNIQUE ({});", fm->name));
//        }
//    }
//    std::sort(unique_field_names.begin(), unique_field_names.end());
//    std::for_each(unique_field_names.begin(), unique_field_names.end(),
//                  [&result](const auto &_info) { return result->AddInfo(_info); });

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

    for (const auto &index: table_meta->index_keys) {
        std::string index_fields;
        for (int i{0}; i < index->field_count; ++i) {
            index_fields += table_meta->field_meta.meta[index->fields[i]]->name;
            if (i != index->field_count - 1) {
                index_fields += ", ";
            }
        }
        if (index->user_created) {
            result->AddInfo(fmt::format("INDEX ({})", index_fields));
        }
    }

    return result;
}


std::shared_ptr<OpNode> DBSystem::GetTrivialScanNode(TableID table_id, const std::shared_ptr<FilterCondition> &cond) {
    return std::make_shared<TrivialScanNode>(buffer, meta_map[table_id], cond, table_data_fd[table_id]);
}


std::shared_ptr<OpNode> DBSystem::GetIndexScanNode(TableID table_id, std::vector<FieldID> field_ids,
                                                   const std::shared_ptr<FilterCondition> &cond,
                                                   const std::shared_ptr<IndexField> &key_start,
                                                   const std::shared_ptr<IndexField> &key_end) {
    auto index_file = GetIndexFile(table_id, field_ids);
    return std::make_shared<IndexScanNode>(buffer, meta_map[table_id], cond, table_data_fd[table_id], *index_file, key_start, key_end);
}


std::shared_ptr<Result> DBSystem::Select(const std::vector<std::string> &header, const std::shared_ptr<OpNode> &plan) {
    auto records{plan->All()};
    return std::make_shared<TableResult>(header, std::move(records));
}

std::shared_ptr<Result> DBSystem::Insert(TableID table_id, RecordList &records) {
    for (const auto &record: records) {
        InsertRecord(table_id, record);
    }
    return std::make_shared<TextResult>(fmt::format("{} record(s) inserted", records.size()));
}

std::shared_ptr<Result> DBSystem::Delete(TableID table_id, const std::shared_ptr<FilterCondition> &cond) {
    assert(cond != nullptr);
    auto data_fd{table_data_fd[table_id]};
    std::size_t delete_counter{0};
    for (PageID i{0}; i < meta_map[table_id]->data_page_count; ++i) {
        auto page = buffer.ReadPage(data_fd, i);
        page->Lock();
        DataPage dp{page, *meta_map[table_id]};
        RecordList ret;
        for (FieldID j{0}; j < dp.header.slot_count; ++j) {
            auto record{dp.Select(j)};
            if (record != nullptr && (*cond)(record)) {
                ++delete_counter;

                // delete entries in index
                DropRecordIndex(table_id, page->id, j, record);

                // perform delete
                dp.Delete(j);
            }
        }
        page->Release();
    }
    return std::make_shared<TextResult>(fmt::format("{} record(s) deleted", delete_counter));
}

void DBSystem::CheckOnUse() const {
    if (!on_use) {
        throw OperationError{"No database selected"};
    }
}

std::shared_ptr<Result>
DBSystem::Update(TableID table_id, const std::vector<std::pair<std::shared_ptr<FieldMeta>,
        std::shared_ptr<Field>>> &updates, const std::shared_ptr<FilterCondition> &cond) {
    auto data_fd{table_data_fd[table_id]};
    auto meta{meta_map[table_id]};
    std::size_t update_counter{0};
    std::unordered_set<FieldID> affected;
    for (const auto &[fm, val]: updates) {
        affected.insert(fm->field_id);
    }
    for (PageID i{0}; i < meta_map[table_id]->data_page_count; ++i) {
        auto page = buffer.ReadPage(data_fd, i);
        page->Lock();
        DataPage dp{page, *meta};
        RecordList ret;
        for (FieldID j{0}; j < dp.header.slot_count; ++j) {
            auto record{dp.Select(j)};
            if (record != nullptr && (*cond)(record)) {
                auto updated_record{record->Copy()};
                updated_record->Update(updates);

                // check for constraints
                CheckConstraint(*meta, updated_record);

                ++update_counter;
                auto original_size{record->Size()};
                if (updated_record->Size() > original_size) {
                    DropRecordIndex(table_id, page->id, j, record);
                    dp.Delete(j);
                    InsertRecord(table_id, updated_record); // index inserted through this function
                } else {  // in-place update
                    dp.Update(j, updated_record);
                    UpdateInPlaceRecordIndex(affected, table_id, page->id, j, record, updated_record);
                }
            }
        }
        page->Release();
    }
    return std::make_shared<TextResult>(fmt::format("{} record(s) updated", update_counter));
}

void DBSystem::InsertRecord(TableID table_id, const std::shared_ptr<Record> &record) {
    auto page{FindPageWithSpace(table_id, record->Size())};
    auto slot_id{page->Insert(record)};
    InsertRecordIndex(table_id, page->page->id, slot_id, record);
}


std::shared_ptr<Result> DBSystem::AddIndex(const std::string &table_name, const std::vector<std::string> &field_name) {
    auto table_id = GetTableID(table_name);
    auto table_meta{meta_map[table_id]};

    // Get FieldID
    std::vector<FieldID> field_id;
    for (const auto &field: field_name) {
        auto id = table_meta->field_meta.ToID(field);
        if (id == -1) {
            throw OperationError("Field {} not found", field);
        }
        field_id.push_back(id);
    }

    return AddIndex(table_id, field_id, true);
}

std::shared_ptr<Result> DBSystem::AddIndex(TableID table_id, const std::vector<FieldID> &field_ids, bool is_user) {
    // First, search in index_keys to see if the index already exists
    auto table_meta{meta_map[table_id]};

    auto new_index = std::make_shared<IndexKey>();
    new_index->field_count = 0;
    new_index->user_created = false;
    new_index->reference_count = 0;  // 1 if system-created, 0 if user-created
    if (!is_user) {
        new_index->reference_count = 1;
    }

    for (const auto &field_id: field_ids) {
        new_index->fields[new_index->field_count++] = field_id;
    }
    auto find_result = std::find_if(table_meta->index_keys.begin(), table_meta->index_keys.end(),
                                    [&](const auto &index) {
                                        return *index == *new_index;
                                    });

    if (find_result == table_meta->index_keys.end()) {

        table_meta->index_keys.push_back(new_index);

        // Create index file
        FileSystem::NewFile(DB_DIR / current_database / GetIndexFilePath(table_id, field_ids));
        FileID fd = FileSystem::OpenFile(DB_DIR / current_database / GetIndexFilePath(table_id, field_ids));

        IndexFieldType type = IndexFieldType::INVALID;
        if (field_ids.size() == 1 && table_meta->field_meta.meta[field_ids[0]]->type == FieldType::INT) {
            type = IndexFieldType::INT;
        } else if (field_ids.size() == 2 && table_meta->field_meta.meta[field_ids[0]]->type == FieldType::INT
                   && table_meta->field_meta.meta[field_ids[1]]->type == FieldType::INT) {
            type = IndexFieldType::INT2;
        } else {
            throw OperationError("Unsupported index field type.");
        }
        auto meta = std::make_shared<IndexMeta>(type);
        auto new_index_file = std::make_shared<IndexFile>(table_id, meta, field_ids, buffer, fd);
        table_index_fd[std::make_pair(table_id, field_ids)] = fd;

        TrivialScanNode scan_node{buffer, meta_map[table_id], nullptr, table_data_fd[table_id]};
        while (!scan_node.Over()) {
            RecordList records = scan_node.Next();
            PageID page_id = scan_node.current_page;
            const std::vector<SlotID> &slot_ids = scan_node.slot_ids;

            for (int i = 0; i < records.size(); i++) {
                std::shared_ptr<Record> record = records[i];

                // Gather fields
                std::vector<std::shared_ptr<Field>> fields;
                for (const auto &field_id: field_ids) {
                    fields.push_back(record->fields[field_id]);
                }

                new_index_file->InsertRecord(page_id, slot_ids[i], IndexField::FromDataField(type, fields));
            }
        }

    }

    auto index = *std::find_if(table_meta->index_keys.begin(), table_meta->index_keys.end(), [&](const auto &index) {
        return *index == *new_index;
    });


    if (is_user) {
        if (index->user_created) {
            throw OperationError("Index already created by user");
        } else {
            index->user_created = true;
            return std::make_shared<TextResult>("Query OK");
        }
    } else {
        ++index->reference_count;
        return std::make_shared<TextResult>("Query OK");
    }

}


std::shared_ptr<Result> DBSystem::DropIndex(const std::string &table_name, const std::vector<std::string> &field_name) {
    auto table_id = GetTableID(table_name);
    auto table_meta{meta_map[table_id]};

    // Get FieldID
    std::vector<FieldID> field_id;
    for (const auto &field: field_name) {
        auto id = table_meta->field_meta.ToID(field);
        if (id == -1) {
            throw OperationError("Field {} not found", field);
        }
        field_id.push_back(id);
    }

    return DropIndex(table_id, field_id, true);
}

std::shared_ptr<Result> DBSystem::DropIndex(TableID table_id, const std::vector<FieldID> &field_ids, bool is_user) {
    auto table_meta{meta_map[table_id]};

    auto new_index = std::make_shared<IndexKey>();
    new_index->field_count = 0;
    new_index->user_created = is_user;
    new_index->reference_count = 0;  // 1 if system-created, 0 if user-created
    if (!is_user) {
        new_index->reference_count = 1;
    }

    for (const auto &field_id: field_ids) {
        new_index->fields[new_index->field_count++] = field_id;
    }
    auto find_result = std::find_if(table_meta->index_keys.begin(), table_meta->index_keys.end(),
                                    [&](const auto &index) {
                                        return *index == *new_index;
                                    });

    if (find_result == table_meta->index_keys.end()) {
        throw OperationError("Index does not exist");
    }

    auto index = *find_result;

    if (is_user) {
        if (index->user_created) {
            index->user_created = false;
        } else {
            throw OperationError("Index does not created by user");
        }
    } else {
        if (index->reference_count == 0) {
            throw OperationError("Index does not exist");
        } else {
            --index->reference_count;
        }
    }

    // Finish up
    if (index->reference_count == 0 && !index->user_created) {
        table_meta->index_keys.erase(find_result);
        table_index_fd.erase(std::make_pair(table_id, field_ids));
        FileSystem::RemoveFile(DB_DIR / current_database / GetIndexFilePath(table_id, field_ids));
    }

    return std::shared_ptr<Result>{new TextResult{"Query OK"}};
}


void DBSystem::DropRecordIndex(TableID table_id, PageID page_id, SlotID j, const std::shared_ptr<Record> record) {
    for (const auto &ik: meta_map[table_id]->index_keys) {
        if (ik->field_count == 1) {
            auto index_field{IndexINT::FromDataField({record->fields[ik->fields[0]]})};
            GetIndexFile(table_id, ik->fields[0])->DeleteRecord(page_id, j, index_field);
            continue;
        }
        else if (ik->field_count == 2) {
            auto index_field{IndexINT2::FromDataField({record->fields[ik->fields[0]], record->fields[ik->fields[1]]})};
            GetIndexFile(table_id, ik->fields[0], ik->fields[1])->DeleteRecord(page_id, j, index_field);
            continue;
        }
    }
}

void DBSystem::InsertRecordIndex(TableID table_id, PageID page_id, SlotID j, const std::shared_ptr<Record> record) {
    for (const auto &ik: meta_map[table_id]->index_keys) {
        if (ik->field_count == 1) {
            auto index_field{IndexINT::FromDataField({record->fields[ik->fields[0]]})};
            GetIndexFile(table_id, ik->fields[0])->InsertRecord(page_id, j, index_field);
            continue;
        }
        else if (ik->field_count == 2) {
            auto index_field{IndexINT2::FromDataField({record->fields[ik->fields[0]], record->fields[ik->fields[1]]})};
            GetIndexFile(table_id, ik->fields[0], ik->fields[1])->InsertRecord(page_id, j, index_field);
            continue;
        }
    }
}

void
DBSystem::UpdateInPlaceRecordIndex(const std::unordered_set<FieldID> &affected,
                                   TableID table_id, PageID page_id, SlotID j,
                                   const std::shared_ptr<Record> &record_prev,
                                   const std::shared_ptr<Record> &record_updated) {
    for (const auto &ik: meta_map[table_id]->index_keys) {
        if (ik->field_count == 1) {
            if (affected.find(ik->fields[0]) == affected.end()) {
                continue;
            }

            auto index_field{IndexINT::FromDataField({record_prev->fields[ik->fields[0]]})};
            GetIndexFile(table_id, ik->fields[0])->DeleteRecord(page_id, j, index_field);
            index_field = IndexINT::FromDataField({record_updated->fields[ik->fields[0]]});
            GetIndexFile(table_id, ik->fields[0])->InsertRecord(page_id, j, index_field);
            continue;
        }
        if (ik->field_count == 2) {
            if (affected.find(ik->fields[0]) == affected.end() && affected.find(ik->fields[1]) == affected.end()) {
                continue;
            }
            auto index_field{
                    IndexINT2::FromDataField({record_prev->fields[ik->fields[0]], record_prev->fields[ik->fields[1]]})};
            GetIndexFile(table_id, ik->fields[0], ik->fields[1])->DeleteRecord(page_id, j, index_field);
            index_field = IndexINT2::FromDataField(
                    {record_updated->fields[ik->fields[0]], record_updated->fields[ik->fields[1]]});
            GetIndexFile(table_id, ik->fields[0], ik->fields[1])->InsertRecord(page_id, j, index_field);
            continue;
        }
    }
}


std::shared_ptr<OpNode> DBSystem::GetScanNodeByCondition(TableID table_id, const std::shared_ptr<AndCondition> &cond) {
    // Iterate through table indexes
    auto table_meta{meta_map[table_id]};
    auto index_keys = table_meta->index_keys;

    int valid_cnt = 0;
    std::vector<FieldID> field_ids;
    std::shared_ptr<IndexField> key_start, key_end;

    for (auto& index_key : index_keys) {
        auto result = index_key->FilterCondition(cond);
        if (result.first > valid_cnt) {
            valid_cnt = result.first;
            key_start = result.second.first;
            key_end = result.second.second;

            field_ids.clear();
            for (int i = 0; i < index_key->field_count; ++i) {
                field_ids.push_back(index_key->fields[i]);
            }
        }
    }

    if (valid_cnt == 0) {
        return GetTrivialScanNode(table_id, cond);
    } else {
        return GetIndexScanNode(table_id, field_ids, cond, key_start, key_end);
    }
}
