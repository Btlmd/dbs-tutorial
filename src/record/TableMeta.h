//
// Created by lambda on 22-11-20.
//

#ifndef DBS_TUTORIAL_TABLEMETA_H
#define DBS_TUTORIAL_TABLEMETA_H

#include <utility>
#include <vector>

#include <boost/bimap.hpp>

#include <stdexcept>
#include <fmt/core.h>

#include <defines.h>
#include <record/Field.h>
#include <io/BufferSystem.h>
#include <exception/OperationException.h>

class FieldMetaTable {
public:
    std::unordered_map<std::string, FieldID> name_id;
    std::vector<std::shared_ptr<FieldMeta>> meta;

    explicit FieldMetaTable(std::vector<std::shared_ptr<FieldMeta>> field_meta) {
        for (const auto &fm: field_meta) {
            name_id.insert({fm->name, fm->field_id});
        }
        meta = std::move(field_meta);
    }

    FieldMetaTable() = default;

    FieldID Count() const {
        assert(name_id.size() == meta.size());
        return name_id.size();
    }

    void Insert(const std::shared_ptr<FieldMeta> &fm) {
        name_id.insert({fm->name, fm->field_id});
        meta.push_back(fm);
    }

    FieldID ToID(const std::string &field_name) const noexcept {
        auto iter{name_id.find(field_name)};
        if (iter == name_id.end()) {
            return -1;
        }
        return iter->second;
    }

    template<typename E, typename... T>
    FieldID ToID(const std::string &field_name, fmt::format_string<T...> fmt, T &&... args) const {
        auto fid{ToID(field_name)};
        if (fid == -1) {
            throw E(fmt, args ...);
        }
        return fid;
    }

    template<typename E, typename... T>
    const std::shared_ptr<FieldMeta> &
    ToMeta(const std::string &field_name, fmt::format_string<T...> fmt, T &&... args) const {
        auto fid{ToID<E>(field_name, fmt, args...)};
        return meta[fid];
    }
};

class TableMeta {
public:
    TableID table_id{-1};
    PageID data_page_count{-1};
    std::string table_name;
    FieldMetaTable field_meta;
    std::shared_ptr<PrimaryKey> primary_key{nullptr};
    std::vector<std::shared_ptr<ForeignKey>> foreign_keys;
    std::vector<std::shared_ptr<UniqueKey>> unique_keys;
    std::vector<std::shared_ptr<IndexKey>> index_keys;  // （table id + fields）

    /**
     * Write TableMeta to file
     */
    void Write();

    static std::shared_ptr<TableMeta> FromSrc(FileID fd, BufferSystem &buffer);

    TableMeta(TableID table_id, std::string table_name, PageID page_count, FieldMetaTable field_meta, FileID fd,
              BufferSystem &buffer)
            : table_id{table_id}, table_name{std::move(table_name)}, fd{fd}, data_page_count{page_count},
              field_meta{std::move(field_meta)}, buffer{buffer} {
        Trace(fmt::format("TableMeta ({}, {}) is assigned with fd {}", this->table_id, this->table_name, fd));
    }

    TableMeta(FileID fd, BufferSystem &buffer) : fd{fd}, buffer{buffer} {}

    ~TableMeta(){
        Trace(fmt::format("TableMeta ({}, {}) destroyed", table_id, table_name));
    }
private:

    BufferSystem &buffer;
    FileID fd{};
};



constexpr int FK_PER_PAGE{PAGE_SIZE / sizeof(ForeignKey)};
constexpr int IK_PER_PAGE{PAGE_SIZE / sizeof(IndexKey)};
constexpr int UK_PER_PAGE{PAGE_SIZE / sizeof(UniqueKey)};
constexpr int FIELD_PER_PAGE{1};

#endif //DBS_TUTORIAL_TABLEMETA_H
