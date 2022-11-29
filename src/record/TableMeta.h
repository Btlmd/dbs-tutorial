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

class FieldMeteTable {
public:
    std::unordered_map<std::string, FieldID> name_id;
    std::map<FieldID, FieldMeta *> id_meta;
    std::vector<FieldID> field_seq;

    FieldMeteTable(const std::vector<FieldMeta *> &field_meta) {
        for (auto fm: field_meta) {
            name_id.insert({fm->name, fm->field_id});
            id_meta.insert({fm->field_id, fm});
            field_seq.push_back(fm->field_id);
        }
    }

    FieldMeteTable() = default;

    void Insert(FieldMeta *fm) {
        name_id.insert({fm->name, fm->field_id});
        id_meta.insert({fm->field_id, fm});
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
};

class TableMeta {
public:

    PageID page_count;
    FieldMeteTable field_meta;
    PrimaryKey *primary_key{nullptr};
    std::vector<ForeignKey *> foreign_keys;

    void Write();

    static TableMeta *FromSrc(FileID fd, BufferSystem &buffer);

    TableMeta(PageID page_count, FieldMeteTable field_meta, FileID fd, BufferSystem &buffer)
            : fd{fd}, page_count{page_count}, field_meta{std::move(field_meta)}, buffer{buffer} {}

    ~TableMeta() {
        delete primary_key;

        /** not best practice,
         *  but I've already written so many factories that yields a raw pointer
         *  :(
         */
        for (auto &fk: foreign_keys) {
            delete fk;
        }
    }

    PageID FindFreeSpace(RecordSize size);

    RecordSize

private:
    TableMeta(BufferSystem buffer) : buffer{buffer} {}

    BufferSystem &buffer;
    FileID fd;
};


#endif //DBS_TUTORIAL_TABLEMETA_H
