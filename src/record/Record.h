//
// Created by lambda on 22-11-19.
//

#ifndef DBS_TUTORIAL_RECORD_H
#define DBS_TUTORIAL_RECORD_H

#include <vector>

#include <record/NullBitmap.h>
#include <record/Field.h>

#include <fmt/format.h>
#include <boost/algorithm/string/join.hpp>

class TableMeta;

class Record;

typedef std::vector<std::shared_ptr<Record>> RecordList;

class Record {
public:
    std::vector<std::shared_ptr<Field>> fields;

    explicit Record() = default;

    explicit Record(std::vector<std::shared_ptr<Field>> fields);

    /**
     * Build a record from source, using information from `meta`
     * @param src
     * @param meta
     * @return
     */
    static std::shared_ptr<Record> FromSrc(const uint8_t *&src, const TableMeta &meta);

    /**
     * Serialize the record into `dst`
     * @param dst
     */
    void Write(uint8_t *&dst);

    /**
     * Return the size of the record in storage
     * @return
     */
    RecordSize Size() {
        if (size > 0) {
            return size;
        }
        auto _size{static_cast<RecordSize>((fields.size() + 7) / 8)};  // offset of null bitmap
        for (const auto &f: fields) {
            if (!f->is_null) {
                _size += f->Size();
            }
        }
        size = _size;
        return _size;
    }

    /**
     * Return a vector of fields as string
     * @return
     */
    [[nodiscard]] std::vector<std::string> ToString() const {
        std::vector<std::string> buffer;
        for (const auto &field: fields) {
            if (!field) {
                buffer.emplace_back("<nullptr>");
                continue;
            }
            if (field->is_null) {
                buffer.emplace_back("NULL");
            } else {
                buffer.emplace_back(field->ToString());
            }
        }
        return std::move(buffer);
    }

    /**
     * Update specified fields of a record
     * @param updates
     */
    void Update(const std::vector<std::pair<std::shared_ptr<FieldMeta>, std::shared_ptr<Field>>> &updates) {
        for (const auto&[field_meta, field_value]: updates) {
            assert(field_meta->field_id < fields.size());
            fields[field_meta->field_id] = field_value;
        }
        size = -1;
    }

#ifdef DEBUG

    /**
     * Return a string representation of the record
     * Used for debugging log
     * @return
     */
    [[nodiscard]] std::string Repr() const {
        return "(" + boost::algorithm::join(ToString(), ", ") + ")";
    }

#endif
private:
    RecordSize size{-1};
};

#endif //DBS_TUTORIAL_RECORD_H
