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
    std::shared_ptr<NullBitmap> nulls;
    std::vector<std::shared_ptr<Field>> fields;

    Record(std::shared_ptr<NullBitmap> nulls, std::vector<std::shared_ptr<Field>> fields);

    /**
     * Build a record from source, using information from `meta`
     * @param src
     * @param meta
     * @return
     */
    static std::shared_ptr<Record> FromSrc(const uint8_t *&src, TableMeta &meta);

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
        RecordSize size{0};
        for (const auto &f: fields) {
            size += f->Size();
        }
        return size;
    }

    /**
     * Return a vector of fields as string
     * @return
     */
    [[nodiscard]] std::vector<std::string> ToString() const {
        std::vector<std::string> buffer;
        for (int i{0}; i < fields.size(); ++i) {
            if (nulls->Get(i)) {
                buffer.emplace_back("NULL");
            } else {
                buffer.emplace_back(fields[i]->ToString());
            }
        }
        return std::move(buffer);
    }

    /**
     * Return a string representation of the record
     * Used for debugging log
     * @return
     */
    [[nodiscard]] std::string Repr() const {
        return boost::algorithm::join(ToString(), ", ");
    }
};

#endif //DBS_TUTORIAL_RECORD_H
