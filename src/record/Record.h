//
// Created by lambda on 22-11-19.
//

#ifndef DBS_TUTORIAL_RECORD_H
#define DBS_TUTORIAL_RECORD_H

#include <vector>

#include <record/NullBitmap.h>
#include <record/Field.h>

class TableMeta;

class Record {
public:
    NullBitmap nulls;
    std::vector<std::shared_ptr<Field>> fields;

    Record(const NullBitmap &nulls, const std::vector<std::shared_ptr<Field>> &&fields);

    static std::shared_ptr<Record> FromSrc(const uint8_t *&src, TableMeta &meta);

    void Write(uint8_t *&dst);

    RecordSize Size() {
        RecordSize size{0};
        for (const auto &f: fields) {
            size += f->Size();
        }
        return size;
    }
};

typedef std::vector<Record> RecordList;

#endif //DBS_TUTORIAL_RECORD_H
