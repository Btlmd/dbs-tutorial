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
    std::vector<Field *> fields;

    Record(const NullBitmap &nulls, const std::vector<Field *> &&fields);

    static Record *FromSrc(const uint8_t *&src, TableMeta &meta);

    void Write(uint8_t *&dst);

    RecordSize Size();
};

typedef std::vector<Record> RecordList;

#endif //DBS_TUTORIAL_RECORD_H
