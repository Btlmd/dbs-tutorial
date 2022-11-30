//
// Created by lambda on 22-11-19.
//

#include "Record.h"
#include <utils/Serialization.h>
#include <record/Field.h>
#include <record/TableMeta.h>

Record::Record(std::shared_ptr<NullBitmap> nulls, std::vector<std::shared_ptr<Field>> fields) : nulls{std::move(nulls)}, fields{std::move(fields)} {

}

void Record::Write(uint8_t *&dst) {
    nulls->Write(dst);
    for (const auto& field: fields) {
        field->Write(dst);
    }
}

std::shared_ptr<Record> Record::FromSrc(const uint8_t *&src, TableMeta &meta) {
    auto nulls = NullBitmap::FromSrc(src, meta.field_meta.Count());
    std::vector<std::shared_ptr<Field>> fields;
    // NOTE: Assume that record is kept within a page
    for (const auto& field: meta.field_meta.field_seq) {
        fields.push_back(Field::LoadField(field->type, src, field->max_size));
    }
    return std::make_shared<Record>(nulls, std::move(fields));
}
