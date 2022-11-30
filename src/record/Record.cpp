//
// Created by lambda on 22-11-19.
//

#include "Record.h"
#include <utils/Serialization.h>
#include <record/Field.h>
#include <record/TableMeta.h>

Record::Record(const NullBitmap & nulls, const std::vector<std::shared_ptr<Field>> && fields) : nulls{nulls}, fields{fields} {

}

void Record::Write(uint8_t *&dst) {

}

std::shared_ptr<Record> Record::FromSrc(const uint8_t *&src, TableMeta &meta) {
    auto nulls = NullBitmap{src};
    FieldID field_count;
    std::vector<std::shared_ptr<Field>> fields;
    read_var(src, field_count);
    // NOTE: Assume that record is kept within a page
    for (const auto& field: meta.field_meta.field_seq) {
        fields.push_back(Field::LoadField(field->type, src, field->max_size));
    }
    return std::make_shared<Record>(nulls, std::move(fields));
}
