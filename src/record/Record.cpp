//
// Created by lambda on 22-11-19.
//

#include "Record.h"
#include <utils/Serialization.h>
#include <record/Field.h>
#include <record/TableMeta.h>

Record::Record(const NullBitmap & nulls, const std::vector<Field *> && fields) : nulls{nulls}, fields{fields} {

}

void Record::Write(uint8_t *&dst) {

}

Record *Record::FromSrc(const uint8_t *&src, TableMeta &meta) {
    auto nulls = NullBitmap{src};
    FieldID field_count;
    std::vector<Field *> fields;
    read_var(src, field_count);
    // NOTE: Assume that record is kept within a page
    for (const auto& field_id: meta.field_meta.field_seq) {
        auto field{meta.field_meta.id_meta[field_id]};
        fields.push_back(Field::LoadField(field->type, src, field->max_size));
    }
    return new Record{nulls, std::move(fields)};
}
