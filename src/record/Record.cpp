//
// Created by lambda on 22-11-19.
//

#include "Record.h"
#include <utils/Serialization.h>
#include <record/Field.h>
#include <record/TableMeta.h>

Record::Record(std::vector<std::shared_ptr<Field>> fields) : fields{std::move(fields)} {}

void Record::Write(uint8_t *&dst) {
    write_var(dst, Size());
    NullBitmap nulls{static_cast<FieldID>(fields.size())};
    for (int i{0}; i < fields.size(); ++i) {
        if (fields[i]->is_null) {
            nulls.Set(i);
        }
    }
    nulls.Write(dst);
    for (const auto& field: fields) {
        if (!field->is_null) {  // skip null records
            field->Write(dst);
        }
    }
}

std::shared_ptr<Record> Record::FromSrc(const uint8_t *&src, const TableMeta &meta) {
    RecordSize read_size;
    read_var(src, read_size);
    auto field_count{meta.field_meta.Count()};
    auto nulls = NullBitmap::FromSrc(src, field_count);
    std::vector<std::shared_ptr<Field>> fields;
    // NOTE: Assume that record is kept within a page
    for (int i{0}; i < field_count; ++i) {
        const auto& field{meta.field_meta.meta[i]};
        if (nulls->Get(i)) {
            fields.push_back(Field::MakeNull(field->type, field->max_size));
            // null read is skipped
        } else {
            fields.push_back(Field::LoadField(field->type, src, field->max_size));
        }
    }
    auto result{std::make_shared<Record>(std::move(fields))};
    assert(result->Size() == read_size);
    return result;
}
