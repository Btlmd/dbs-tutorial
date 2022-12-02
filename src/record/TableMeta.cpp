//
// Created by lambda on 22-11-20.
//

#include "TableMeta.h"

#include <record/Field.h>
#include <utils/Serialization.h>

#include <utility>

/**
 * TableMeta Layout
 *
 * Page 0: Fixed info (ensured that will not exceed one page)
 *
 * Page 1 ~ Page n: Foreign Keys (each foreign key has fixed length)
 *
 * Page (n + 1) ~ Page k: FieldMetas
 *
 * Page (k + 1) ~ Page ([total_page_count] - 1): FSM Pages
 */


std::shared_ptr<TableMeta> TableMeta::FromSrc(FileID fd, BufferSystem &buffer) {
    auto ret{std::make_shared<TableMeta>(buffer)};
    PageID page_id{-1};
    Page *page;
    const uint8_t *src;

    auto NextPage{[&](){
        ++page_id;
        page = buffer.ReadPage(fd, page_id);
        src = page->data;
    }};

    // Fixed Info
    NextPage();
    read_var(src, ret->table_id);
    read_var(src, ret->data_page_count);
    read_string(src, ret->table_name);
    FieldID field_count;
    read_var(src, field_count);
    FieldID fk_count;
    read_var(src, fk_count);
    bool has_pk;
    read_var(src, has_pk);
    if (has_pk) {
        auto pk{std::make_shared<PrimaryKey>()};
        read_var(src, *pk);
        ret->primary_key = pk;
    } else {
        ret->primary_key = nullptr;
    }

    // Foreign Keys
    FieldID fk_loaded{0};
    while (fk_loaded < fk_count) {
        NextPage();
        for (FieldID j{0}; j < FK_PER_PAGE && fk_loaded < fk_count; ++j) {
            ForeignKey fk;
            read_var(src, fk);
            ret->foreign_keys.push_back(std::make_shared<ForeignKey>(fk));
            ++fk_loaded;
        }
    }

    // Fields
    FieldID field_loaded{0};
    while (field_loaded < field_count) {
        NextPage();
        for (FieldID j{0}; j < FK_PER_PAGE && field_loaded < field_count; ++j) {
            ret->field_meta.Insert(FieldMeta::FromSrc(src));
            ++fk_loaded;
        }
    }

    return ret;
}

void TableMeta::Write() {
    PageID page_id{-1};
    Page *page;
    uint8_t *page_end;
    uint8_t *dst;

    auto NextPage{[&]() {
        ++page_id;
        page = buffer.ReadPage(fd, page_id);
        page->SetDirty();
        dst = page->data;
        page_end = page->data + PAGE_SIZE;
    }};

    // Fixed Info
    NextPage();
    write_var(dst, table_id);
    write_var(dst, data_page_count);
    write_string(dst, table_name);
    write_var(dst, static_cast<FieldID>(field_meta.Count()));
    write_var(dst, static_cast<FieldID>(foreign_keys.size()));
    write_var(dst, bool(primary_key == nullptr));
    if (primary_key != nullptr) {
        write_var(dst, *primary_key);
    }

    // Foreign Keys
    for (FieldID i{0}; i < foreign_keys.size(); ++i) {
        if (i % FK_PER_PAGE == 0) {
            NextPage();
        }
        write_var(dst, *foreign_keys[i]);
    }

    // Fields
    FieldID fields_written;
    auto EndThisPage{[&]() {
        uint8_t *header{page->data};
        write_var(header, fields_written);  // mark written field count
    }};
    auto TurnNewPage{[&]() {
        NextPage();
        dst += sizeof(fields_written);  // skip field_count
        fields_written = 0;
    }};

    TurnNewPage();
    for (FieldID i{0}; i < field_meta.meta.size(); ++i) {
        auto &f{field_meta.meta[i]};
        auto end_pos{dst + f->Size()};
        if (end_pos > page_end) {
            EndThisPage();
            TurnNewPage();
        }
        f->Write(dst);
        ++fields_written;
    }
    EndThisPage();
}