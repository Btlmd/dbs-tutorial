//
// Created by lambda on 22-11-20.
//

#include "TableMeta.h"

#include <record/Field.h>
#include <utils/Serialization.h>

#include <utility>

TableMeta *TableMeta::FromSrc(FileID fd, BufferSystem &buffer) {
    /**
     *  - Page 0
     *    {field_count, page_count}
     *    {field_in_page}
     *    {FieldMeta [0]}
     *    ...
     *    {FieldMeta [i]}
     *
     *  - Page 1
     *    {field_in_page}
     *    {FieldMeta [i + 1]}
     *    ...
     *    {FieldMeta [field_count]}
     *
     *  - Page k  // begin FSM page
     *
     */
    auto meta = new TableMeta{buffer};
    auto curr_page{0};
    const uint8_t *ptr{buffer.ReadPage(fd, curr_page)->data};
    FieldID field_in_page, field_count;
    // load field_count and page_count
    read_var(ptr, field_count);
    read_var(ptr, meta->page_count);

    for (int i{0}; i < field_count; ++curr_page, ptr = buffer.ReadPage(fd, curr_page)->data) {
        read_var(ptr, field_in_page);
        for (int j{0}; j < field_in_page; ++j, ++i) {
            meta->field_meta.push_back(FieldMeta::FromSrc(ptr));
        }
    }
    return meta;
}




void TableMeta::Write() {
    PageID curr_page{0};
    auto page{buffer.ReadPage(fd, curr_page)};
    uint8_t *dst{page->data};
    uint8_t *page_end{dst + PAGE_SIZE};
    write_var(dst, FieldID(field_meta.size()));
    write_var(dst, page_count);

    auto page_head{dst};
    dst += sizeof(FieldID);
    FieldID field_in_page{0};
    for (const auto& field: field_meta) {
        if ((page_end - dst) < field->Size()) {
            write_var(page_head, field_in_page);

            field_in_page = 0;
            ++curr_page;
            page = buffer.ReadPage(fd, curr_page);  // page read here will be written
            page->SetDirty();
            dst = page->data;
            page_head = dst;
            page_end = dst + PAGE_SIZE;
        }
        field->Write(dst);
    }
    write_var(page_head, field_in_page);
}


