//
// Created by c7w on 2022/12/18.
//

#ifndef DBS_TUTORIAL_INDEXFILE_H
#define DBS_TUTORIAL_INDEXFILE_H

#include <defines.h>
#include <io/BufferSystem.h>
#include <index/IndexMeta.h>
#include <index/IndexPage.h>

// A Typical B+Tree structure.

class IndexFile {

    TableID table_id{-1};
    std::shared_ptr<IndexMeta> meta;
    std::vector<FieldID> field_ids;

    explicit IndexFile(BufferSystem& buffer, FileID fd) : buffer{buffer}, fd{fd} {
        // Read Page 0
        Page* page = buffer.ReadPage(fd, 0);
        const uint8_t *src = page->data;
        read_var(src, table_id);
        meta = IndexMeta::FromSrc(src);

        int field_num;
        read_var(src, field_num);
        for (int i = 0; i < field_num; ++i) {
            FieldID field_id;
            read_var(src, field_id);
            field_ids.push_back(field_id);
        }
    }

    ~IndexFile() {
        // Write back Page 0
        Page* page = buffer.ReadPage(fd, 0);
        uint8_t *dst = page->data;
        write_var(dst, table_id);
        meta->Write(dst);
        write_var(dst, (int)field_ids.size());
        for (auto field_id : field_ids) {
            write_var(dst, field_id);
        }
        page->SetDirty();
    }

    /* Interfaces for index searching */
    // Return the position of the first element that is greater than or equal to key
    std::pair<PageID, TreeOrder> SelectRecord(IndexField* key);
    void InsertRecord(PageID page_id, SlotID slot_id, IndexField* key);
    void DeleteRecord(PageID page_id, SlotID slot_id, IndexField* key);
    void UpdateRecord(PageID page_id_old, SlotID slot_id_old, IndexField* key_old,
                      PageID page_id, SlotID slot_id, IndexField* key) {
        DeleteRecord(page_id_old, slot_id_old, key_old);
        InsertRecord(page_id, slot_id, key);
    }


    /* Interfaces for index management */
    PageID PageNum() {
        return meta->page_num;
    }

   private:
    BufferSystem &buffer;
    FileID fd{};
};

#endif  // DBS_TUTORIAL_INDEXFILE_H
