//
// Created by c7w on 2022/12/18.
//

#ifndef DBS_TUTORIAL_INDEXRECORD_H
#define DBS_TUTORIAL_INDEXRECORD_H

#include <defines.h>
#include <index/IndexMeta.h>
#include <index/IndexField.h>

class IndexRecord {
   public:
    IndexField* key;
    PageID page_id;
    static std::shared_ptr<IndexRecord> FromSrc(const uint8_t *&src, const IndexMeta& meta, bool is_leaf);
    virtual void Write(uint8_t *&dst) const = 0;
    virtual RecordSize Size() const = 0;
};

class IndexRecordInternal : public IndexRecord {
public:
    explicit IndexRecordInternal(PageID page_id, IndexField* key) : page_id{page_id}, key{key} {}

    static std::shared_ptr<IndexRecordInternal> FromSrc(const uint8_t *&src, const IndexMeta &meta) {
        PageID page_id;
        read_var(src, page_id);
        auto key = IndexField::LoadIndexFieldShort(meta.field_type, src);
        return std::make_shared<IndexRecordInternal>(page_id, key.get());
    }

    virtual void Write(uint8_t *&dst) const override {
        write_var(dst, page_id);
        key->WriteShort(dst);
    }

    virtual RecordSize Size() const override { return sizeof(PageID) + key->ShortSize(); }
};


class IndexRecordLeaf : public IndexRecord {
public:
    SlotID slot_id;
    explicit IndexRecordLeaf(PageID page_id, SlotID slot_id, IndexField* key)
                                        : page_id{page_id}, slot_id{slot_id}, key{key} {}

    static std::shared_ptr<IndexRecordLeaf> FromSrc(const uint8_t *&src, const IndexMeta &meta) {
        PageID page_id;
        read_var(src, page_id);
        SlotID slot_id;
        read_var(src, slot_id);
        auto key = IndexField::LoadIndexField(meta.field_type, src);
        return std::make_shared<IndexRecordLeaf>(page_id, slot_id, key.get());
    }

    virtual void Write(uint8_t *&dst) const override {
        write_var(dst, page_id);
        write_var(dst, slot_id);
        key->Write(dst);
    }

    virtual RecordSize Size() const override { return sizeof(PageID) + sizeof(SlotID) + key->Size(); }
};

#endif  // DBS_TUTORIAL_INDEXRECORD_H
