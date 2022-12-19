//
// Created by c7w on 2022/12/18.
//

#ifndef DBS_TUTORIAL_INDEXRECORD_H
#define DBS_TUTORIAL_INDEXRECORD_H

#include <defines.h>
#include <index/IndexMeta.h>
#include <index/IndexField.h>

class IndexMeta;

class IndexRecord {
   public:
    explicit IndexRecord(PageID page_id, std::shared_ptr<IndexField> key) : page_id{page_id}, key{key} {}
    std::shared_ptr<IndexField> key;
    PageID page_id;
    static std::shared_ptr<IndexRecord> FromSrc(const uint8_t *&src, const IndexMeta& meta, bool is_leaf);
    virtual void Write(uint8_t *&dst) const = 0;
    virtual RecordSize Size() const = 0;
};

class IndexRecordInternal : public IndexRecord {
public:
    explicit IndexRecordInternal(PageID page_id, std::shared_ptr<IndexField> key) : IndexRecord{page_id, key} {}

    static std::shared_ptr<IndexRecordInternal> FromSrc(const uint8_t *&src, const IndexMeta &meta);

    virtual void Write(uint8_t *&dst) const override {
        write_var(dst, page_id);
        key->WriteShort(dst);
    }

    virtual RecordSize Size() const override { return sizeof(PageID) + key->ShortSize(); }
};


class IndexRecordLeaf : public IndexRecord {
public:
    SlotID slot_id;
    explicit IndexRecordLeaf(PageID page_id, SlotID slot_id, std::shared_ptr<IndexField> key)
                                        : IndexRecord{page_id, key}, slot_id{slot_id} {}

    static std::shared_ptr<IndexRecordLeaf> FromSrc(const uint8_t *&src, const IndexMeta &meta);

    virtual void Write(uint8_t *&dst) const override {
        write_var(dst, page_id);
        write_var(dst, slot_id);
        key->Write(dst);
    }

    virtual RecordSize Size() const override { return sizeof(PageID) + sizeof(SlotID) + key->Size(); }

    [[nodiscard]] std::shared_ptr<IndexRecordInternal> ToInternal() const {
        return std::make_shared<IndexRecordInternal>(page_id, key);
    }
};

#endif  // DBS_TUTORIAL_INDEXRECORD_H
