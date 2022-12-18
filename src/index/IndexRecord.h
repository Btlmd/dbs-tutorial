//
// Created by c7w on 2022/12/18.
//

#ifndef DBS_TUTORIAL_INDEXRECORD_H
#define DBS_TUTORIAL_INDEXRECORD_H

#include <defines.h>
#include <index/IndexMeta.h>
#include <index/IndexField.h>

class IndexRecordInternal {
public:
    PageID page_id;
    IndexField* key;

    /**
     * Build a record from source, using information from `meta`
     * @param src
     * @param meta
     * @return
     */
    static std::shared_ptr<IndexRecordInternal> FromSrc(const uint8_t *&src, const IndexMeta &meta);

    /**
     * Serialize the record into `dst`
     * @param dst
     */
    void Write(uint8_t *&dst);

    /**
     * Return the size of the record in storage
     * @return
     */
    RecordSize Size() { return sizeof(PageID) + key->ShortSize(); }
};


class IndexRecordLeaf {
public:
    PageID page_id;
    SlotID slot_id;
    IndexField* key;

    /**
     * Build a record from source, using information from `meta`
     * @param src
     * @param meta
     * @return
     */
    static std::shared_ptr<IndexRecordLeaf> FromSrc(const uint8_t *&src, const IndexMeta &meta);

    /**
     * Serialize the record into `dst`
     * @param dst
     */
    void Write(uint8_t *&dst);

    /**
     * Return the size of the record in storage
     * @return
     */
    RecordSize Size() { return sizeof(PageID) + key->Size(); }
};

#endif  // DBS_TUTORIAL_INDEXRECORD_H
