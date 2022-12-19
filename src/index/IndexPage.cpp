//
// Created by c7w on 2022/12/18.
//

#include "IndexPage.h"

std::shared_ptr<IndexRecord> IndexPage::Select(TreeOrder slot) const {
    auto base_offset = sizeof(IndexPage::PageHeader);
    auto offset = base_offset + slot * IndexRecordSize();
    const uint8_t *src = page->data + offset;
    return IndexRecord::FromSrc(src, meta, header.is_leaf);
}

TreeOrder IndexPage::Insert(TreeOrder slot, std::shared_ptr<IndexRecord> record) {
    assert (header.child_cnt <= meta.m);

    auto offset = sizeof(IndexPage::PageHeader) + slot * IndexRecordSize();
    auto size = (ChildCount() - slot) * IndexRecordSize();
    auto base = page->data;
    memmove(base + offset + IndexRecordSize(), base + offset, size);

    uint8_t *dst = page->data + offset;

    record = CastRecord(record);  // Decide use internal writing pattern or leaf writing pattern
    assert (record != nullptr);
    record->Write(dst);

    page->SetDirty();

    return header.child_cnt++;
}


void IndexPage::Update(TreeOrder slot, std::shared_ptr<IndexRecord> record) {
    assert (slot < header.child_cnt);

    auto offset = sizeof(IndexPage::PageHeader) + slot * IndexRecordSize();
    uint8_t *dst = page->data + offset;

    record = CastRecord(record);  // Decide use internal writing pattern or leaf writing pattern
    record->Write(dst);
    page->SetDirty();
}


void IndexPage::Delete(TreeOrder slot_id) {
    assert (slot_id < header.child_cnt);
    auto base_offset = sizeof(IndexPage::PageHeader);
    auto offset = base_offset + slot_id * IndexRecordSize();
    uint8_t *dst = page->data + offset;

    // Move trailing records forward
    for (TreeOrder i = slot_id + 1; i < header.child_cnt; i++) {
        auto src_offset = base_offset + i * IndexRecordSize();
        uint8_t *src = page->data + src_offset;
        std::memmove(dst, src, IndexRecordSize());
        dst += IndexRecordSize();
    }

    page->SetDirty();
    header.child_cnt--;
}
