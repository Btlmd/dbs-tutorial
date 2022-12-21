//
// Created by c7w on 2022/12/18.
//

#include "IndexPage.h"

std::shared_ptr<IndexRecord> IndexPage::Select(TreeOrder slot) {
    auto base_offset = sizeof(IndexPage::PageHeader);
    auto offset = base_offset + slot * IndexRecordSize();
    const uint8_t *src = page->data + offset;
    return IndexRecord::FromSrc(src, meta, header.is_leaf);
}

std::vector<std::shared_ptr<IndexRecord>> IndexPage::SelectRange(TreeOrder start, TreeOrder end) {
    auto base_offset = sizeof(IndexPage::PageHeader);
    const uint8_t *src;
    std::vector<std::shared_ptr<IndexRecord>> records {};
    for (int i = start; i <= end; ++i) {
        src = page->data + base_offset + i * IndexRecordSize();
        records.push_back(IndexRecord::FromSrc(src, meta, header.is_leaf));
    }
    return records;
}

TreeOrder IndexPage::Insert(TreeOrder slot, std::shared_ptr<IndexRecord> record) {
    assert (header.child_cnt + 1 <= meta.m + 1);

    auto offset = sizeof(IndexPage::PageHeader) + slot * IndexRecordSize();
    auto size = (ChildCount() - slot) * IndexRecordSize();
    auto base = page->data;
    memmove(base + offset + IndexRecordSize(), base + offset, size);

    uint8_t *dst = page->data + offset;

    record = CastRecord(record);  // Decide use internal writing pattern or leaf writing pattern
    assert (record != nullptr);
    record->Write(dst);

    page->SetDirty();

    SetChildCount(ChildCount() + 1);
    return ChildCount();
}

void IndexPage::InsertRange(TreeOrder slot, std::vector<std::shared_ptr<IndexRecord>> records) {
    assert (header.child_cnt + records.size() <= meta.m + 1);
    auto offset = sizeof(IndexPage::PageHeader) + slot * IndexRecordSize();
    auto size = (ChildCount() - slot) * IndexRecordSize();
    auto base = page->data;
    memmove(base + offset + records.size() * IndexRecordSize(), base + offset, size);

    uint8_t *dst;
    for (int i = 0; i < records.size(); ++i) {
        dst = page->data + offset + i * IndexRecordSize();
        auto record2 = CastRecord(records[i]);
        assert (record2 != nullptr);
        record2->Write(dst);
    }

    page->SetDirty();
    SetChildCount(ChildCount() + records.size());
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
        std::memcpy(dst, src, IndexRecordSize());
        dst += IndexRecordSize();
    }

    page->SetDirty();
    SetChildCount(ChildCount() - 1);
}


void IndexPage::DeleteRange(TreeOrder start, TreeOrder end) {
    // Delete [start, end]
    assert (start <= end && end < header.child_cnt);
    auto base_offset = sizeof(IndexPage::PageHeader);
    auto offset = base_offset + start * IndexRecordSize();
    uint8_t *dst = page->data + offset;

    // Move trailing records forward
    for (TreeOrder i = end + 1; i < header.child_cnt; i++) {
        auto src_offset = base_offset + i * IndexRecordSize();
        uint8_t *src = page->data + src_offset;
        std::memcpy(dst, src, IndexRecordSize());
        dst += IndexRecordSize();
    }

    page->SetDirty();
    SetChildCount(ChildCount() - (end - start + 1));
}


RecordSize IndexPage::IndexRecordSize() {
    return meta.Size(header.is_leaf);
}

bool IndexPage::IsOverflow() {
    return header.child_cnt > meta.m;
}

bool IndexPage::IsUnderflow(bool is_root) {
    return header.child_cnt < (meta.m + 1) / 2 && !is_root;
}