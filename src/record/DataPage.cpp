//
// Created by lambda on 22-11-26.
//

#include "DataPage.h"

#include <cstring>

#include <record/TableMeta.h>
#include <record/Record.h>

void DataPage::Contiguous() {
    uint8_t *data_ptr{page->data + sizeof(PageHeader)};
    auto slot_ptr{FooterSlot(0)};
    for (SlotID i{0}; i < header.slot_count; ++i) {
        if (*slot_ptr < 0) {
            continue;
        }
        std::memmove(data_ptr, page->data + *slot_ptr, slot_ptr[-1] - slot_ptr[0]);
    }
    page->SetDirty();
}

std::shared_ptr<Record> DataPage::Select(SlotID slot) const {
    auto offset{*FooterSlot(slot)};  // offset of a slot, from the very beginning of the page
    if (offset < 0) {  // invalid slot
        return nullptr;
    }
    const uint8_t *src{page->data + offset};
    return Record::FromSrc(src, meta);
}

void DataPage::Update(SlotID slot, std::shared_ptr<Record> record) {
    auto record_size{record->Size()};
    auto slot_ptr{FooterSlot(slot)};
    assert(slot_ptr[-1] - slot_ptr[0] - record_size >= 0);
    header.free_space += slot_ptr[-1] - slot_ptr[0] - record_size;
    uint8_t *dst{page->data + *slot_ptr};
    record->Write(dst);
    page->SetDirty();
}

void DataPage::Delete(SlotID slot) {
    *FooterSlot(slot) = -1;
    page->SetDirty();
}

SlotID DataPage::Insert(std::shared_ptr<Record> record) {
    auto record_size{record->Size()};
    auto slot_ptr{FooterSlot(header.slot_count)};
    if (PAGE_SIZE - *slot_ptr - sizeof(SlotID) * header.slot_count - sizeof(SlotID) * 2 /* free space offset and new offset*/ < record_size) {
        Contiguous();
    }
    assert(PAGE_SIZE - *slot_ptr - sizeof(SlotID) * header.slot_count - sizeof(SlotID) * 2 >= record_size);

    uint8_t *dst{page->data + *slot_ptr};
    record->Write(dst);

    // update slot offset
    slot_ptr[-1] = slot_ptr[0] + record_size;
    header.free_space -= record_size + sizeof(SlotID);
    assert(header.free_space >= 0);
    page->SetDirty();
    TraceLog << "Insert " << " @" << page->fd << " #" << page->id << " Slot " << header.slot_count << " : " << record->Repr();
    return header.slot_count++;
}
