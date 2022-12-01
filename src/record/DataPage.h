//
// Created by lambda on 22-11-26.
//

#ifndef DBS_TUTORIAL_DATAPAGE_H
#define DBS_TUTORIAL_DATAPAGE_H

#include <defines.h>
#include <io/BufferSystem.h>
#include <record/Record.h>

class TableMeta;

/** DataPage Layout
 * | PageHeader |
 * | Slot 0 | Slot 1 | ... | Slot <slot_count - 1> |
 * | Free Space | ...
 * | Pointer to free space | Pointer to <Slot <slot_count - 1>> | ... | Pointer to <Slot 0> |
 */

class DataPage {
public:
    class PageHeader {
    public:
        // total slots in the page (including deleted slots)
        // can be used to scan the slot mapping in the page footer
        SlotID slot_count;

        /** size of total free space
         * can be used to determine whether to move the records
         * note that this is not the contiguous free space
         */
        RecordSize free_space;
    } &header;

    Page *page;
    const TableMeta &meta;

    explicit DataPage(Page *page, const TableMeta &table_meta) : header{*reinterpret_cast<PageHeader *>(page->data)},
                                                           page{page}, meta{table_meta} {}

    [[nodiscard]] std::shared_ptr<Record> GetRecord(SlotID slot) const;

    /**
     * Insert record into the end of page
     * May trigger DataPage::Contiguous if necessary
     * @param record
     */
    SlotID Insert(std::shared_ptr<Record> record);

    /**
     * Mark `slot` as invalid
     * @param slot
     */
    void Delete(SlotID slot);

    /**
     * Update record in `slot`
     * Only in-place update is supported, otherwise assertion fails
     * @param slot
     * @param record
     */
    void Update(SlotID slot, std::shared_ptr<Record> record);

    [[nodiscard]] bool Contains(RecordSize record_size) const {
        return header.free_space - sizeof(SlotID) >= record_size;
    }

    /**
     * Initialize the page structure
     * Used when the page is first created
     */
    void Init() {
        header.slot_count = 0;
        header.free_space = PAGE_SIZE - sizeof(PageHeader) - sizeof(SlotID);
        *FooterSlot(0) = sizeof(PageHeader);
    }

private:
    /**
     * Make the free space in the page contiguous
     * Note that invalid pointers in the footer will left unchanged
     */
    void Contiguous();

    [[nodiscard]] SlotID *FooterSlot(SlotID slot) const {
        uint8_t *slot_begin{page->data + PAGE_SIZE - sizeof(SlotID) - sizeof(SlotID) * slot};
        return reinterpret_cast<SlotID *>(slot_begin);
    }
};


#endif //DBS_TUTORIAL_DATAPAGE_H
