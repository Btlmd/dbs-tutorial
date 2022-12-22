//
// Created by c7w on 22-12-22.
//

#ifndef DBS_TUTORIAL_FREESPACEMANAGER_H
#define DBS_TUTORIAL_FREESPACEMANAGER_H

#include "defines.h"
#include "utils/Serialization.h"

typedef uint8_t PageState;
constexpr int FREE_PAGES_PER_PAGE {(PAGE_SIZE - 1 - sizeof(PageState) - 4) / sizeof(PageID)};
constexpr int PAGE_STATES_PER_PAGE {(PAGE_SIZE - 1 - 4) / (sizeof(PageID) + sizeof(PageState))};

class FreeSpaceManager {

    enum PageType : uint8_t {
        FREE_PAGES,
        PAGE_STATES,
        DONE
    };

   public:

    FreeSpaceManager() {
        free_pages.clear();
        for (int i = 0; i <= std::numeric_limits<PageState>::max(); i++) {
            free_pages.push_back(std::list<PageID>());
            free_pages[i].clear();
        }
        page_states.clear();
    }

    // Returns PageID if found, otherwise returns -1.
    PageID SearchFreeSpace(RecordSize size) {
        PageState required_state = GetPageStateFromSize(size + 15);
        for (PageState i = required_state; i < std::numeric_limits<PageState>::max(); i++) {
            if (free_pages[i].empty()) {
                continue;
            }
            PageID page_id = free_pages[i].front();
            free_pages[i].pop_front();
            return page_id;
        }
        return -1;
    }

    void UpdateFreeSpace(PageID page_id, RecordSize new_freespace_size) {
        // Delete old record: try to find page_id in page_states
        auto it = page_states.find(page_id);
        if (it != page_states.end()) {
            PageState former_state = page_states[page_id];
            auto iter = std::find(free_pages[former_state].begin(), free_pages[former_state].end(), page_id);
            if (iter != free_pages[former_state].end()) {
                free_pages[former_state].erase(iter);
            }
            page_states.erase(it);
        }

        // Insert new record
        PageState new_state = GetPageStateFromSize(new_freespace_size);
        page_states[page_id] = new_state;
        free_pages[new_state].push_back(page_id);
    }

    bool FromSrc(const uint8_t *&src, bool init = false) {
        if (init) {
            free_pages.clear();
            for (int i = 0; i <= std::numeric_limits<PageState>::max(); i++) {
                free_pages.push_back(std::list<PageID>());
                free_pages[i].clear();
            }
            page_states.clear();
        }

        // Record former src
        PageType page_type;
        read_var(src, page_type);

        if (page_type == FREE_PAGES) {
            PageState curr_page_state;
            read_var(src, curr_page_state);
            int entry_cnt;
            read_var(src, entry_cnt);
            for (int i = 0; i < entry_cnt; i++) {
                PageID page_id;
                read_var(src, page_id);
                free_pages[curr_page_state].push_back(page_id);
            }
            return false;
        } else if (page_type == PAGE_STATES) {
            int entry_cnt;
            read_var(src, entry_cnt);

            for (int i = 0; i < entry_cnt; i++) {
                PageID page_id;
                PageState page_state;
                read_var(src, page_id);
                read_var(src, page_state);
                page_states[page_id] = page_state;
            }
            return false;
        } else if (page_type == DONE) {
            return true;
        }
    }

    bool Write(uint8_t *&dst, bool init = false) {
        if (init) {
            iter_free_pages_cnt = 0;
            iter_free_pages_res = free_pages[iter_free_pages_cnt].size();
            iter_free_pages = free_pages[iter_free_pages_cnt].begin();

            iter_page_states_res = page_states.size();
            iter_page_states = page_states.begin();
        }

        if (iter_free_pages_cnt != 256) {
            write_var(dst, FREE_PAGES);
            write_var(dst, (PageState) iter_free_pages_cnt);
            int entry_cnt;
            if (iter_free_pages_res >= FREE_PAGES_PER_PAGE) {
                entry_cnt = FREE_PAGES_PER_PAGE;
            } else {
                entry_cnt = iter_free_pages_res;
            }
            write_var(dst, entry_cnt);
            iter_free_pages_res -= entry_cnt;

            for (int i = 0; i < entry_cnt; i++) {
                write_var(dst, *iter_free_pages);
                iter_free_pages++;
            }

            if (iter_free_pages_res == 0) {
                iter_free_pages_cnt++;
                iter_free_pages_res = free_pages[iter_free_pages_cnt].size();
                iter_free_pages = free_pages[iter_free_pages_cnt].begin();
            }

            return false;

        } else if (iter_page_states_res != 0) {
            write_var(dst, PAGE_STATES);

            int entry_cnt;
            if (iter_page_states_res >= PAGE_STATES_PER_PAGE) {
                    entry_cnt = PAGE_STATES_PER_PAGE;
            } else {
                    entry_cnt = iter_page_states_res;
            }
            write_var(dst, entry_cnt);
            iter_page_states_res -= entry_cnt;

            for (int i = 0; i < entry_cnt; i++) {
                write_var(dst, iter_page_states->first);
                write_var(dst, iter_page_states->second);
                iter_page_states++;
            }

            return false;
        } else {
            write_var(dst, DONE);
            return true;
        }

    }

    int iter_free_pages_cnt; int iter_free_pages_res;
    std::list<PageID>::iterator iter_free_pages;

    int iter_page_states_res;
    std::unordered_map<PageID, PageState>::iterator iter_page_states;


   private:
    PageState GetPageStateFromSize (RecordSize size) {
        assert (size <= PAGE_SIZE);
        return size >> 4;
    }

    // For every PageState, we maintain a list of free pages.
    std::vector<std::list<PageID>> free_pages;

    // For every free page, we record its page size state.
    std::unordered_map<PageID, PageState> page_states;
};

#endif  // DBS_TUTORIAL_FREESPACEMANAGER_H
