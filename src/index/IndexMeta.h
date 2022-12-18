//
// Created by c7w on 2022/12/18.
//

#ifndef DBS_TUTORIAL_INDEXMETA_H
#define DBS_TUTORIAL_INDEXMETA_H

#include <defines.h>
#include <index/IndexField.h>
#include <index/IndexRecord.h>

class IndexMeta {
public:
    TreeOrder m;  // order of B+ tree,
    PageID page_num{1};  // number of pages in the index file
    PageID root_page{-1};  // -1 for empty tree
    IndexFieldType field_type{IndexFieldType::INVALID};  // Only support INT and INT2 (joint indexing)

    explicit IndexMeta(TreeOrder m, PageID page_num, PageID root_page, IndexFieldType field_type)
            : m{m}, page_num{page_num}, root_page{root_page}, field_type{field_type} {}

    IndexMeta() = default;

    RecordSize Size(bool is_leaf) const {
        // Construct a null object of field_type
        auto null_field{IndexField::MakeNull(field_type)};
        // Return its size
        if (is_leaf) {
            return null_field->Size();
        } else {
            return null_field->ShortSize();
        }
    }

    // Serialize the meta into `dst` and move `dst` forward
    void Write(uint8_t *&dst) const {
        write_var(dst, m);
        write_var(dst, page_num);
        write_var(dst, root_page);
        write_var(dst, field_type);
    }

    // Deserialize the meta from `src` and move `src` forward
    static std::shared_ptr<IndexMeta> FromSrc(const uint8_t *&src) {
        TreeOrder _m;
        PageID _page_num;
        PageID _root_page;
        IndexFieldType _field_type;
        read_var(src, _m);
        read_var(src, _page_num);
        read_var(src, _root_page);
        read_var(src, _field_type);
        return std::make_shared<IndexMeta>(_m, _page_num, _root_page, _field_type);
    }


};

#endif  // DBS_TUTORIAL_INDEXMETA_H
