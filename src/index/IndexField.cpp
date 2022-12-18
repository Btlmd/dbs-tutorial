//
// Created by c7w on 2022/12/18.
//

#include "IndexField.h"

inline std::shared_ptr<IndexField> IndexField::LoadIndexField(IndexFieldType type, const uint8_t *&src) {
    switch (type) {
        case IndexFieldType::INT:
            return IndexINT::FromSrc(src);
        case IndexFieldType::INT2:
            return IndexINT2::FromSrc(src);
        default:
            assert(false);
    }
}

inline std::shared_ptr<IndexField> IndexField::LoadIndexFieldShort(IndexFieldType type, const uint8_t *&src) {
    switch (type) {
        case IndexFieldType::INT:
            return IndexINT::FromSrcShort(src);
        case IndexFieldType::INT2:
            return IndexINT2::FromSrcShort(src);
        default:
            assert(false);
    }
}

inline std::shared_ptr<IndexField> IndexField::MakeNull(IndexFieldType _type) {
    switch (_type) {
        case IndexFieldType::INT:
            return std::make_shared<IndexINT>(0, true);
        case IndexFieldType::INT2:
            return std::make_shared<IndexINT2>(0, true, 0, true);
        default:
            assert(false);
    }
}
