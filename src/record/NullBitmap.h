//
// Created by lambda on 22-11-20.
//

#ifndef DBS_TUTORIAL_NULLBITMAP_H
#define DBS_TUTORIAL_NULLBITMAP_H

#include <cstdint>
#include <cassert>
#include <memory>

#include <defines.h>
#include <utils/Serialization.h>

class NullBitmap {
public:
    FieldID field_count{};
    uint8_t *data{};

    explicit NullBitmap(FieldID field_count) :
            field_count{field_count}, data{new uint8_t[(field_count + 7) / 8]} {
        memset(data, 0 ,(field_count + 7) / 8);
    }

    static std::shared_ptr<NullBitmap> FromSrc(const uint8_t *&src, FieldID field_count) {
        auto ret{std::make_shared<NullBitmap>(field_count)};
        read_var(src, ret->data, (ret->field_count + 7) / 8);
        return std::move(ret);
    }

    void Write(uint8_t *&dst) const {
        write_var(dst, data, (field_count + 7) / 8);
    }

    void Set(std::size_t idx) const {
        assert(idx < field_count);
        data[idx / 8] |= 1 << (idx % 8);
    }

    void Reset(std::size_t idx) const {
        assert(idx < field_count);
        data[idx / 8] &= ~(1 << (idx % 8));
    }

    [[nodiscard]] bool Get(std::size_t idx) const {
        assert(idx < field_count);
        return data[idx / 8] & (1 << (idx % 8));
    }

    ~NullBitmap() {
        delete[] data;
    }

private:
};


#endif //DBS_TUTORIAL_NULLBITMAP_H
