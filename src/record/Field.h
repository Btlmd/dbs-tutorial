//
// Created by lambda on 22-11-20.
//

#ifndef DBS_TUTORIAL_FIELD_H
#define DBS_TUTORIAL_FIELD_H

#include <cstdint>
#include <string>
#include <cstring>
#include <compare>

#include <defines.h>
#include <utils/Serialization.h>

enum class FieldType {
    INT = 0,
    FLOAT = 1,
    CHAR = 2,
    VARCHAR = 3,
};

class PrimaryKey {
public:
    char name[CONSTRAINT_NAME_LEN_MAX + 1];
    FieldID field_count;
    FieldID fields[MAX_FIELD_COUNT];
};

class ForeignKey {
public:
    char name[CONSTRAINT_NAME_LEN_MAX + 1];
    FieldID field_count;
    FieldID fields[MAX_FIELD_COUNT];
    TableID reference_table;
    FieldID reference_fields[MAX_FIELD_COUNT];
};

class Field {
public:
    FieldType type;
    bool is_null;

    Field(FieldType type) : type{type} {}

    Field() = default;

    /**
     * Return the size of the field in storage
     * @return
     */
    [[nodiscard]] virtual std::size_t Size() const = 0;

    /**
     * Dump the field into `dst` and move `dst` forward
     * Note that `NULL` is not dumped and is expected to be stored in null bitmap
     * @param dst
     */
    virtual void Write(uint8_t *&dst) const = 0;

    [[nodiscard]] virtual std::string ToString() const = 0;

    virtual std::partial_ordering operator<=>(const Field &rhs) const = 0;

    static Field *LoadField(FieldType type, const uint8_t *&src, RecordSize max_len = -1);
};

class Int : public Field {
public:
    int value;

    Int(int value) : Field{FieldType::INT}, value{value} {}

    static Int *FromSrc(const uint8_t *&src) {
        int value;
        read_var(src, value);
        return new Int{value};
    }

    std::partial_ordering operator<=>(const Field &rhs) const override {
        if (is_null || rhs.is_null) {
            return std::partial_ordering::unordered;
        }
        auto int_rhs{dynamic_cast<const Int &>(rhs)};
        return value <=> int_rhs.value;
    }

    [[nodiscard]] std::size_t Size() const override {
        return sizeof(value);
    }

    [[nodiscard]] std::string ToString() const override {
        return std::to_string(value);
    }

    void Write(uint8_t *&dst) const override {
        write_var(dst, value);
    }
};

class Float : public Field {
public:
    float value;

    Float(float value) : Field{FieldType::FLOAT}, value{value} {}

    static Float *FromSrc(const uint8_t *&src) {
        float value;
        read_var(src, value);
        return new Float{value};
    }

    std::partial_ordering operator<=>(const Field &rhs) const override {
        if (is_null || rhs.is_null) {
            return std::partial_ordering::unordered;
        }
        auto float_rhs{dynamic_cast<const Float &>(rhs)};
        return value <=> float_rhs.value;
    }

    [[nodiscard]] std::size_t Size() const override {
        return sizeof(value);
    }

    [[nodiscard]] std::string ToString() const override {
        return std::to_string(value);
    }

    void Write(uint8_t *&dst) const override {
        write_var(dst, value);
    }

private:
};

class Char : public Field {
public:
    char *data;  // terminated with null
    RecordSize max_len;

    Char(RecordSize max_len) : data{new char[max_len + 1]} {}

    ~Char() {
        delete[] data;
    }

    static Char *FromSrc(const uint8_t *&src, RecordSize max_len) {
        auto ret{new Char{max_len}};
        read_var(src, ret->data, max_len + 1);
        return ret;
    }

    std::partial_ordering operator<=>(const Field &rhs) const override {
        if (is_null || rhs.is_null) {
            return std::partial_ordering::unordered;
        }
        auto cmp{strcmp(data, dynamic_cast<const Char &>(rhs).data)};
        if (cmp > 0) {
            return std::partial_ordering::less;
        }
        if (cmp < 0) {
            return std::partial_ordering::greater;
        }
        return std::partial_ordering::equivalent;
    }

    [[nodiscard]] std::size_t Size() const override {
        return max_len + 1;
    }

    void Write(uint8_t *&dst) const override {
        write_var(dst, data, max_len + 1);
    }

    [[nodiscard]] std::string ToString() const override {
        return {data};
    }
};

class VarChar : public Field {
public:
    char *data;  // terminated with null
    RecordSize str_len;

    VarChar() = default;
    VarChar(const std::string &stream) {
        str_len = stream.size();
        data = new char[str_len + 1];
        stream.copy(data, str_len, 0);
    }

    ~VarChar() {
        delete[] data;
    }

    static VarChar *FromSrc(const uint8_t *&src) {
        auto ret{new VarChar};
        read_var(src, ret->str_len);
        ret->data = new char[ret->str_len + 1];
        read_var(src, ret->data, ret->str_len);
        ret->data[ret->str_len] = 0;
        return ret;
    }

    std::partial_ordering operator<=>(const Field &rhs) const override {
        if (is_null || rhs.is_null) {
            return std::partial_ordering::unordered;
        }
        auto cmp{strcmp(data, dynamic_cast<const VarChar &>(rhs).data)};
        if (cmp > 0) {
            return std::partial_ordering::less;
        }
        if (cmp < 0) {
            return std::partial_ordering::greater;
        }
        return std::partial_ordering::equivalent;
    }

    [[nodiscard]] std::size_t Size() const override {
        return str_len + sizeof(str_len);
    }

    void Write(uint8_t *&dst) const override {
        write_var(dst, str_len);
        write_var(dst, data, str_len);
    }

    [[nodiscard]] std::string ToString() const override {
        return {data};
    }
};

class FieldMeta {
public:
    FieldType type;
    std::string name;
    FieldID field_id;

    RecordSize max_size{-1};
    bool unique{false};
    bool not_null{false};
    bool has_default{false};
    Field *default_value{nullptr};

    [[nodiscard]] std::size_t Size() const {
        return sizeof(FieldMeta) - sizeof(std::string) + sizeof(std::size_t) + name.size();
    }

    static FieldMeta *FromSrc(const uint8_t *&src) {
        const uint8_t *start_point{src};  // be optimized ifndef DEBUG
        auto meta{new FieldMeta};
        read_var(src, meta->type);
        read_string(src, meta->name);
        read_var(src, meta->max_size);
        read_var(src, meta->unique);
        read_var(src, meta->not_null);
        read_var(src, meta->has_default);
        if (meta->has_default) {
            meta->default_value = Field::LoadField(meta->type, src, meta->max_size);
        }
        assert(src - start_point == meta->Size());
        return meta;
    }

    void Write(uint8_t *&dst) const {
        const uint8_t *start_point{dst};
        write_var(dst, type);
        write_string(dst, name);
        write_var(dst, max_size);
        write_var(dst, unique);
        write_var(dst, not_null);
        write_var(dst, has_default);
        if (has_default) {
            default_value->Write(dst);
        }
        assert(dst - start_point == Size());
    }
};

inline Field *Field::LoadField(FieldType type, const uint8_t *&src, RecordSize max_len) {
    switch (type) {
        case FieldType::INT:
            return Int::FromSrc(src);
        case FieldType::FLOAT:
            return Float::FromSrc(src);
        case FieldType::CHAR:
            assert(max_len > 0);
            return Char::FromSrc(src, max_len);
        case FieldType::VARCHAR:
            return VarChar::FromSrc(src);
    }
}

#endif //DBS_TUTORIAL_FIELD_H
