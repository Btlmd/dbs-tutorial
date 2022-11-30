//
// Created by lambda on 22-11-19.
//

#ifndef DBS_TUTORIAL_UITLS_H
#define DBS_TUTORIAL_UITLS_H

#include <string>
#include <cstring>
#include <type_traits>
#include <limits>
#include <defines.h>


inline void write_var(uint8_t *&dst, const void *src, int len) {
    memcpy(dst, src, len);
    dst += len;
}

inline void read_var(uint8_t const *&src, void *dst, int len) {
    memcpy(dst, src, len);
    src += len;
}

template<typename T>
inline void write_var(uint8_t *&dst, const T &src) {
    static_assert(!std::is_pointer<T>::value);
    write_var(dst, &src, sizeof(T));
}

template<typename T>
inline void read_var(uint8_t const *&src, T &dst) {
    static_assert(!std::is_pointer<T>::value);
    read_var(src, &dst, sizeof(T));
}

inline void read_string(uint8_t const *&src, std::string &dst) {
    RecordSize len;
    read_var(src, len);
    dst.assign(reinterpret_cast<const char *>(src), len);
    src += len;
}

inline void write_string(uint8_t *&dst, const std::string& src) {
    assert(src.size() < std::numeric_limits<RecordSize>().max());
    write_var(dst, static_cast<RecordSize>(src.size()));
    src.copy(reinterpret_cast<char *>(dst), src.size(), 0);
    dst += src.size();
}

//inline void read_string(uint8_t const *&src, std::string &dst) {
//    // requires that `src` is null-terminated
//    dst.assign(reinterpret_cast<const char *>(src));
//    src += dst.size() + 1;
//}
//
//inline void write_string(uint8_t *&dst, const std::string &src) {
//    src.copy(reinterpret_cast<char *>(dst), src.size());
//    // return value of `std::string::copy` never copies ending null
//    dst += src.size();
//    *dst = 0;
//    dst += 1;
//}

#endif //DBS_TUTORIAL_UITLS_H
