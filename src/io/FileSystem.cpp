//
// Created by lambda on 22-11-17.
//

#include "FileSystem.h"

#include <filesystem>
#include <unistd.h>
#include <fcntl.h>
#include <fmt/format.h>

#include <exception/FilesystemException.h>

void FileSystem::ReadPage(FileID fd, PageID page_id, uint8_t *dst) {
    lseek(fd, PAGE_SIZE * page_id, SEEK_SET);
    ssize_t read_size = read(fd, dst, PAGE_SIZE);
    if (read_size != PAGE_SIZE) {
        throw FileSystemError{
                "Expecting {} bytes for page read, got {} instead.",
                PAGE_SIZE,
                read_size
        };
    }
    TraceLog << "FileSystem Read Page @" << fd << " #" << page_id;
}

void FileSystem::WritePage(FileID fd, PageID page_id, uint8_t *src) {
    lseek(fd, PAGE_SIZE * page_id, SEEK_SET);
    ssize_t write_size = write(fd, src, PAGE_SIZE);
    if (write_size != PAGE_SIZE) {
        if (write_size < 0) {
            const char *errmsg{strerror(errno)};
            throw FileSystemError{"Write Page @{} #{} with returns value {}; Error #{}: {}",
                                  fd, page_id, write_size, errno, errmsg};
        }
        throw FileSystemError{
                "Expecting {} bytes for page write, got {} instead.",
                PAGE_SIZE,
                write_size
        };
    }
    TraceLog << "Write Page @" << fd << " #" << page_id;
}

FileID FileSystem::OpenFile(const std::string &path) {
    if (!std::filesystem::exists(path)) {
        throw FileSystemError{"File {} not found", path};
    }
    FileID fd{open(path.c_str(), O_RDWR)};
    if (fd < 0) {
        const char *errmsg{strerror(errno)};
        throw FileSystemError{"Open file {} with non-zero returns value {}; Error #{}: {}",
                              path, fd, errno, errmsg};
    }
    TraceLog << "Open file " << path << " with FileID " << fd;

    return fd;
}

void FileSystem::CloseFile(FileID fd) {
    int close_status{close(fd)};
    if (close_status != 0) {
        const char *errmsg{strerror(errno)};
        throw FileSystemError{"Close FD {} with non-zero return value {}; Error #{}: {}",
                              fd, close_status, errno, errmsg};
    }
    TraceLog << "Close file @" << fd;
}

void FileSystem::MakeDirectory(const std::string &path, bool exist_ok) {
    if (exist_ok) {
        if (!std::filesystem::is_directory(path)) {
            if (std::filesystem::exists(path)) {
                throw FileSystemError{
                        "{} exists as non-directory",
                        path
                };
            } else {
                std::filesystem::create_directory(path);
                TraceLog << "Make directory (exist_ok=true) (created)" << path;
            }
        } else {
            TraceLog << "Make directory (exist_ok=true) (existed)" << path;
        }
    } else {
        if (std::filesystem::exists(path)) {
            throw FileSystemError{
                    "Fail to create directory {}; Directory exists",
                    path
            };
        }
        std::filesystem::create_directory(path);
        TraceLog << "Make directory (exist_ok=false) " << path;
    }
}

void FileSystem::RemoveDirectory(const std::string &path) {
    std::filesystem::remove_all(path);
    TraceLog << "Remove directory " << path;
}

FileID FileSystem::NewFile(const std::string &path) {
    if (std::filesystem::exists(path)) {
        throw FileSystemError{"Fail to create file {}; File exists", path};
    }
    FileID fd = open(path.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        const char *errmsg{strerror(errno)};
        throw FileSystemError{"Create file {} with non-zero returns value {}; Error #{}: {}",
                              path, fd, errno, errmsg};
    }
    TraceLog << "Create file " << path << " with FileID @" << fd;
    return fd;
}


