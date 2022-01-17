#pragma once

#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <unistd.h>
#include <exception>
#include <chrono>
#include <string_view>


enum RawFileOptions {
    ReadOnly            = 0x01,
    WriteOnly           = 0x02,
    AppendIfExistRaw    = 0x04,
    OverWriteIfExistRaw = 0x08
};

class RawFileOpenException: public std::exception
{
  virtual const char* what() const throw()
  {
    return "Failed to open file";
  }
};


class RawFile {
    private:
        bool file_exists = false;
        std::chrono::_V2::system_clock::time_point last_update_time;
        std::fstream raw_file;

        bool init_file();
        bool seek_to_beginning();
        bool seek_to_end();
        inline bool check_file_exists (const std::string& name);
        
        char line_buffer[1024*1024];
        std::string_view  line_str_view;
        std::string_view  message_str_view;
        std::string_view  ts_str_view;

    public:
        RawFile(std::string filename, unsigned char options);
        ~RawFile(); // all it does is closes the file appropriately - will also update headers etc in the future
        bool close();
        uint64_t get_ts_of_message();
        void write_message(std::string_view dump_line, uint64_t recv_ts);
        std::string_view read_message();
};

