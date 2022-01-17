#pragma once
#include <iostream>
#include <sstream>
#include <fstream>
#include <chrono>
#include <string_view>

class FileWriter {
    private:
        std::ofstream writer_fh;

    public:
        FileWriter(std::string output_filename);
        ~FileWriter();
        void write_to_file(std::string_view dump_line, uint64_t recv_ts);
        void flush_file();
};