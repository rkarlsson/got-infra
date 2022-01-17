#include "file_writer.hpp"

FileWriter::FileWriter(std::string output_filename) {
    writer_fh.open(output_filename, std::ios::out | std::ios::app); 
}

FileWriter::~FileWriter() {
    if (writer_fh.is_open()){
        flush_file();
        writer_fh.close();
    }
}

void FileWriter::write_to_file(std::string_view dump_line, uint64_t recv_ts) {
    if(dump_line.length() > 3){
        std::string new_string = std::to_string(recv_ts) + ":" + std::string(dump_line) + "\n";
        writer_fh << new_string;
        // writer_fh << recv_ts << ":" << dump_line << "\n";
    }
}

void FileWriter::flush_file() {
    writer_fh.flush();
}