#include "raw_file.hpp"


RawFile::RawFile(std::string filename, unsigned char options) {
    // First thing - check if file exists..
    file_exists = check_file_exists(filename);

    if(! file_exists && (options & ReadOnly)){
        // we can't read a 0 byte file..
        throw(RawFileOpenException());
    }

    else if (! file_exists && (options & WriteOnly)){
        raw_file = std::fstream(filename, std::ios::out | std::ios::app);
    }
    
    else if (file_exists && (options & WriteOnly)) {
        // open existing file
        if (options & AppendIfExistRaw) {
            // open and go to end of file
            raw_file = std::fstream(filename, std::ios::out | std::ios::app);
        }
        if (options & OverWriteIfExistRaw) {
            // open and go to end of file
            raw_file = std::fstream(filename, std::ios::out | std::ios::trunc);
        }
    }

    else if (file_exists && (options & ReadOnly)) {
        raw_file = std::fstream(filename, std::ios::in);
    }
}

RawFile::~RawFile() {
    if(raw_file.is_open())
        close();
}

inline bool RawFile::check_file_exists (const std::string& name) {
  struct stat buffer;   
  return (stat (name.c_str(), &buffer) == 0); 
}

void RawFile::write_message(std::string_view dump_line, uint64_t recv_ts) {
    raw_file << recv_ts << ":" << dump_line << "\n";
}

std::string_view RawFile::read_message() {
    if(raw_file.good()){
        raw_file.getline(line_buffer,1024*1024);
        line_str_view = std::string_view(line_buffer);

        std::size_t found = line_str_view.find(":");
        if (found!=std::string::npos){
            message_str_view = line_str_view.substr(found + 1, std::string::npos);
            ts_str_view = line_str_view.substr(0, found);
            return(message_str_view);
        }        
    }
    // we had issues parsing the file or the file had no more messages
    return std::string_view(std::string());
}

uint64_t RawFile::get_ts_of_message() {

    return(std::stoull(std::string(ts_str_view)));
}

bool RawFile::close() {
    raw_file.close();
    return(true);
}
