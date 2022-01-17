#pragma once

#include "base_signal.hpp"
#include <unordered_map>

struct SymbolStats {
    Statistics symbol_stats;
    // anything else we may need to calculate the above in the future
};

class StatisticsSignal : public BaseSignal {
    private:
        std::unordered_map<uint32_t, SymbolStats *> symbol_info;
        uint64_t    last_second;
        uint64_t    last_minute;

    public:
        StatisticsSignal(bool file_output, std::string output_filename, bool file_input, SequenceBinaryFiles *seq_bin_file);
        ~StatisticsSignal();
        void msg_manager(char *binary_msg);
        void publish_updates();
};