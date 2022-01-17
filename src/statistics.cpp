#include "statistics.hpp"

StatisticsSignal::StatisticsSignal(bool file_output, std::string output_filename, bool file_input, SequenceBinaryFiles *seq_bin_file)  :
    BaseSignal(file_output, output_filename, file_input, seq_bin_file) {
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    current_ts = ((t.tv_sec*1000000000L)+t.tv_nsec);
    last_second = current_ts - (current_ts % 1000000000L);
    last_minute = current_ts - (current_ts % 60000000000L);;
    // Here we initiate anything we need for the specific signal strategy
}

StatisticsSignal::~StatisticsSignal() {
    // mostly an empty shell so that we can call the base descturcot which closes the binaryfile if we had one written to
}

void StatisticsSignal::publish_updates() {
    
    
    last_second = tob_update->receive_timestamp - (tob_update->receive_timestamp % 1000000000)
}

void StatisticsSignal::msg_manager(char *binary_message) {
    struct ToBUpdate *tob_update;
    struct Trade *trade_update;

    switch(((struct MessageHeader *) binary_message)->msgType) {
        case TOB_UPDATE:
            tob_update = (struct ToBUpdate *) binary_message;
            if((tob_update->receive_timestamp - (tob_update->receive_timestamp % 1000000000) - last_second) != 0)
                publish_updates();

            if(symbol_info.count(tob_update->instrument_id)) {
                SymbolStats *sym = symbol_info[tob_update->instrument_id];
                sym->symbol_stats.quotes_last_second += 1;
                sym->symbol_stats.quotes_last_minute += 1;
            }
            
            break;

        case TRADE:
            trade_update = (struct Trade *) binary_message;
            std::cout << "Trade\n";
            break;

    }
}
