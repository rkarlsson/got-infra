#include <iostream>
#include <algorithm>
#include <cctype>
#include <string>
#include <sstream>
#include <list>
#include <unordered_map>
#include <getopt.h>
#include <sys/stat.h>
#include "raw_file.hpp"
#include "binary_file.hpp"
#include "aeron_types.hpp"
#include "merged_orderbook.hpp"
#include "binance_md_process.hpp"


void print_options(){
    std::cout << "Options for svc_md_binance:" << std::endl;
    std::cout << "  -i (--inputfile) <JSONFILE_TO_PROCESS>                  = Input JSON file to process" << std::endl;
    std::cout << "  -I (--ssinputfile) <SS JSONFILE_TO_PROCESS>             = Input snapshot JSON file to process" << std::endl;    
    std::cout << "  -o (--outputfile) <OUTPUT_BINARY_FILE>                  = Output file to write the binary messages to" << std::endl;
    std::cout << "  -e (--exchange-id) <EXCHANGE_ID>                        = The ID of the exchange for the symbol" << std::endl;
    std::cout << "  -s (--symbol-id) <INSTRUMENT_ID>                        = The ID of the symbol in uint32_t" << std::endl;
    std::cout << "  -P (--price-precision) <PRICE_PRECISION>                = The price precision of the symbol in uint8_t" << std::endl;
    std::cout << "  -Q (--quantity-precision) <QUANTITY_PRECISION>          = The quantity precision of the symbol in uint8_t" << std::endl;
    std::cout << "  -T (--tick-size) <TICK_SIZE>                            = The tick_size of the symbol in double" << std::endl;
    std::cout << "  -S (--step-size) <STEP_SIZE>                            = The step_size of the symbol in double" << std::endl;
    std::cout << "  -C (--contract-size) <CONTRACT_SIZE>                    = The contract_size of the symbol in double" << std::endl;
    std::cout << "  [-h (--help)]                                           = Prints this message" << std::endl;
}

bool file_exists(const std::string& name) {
  struct stat buffer;   
  return (stat (name.c_str(), &buffer) == 0); 
}

int main(int argc, char** argv) {
    bool raw_file_given = false;
    bool ss_raw_file_given = false;
    bool binary_file_given = false;
    std::string raw_filename = "";
    std::string ss_raw_filename = "";
    std::string binary_filename = "";
    BinaryFile *bin_file = nullptr;
    RawFile *raw_file = nullptr;
    RawFile *ss_raw_file = nullptr;

    bool written_snapshot = false;

    static struct option long_options[] = {
        {"input-update-file"    , required_argument, NULL, 'i'},
        {"input-ss-file"        , required_argument, NULL, 'I'},
        {"output-file"          , required_argument, NULL, 'o'},
        {"exchange-id"          , required_argument, NULL, 'e'},
        {"symbol-id"            , required_argument, NULL, 's'},
        {"price-precision"      , optional_argument, NULL, 'P'},
        {"quantity-precision"   , optional_argument, NULL, 'Q'},
        {"tick-size"            , optional_argument, NULL, 'T'},
        {"step-size"            , optional_argument, NULL, 'S'},
        {"contract-size"        , optional_argument, NULL, 'C'},
        {"help"                 , optional_argument, NULL, 'h'}};

    bool exchange_id_given = false;
    bool instrument_id_given = false;

    uint32_t instrument_id = 0;
    uint32_t exchange_id = 0;
    uint32_t price_precision = 0;
    uint32_t quantity_precision = 0;
    double tick_size = 0.0;
    double step_size = 0.0;
    double contract_size = 0.0;
    double maintenance_margin = 0.0;
    double required_margin = 0.0;

    
    std::stringstream ss;
    ss.clear();

    int cmd_option;
    while((cmd_option = getopt_long(argc, argv, "i:I:o:e:s:P:Q:T:S:C:h", long_options, NULL)) != -1) {
        switch (cmd_option) {
            case 'i':
                raw_file_given = true;
                raw_filename = optarg;
                break;

            case 'I':
                ss_raw_file_given = true;
                ss_raw_filename = optarg;
                break;

            case 'o':
                binary_file_given = true;
                binary_filename = optarg;
                break;

            case 'e':
                exchange_id_given = true;
                ss << optarg;
                ss >> exchange_id;
                ss.clear();
                break;

            case 's':
                instrument_id_given = true;
                ss << optarg;
                ss >> instrument_id;
                ss.clear();
                break;

            case 'P':
                ss << optarg;
                ss >> price_precision;
                ss.clear();
                break;

            case 'Q':
                ss << optarg;
                ss >> quantity_precision;
                ss.clear();
                break;

            case 'T':
                ss << optarg;
                ss >> tick_size;
                ss.clear();
                break;

            case 'S':
                ss << optarg;
                ss >> step_size;
                ss.clear();
                break;

            case 'C':
                ss << optarg;
                ss >> contract_size;
                ss.clear();
                break;

            case 'h':
                print_options();
                exit(1);

            default:
                break;
        }
    }

    // Create filtered timestamps based on filename
    tm tm1;
    std::string filename_only = raw_filename.substr(raw_filename.find_last_of("\\/") + 1);
	sscanf(filename_only.c_str(),"%4d-%2d-%2d",&tm1.tm_year,&tm1.tm_mon,&tm1.tm_mday);
    tm1.tm_year -= 1900; // year starts from 1900 in this struct
    tm1.tm_mon--; // month from 0 to 11..
    tm1.tm_hour = 0;
    tm1.tm_min = 0;
    tm1.tm_sec = 0;
    uint64_t filter_start_time = mktime(&tm1);
    tm1.tm_hour = 23;
    tm1.tm_min = 59;
    tm1.tm_sec = 59;
    uint64_t filter_end_time = mktime(&tm1);
    filter_start_time *= 1000000000;
    filter_end_time *= 1000000000;
    filter_end_time += 999999999;

    // Setup the clear message
    InstrumentClearBook instrument_clear_msg;
    instrument_clear_msg.msg_header     = {sizeof(InstrumentClearBook), INSTRUMENT_CLEAR_BOOK, 1};
    instrument_clear_msg.instrument_id = instrument_id;
    instrument_clear_msg.exchange_id = exchange_id;
    instrument_clear_msg.book_type_to_clear = PL_BOOK_TYPE;
    instrument_clear_msg.clear_reason = EXCHANGE_SNAP;

    if(!exchange_id_given || !instrument_id_given){
        std::cout << "Need to provide exchange and instrument ID, exiting" << std::endl;
        exit(1);
    }

    if(!raw_file_given || !binary_file_given){
        std::cout << "In and out files are required as argument, exiting" << std::endl;
        exit(1);
    }

    if(file_exists(raw_filename)){
        raw_file = new RawFile(raw_filename, ReadOnly);
    } else {
        std::cout << "Raw file does not exist - ending" << std::endl;
        exit(1);
    }

    if(ss_raw_file_given){
        if(file_exists(ss_raw_filename)){
            ss_raw_file = new RawFile(ss_raw_filename, ReadOnly);
        } else {
            std::cout << "SS raw file given does not exist - ending" << std::endl;
            exit(1);
        }
    }


    bin_file = new BinaryFile(binary_filename, WriteOnlyBinary);
    bin_file->add_file_symbol_definition(   instrument_id,
                                            exchange_id,
                                            price_precision,
                                            quantity_precision,
                                            tick_size,
                                            step_size,
                                            contract_size,
                                            maintenance_margin,
                                            required_margin);
    bin_file->write_fileheader();


    // Message initialisation
    DecodeResponse decode_response;
    DecodeResponse decode_snapshot_response;
    char bin_message_buffer[1024*1024];
    char bin_snapshot_buffer[1024*1024];

    MergedOrderbook         pl_book;
    std::string_view    message_to_print;
    double              ask_price = 0.0;
    double              bid_price = 0.0;
    uint64_t            previous_last_seq_no = 0;
    uint64_t last_receive_timestamp = 0;
    uint64_t last_exchange_timestamp = 0;
    uint64_t last_sending_timestamp = 0;

    
    
    auto binance_processor = new BinanceMDProcessor(bin_message_buffer, bin_snapshot_buffer, &decode_response, &decode_snapshot_response);
    decode_response.previous_end_seq_no = 0;
    std::map<double, PLInfo*> *pl_map = new std::map<double, PLInfo*>();

    for(;;) {
        message_to_print = raw_file->read_message();
        if (message_to_print.length() > 0){
            binance_processor->process_message( message_to_print, 
                                                raw_file->get_ts_of_message(), 
                                                instrument_id, 
                                                (uint8_t)exchange_id,
                                                bid_price,
                                                ask_price,
                                                false);
            int bin_message_offset = 0;
            char *msg_pointer;
            for(int i = 0; i < decode_response.num_messages; i++){
                msg_pointer = bin_message_buffer + bin_message_offset;
                // m_header = (MessageHeader *) (bin_message_buffer + bin_message_offset);
                switch(((MessageHeader *) msg_pointer)->msgType){
                    case TOB_UPDATE:
                        // Set last event timestamp processed
                        last_receive_timestamp = ((ToBUpdate *) msg_pointer)->receive_timestamp;
                        last_exchange_timestamp = ((ToBUpdate *) msg_pointer)->exchange_timestamp;
                        last_sending_timestamp = ((ToBUpdate *) msg_pointer)->sending_timestamp;

                        // update what last touch prices were to establish trade sides in decoder
                        ask_price = ((ToBUpdate *) msg_pointer)->ask_price;
                        bid_price = ((ToBUpdate *) msg_pointer)->bid_price;

                        ((ToBUpdate *) msg_pointer)->sending_timestamp = ((ToBUpdate *) msg_pointer)->receive_timestamp + 2500;

                        if( (((ToBUpdate *) msg_pointer)->exchange_timestamp >= filter_start_time) &&
                            (((ToBUpdate *) msg_pointer)->exchange_timestamp <= filter_end_time))
                            bin_file->write_message(msg_pointer, ((MessageHeader *) msg_pointer)->msgLength);
                        break;

                    case SIGNAL:
                        last_receive_timestamp = ((Signal *) msg_pointer)->receive_timestamp;
                        last_exchange_timestamp = ((Signal *) msg_pointer)->exchange_timestamp;

                        if( (((Signal *) msg_pointer)->exchange_timestamp >= filter_start_time) &&
                            (((Signal *) msg_pointer)->exchange_timestamp <= filter_end_time))                    
                            bin_file->write_message(msg_pointer, ((MessageHeader *) msg_pointer)->msgLength);
                        break;

                    case TRADE:
                        // Set last event timestamp processed
                        last_receive_timestamp = ((Trade *) msg_pointer)->receive_timestamp;
                        last_exchange_timestamp = ((Trade *) msg_pointer)->exchange_timestamp;
                        last_sending_timestamp = ((Trade *) msg_pointer)->sending_timestamp;

                        ((Trade *) msg_pointer)->sending_timestamp = ((Trade *) msg_pointer)->receive_timestamp + 2500;

                        if( (((Trade *) msg_pointer)->exchange_timestamp >= filter_start_time) &&
                            (((Trade *) msg_pointer)->exchange_timestamp <= filter_end_time))                    
                            bin_file->write_message(msg_pointer, ((MessageHeader *) msg_pointer)->msgLength);
                        break;

                    case PL_UPDATE:
                        // This makes sure we have a synthetic snapshot written to the binary file as we only print what belongs to the specific day
                        if(! written_snapshot){
                            if( (((PLUpdates *) msg_pointer)->exchange_timestamp >= filter_start_time) &&
                                (((PLUpdates *) msg_pointer)->exchange_timestamp <= filter_end_time)){
                                    // Get the snapshot from the built orderbook
                                    char synthetic_snap[1024*1024];
                                    int num_update_messages = pl_book.build_snapshot_from_current_book(synthetic_snap);

                                    // Write out the clear message to the binary file
                                    if(last_receive_timestamp != 0){
                                        instrument_clear_msg.sending_timestamp = last_exchange_timestamp + 1;
                                    } else {
                                        // Using timestamp from snapshot so that we don't have out of order timestamps
                                        instrument_clear_msg.sending_timestamp = ((PLUpdates *) synthetic_snap)->exchange_timestamp;
                                    }

                                    bin_file->write_message((char *)&instrument_clear_msg, sizeof(InstrumentClearBook));

                                    int bin_snapshot_message_offset = 0;
                                    char *snapshot_msg_pointer;
                                    // Loop over the multiple messages that the snapshot will return
                                    for(int i = 0; i < num_update_messages; i++){
                                        snapshot_msg_pointer = synthetic_snap + bin_snapshot_message_offset;
                                        if(last_receive_timestamp != 0){
                                            ((PLUpdates *) snapshot_msg_pointer)->exchange_timestamp = last_exchange_timestamp + 2;
                                            ((PLUpdates *) snapshot_msg_pointer)->receive_timestamp = last_receive_timestamp + 3;
                                            ((PLUpdates *) snapshot_msg_pointer)->sending_timestamp = last_receive_timestamp + 2500;
                                        }

                                        // Write out the snapshot to the binary file
                                        bin_file->write_message(snapshot_msg_pointer, ((MessageHeader *) snapshot_msg_pointer)->msgLength);

                                        bin_snapshot_message_offset += ((MessageHeader *) snapshot_msg_pointer)->msgLength;
                                    }
                                    // Done - no need to do it anymore
                                    written_snapshot = true;
                                }
                        }

                        sbe::PLUpdates *pl = new sbe::PLUpdates((char *)msg_pointer, 1024*1024);
                        // Check if seq number mismatch.. then start the process to figure out if we need to snapshot..
                        if(previous_last_seq_no != decode_response.previous_end_seq_no){
                            if(((PLUpdates *) msg_pointer)->end_seq_number < previous_last_seq_no){
                                // std::cout << ".";
                                break;
                            } else{
                                if((((PLUpdates *) msg_pointer)->start_seq_number > previous_last_seq_no) || (previous_last_seq_no == 0)){
                                    std::cout << "Found a gap: (previous end seq): " << std::to_string(previous_last_seq_no);
                                    std::cout << " - (current end seq): " << std::to_string(decode_response.previous_end_seq_no);
                                    std::cout << " (initiating snapshot)" << std::endl;
                                    if(ss_raw_file_given) // only do this if we have initated SSfile
                                    {
                                        auto ss_to_print = ss_raw_file->read_message();
                                        if (ss_to_print.length() > 0){
                                            binance_processor->process_message( ss_to_print, 
                                                    ss_raw_file->get_ts_of_message(), 
                                                    instrument_id, 
                                                    (uint8_t)exchange_id,
                                                    0.0,
                                                    0.0,
                                                    true);                                            

                                            pl_book.clear_orderbook();

                                            int bin_snapshot_message_offset = 0;
                                            char *snapshot_msg_pointer;
                                            for(int i = 0; i < decode_snapshot_response.num_messages; i++){
                                                snapshot_msg_pointer = bin_snapshot_buffer + bin_snapshot_message_offset;
                                                // This is  true most of the time - that we want to write the message..
                                                ((PLUpdates *) snapshot_msg_pointer)->sending_timestamp = ((PLUpdates *) snapshot_msg_pointer)->receive_timestamp + 2500;
                                                // Last but not least - update the sequence number
                                                previous_last_seq_no = ((PLUpdates *) snapshot_msg_pointer)->end_seq_number;                                            
                                                pl_book.process_update(pl);

                                                if( (((PLUpdates *) msg_pointer)->exchange_timestamp >= filter_start_time) &&
                                                    (((PLUpdates *) msg_pointer)->exchange_timestamp <= filter_end_time) &&
                                                    (! written_snapshot))
                                                {
                                                        written_snapshot = true; // We should never be here, but if we do..
                                                        bin_file->write_message((char *)&instrument_clear_msg, sizeof(InstrumentClearBook));
                                                        bin_file->write_message(snapshot_msg_pointer, ((MessageHeader *) snapshot_msg_pointer)->msgLength);
                                                }
                                                bin_snapshot_message_offset += ((MessageHeader *) snapshot_msg_pointer)->msgLength;
                                            }

                                            break;
                                        } else {
                                            std::cout << "Gap found and no snapshot available to read from" << std::endl;
                                        }
                                    }
                                }
                            }
                        }

                        // Update the orderbook with the updates - then check if bid/ask is crossed..
                        pl_book.process_update(pl);

                        // Only check if crossed book at the last update in the series from the decoder
                        if(i == (decode_response.num_messages - 1)){
                            if(pl_book.is_book_crossed()){
                                std::cout << "Found crossed books" << std::endl;

                                pl_map->clear();
                                pl_book.get_x_price_levels(BUY_SIDE, 2, pl_map);
                                std::map<double, PLInfo*>::iterator it;
                                std::cout << "BUY-> ";
                                for (it = pl_map->begin(); it != pl_map->end(); it++) {
                                    std::cout << std::to_string(it->first) << "(" << std::to_string(it->second->timestamp) << ")," ;
                                }
                                std::cout << " : ";
                                pl_map->clear();
                                pl_book.get_x_price_levels(SELL_SIDE, 2, pl_map);
                                
                                for (it = pl_map->begin(); it != pl_map->end(); it++) {
                                    std::cout << std::to_string(it->first) << "(" << std::to_string(it->second->timestamp) << "),";
                                }
                                std::cout << " <-SELL";
                                std::cout << std::endl;
                            }
                        }

                        // This is  true most of the time - that we want to write the message..
                        ((PLUpdates *) msg_pointer)->sending_timestamp = ((PLUpdates *) msg_pointer)->receive_timestamp + 2500;
                        if( (((PLUpdates *) msg_pointer)->exchange_timestamp >= filter_start_time) &&
                            (((PLUpdates *) msg_pointer)->exchange_timestamp <= filter_end_time)){
                            bin_file->write_message(msg_pointer, ((MessageHeader *) msg_pointer)->msgLength);
                        }
                        // Last but not least - update the sequence number
                        previous_last_seq_no = ((PLUpdates *) msg_pointer)->end_seq_number;
                        break;                    
                }
                // Increment offset in case there is more than one message in the buffer
                bin_message_offset += ((MessageHeader *) msg_pointer)->msgLength;
            }
        }
        else
            break;
       
    }
    bin_file->close();
    return(0);
}