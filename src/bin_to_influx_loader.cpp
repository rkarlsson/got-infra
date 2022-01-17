#include <getopt.h>
#include <iostream>
#include "aeron_types.hpp"
#include "binary_file.hpp"
#include "sequence_binary_files.hpp"
#include "InfluxDB.h"
#include "InfluxDBFactory.h"
#include "Point.h"
#include "strat_calculator.hpp"
#include <vector>
#include <map>

void print_options(){
    std::cout << "Options for bin_to_influx_loader:" << std::endl;
    std::cout << "  -b (--binary_file) <binaryfilename>                     = Name of Binary File to read in" << std::endl;
    std::cout << "  [-t (--no-tob)]                                         = Will not publish ToB messages, just consume to calculate mid price" << std::endl;    
    std::cout << "  [-h (--help)]                                           = Prints this message" << std::endl;
}

constexpr std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>  nsToTimePoint(uint64_t ts)
{
    auto duration = std::chrono::nanoseconds{ts};

    return std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>{
        std::chrono::duration_cast<std::chrono::system_clock::duration>(duration)
    };
}

int main(int argc, char* argv[]) {
    int option;
    std::vector<std::string> binary_filenames;
    SequenceBinaryFiles *seq_bin_files;
    bool no_tob = false;

    auto strat_calc = new StratCalculator();

    static struct option long_options[] = {
        {"binary_file"  , optional_argument, NULL, 'b'},
        {"no_tob"  , optional_argument, NULL, 't'},        
        {"help"         , optional_argument, NULL,'h'}};

    while((option = getopt_long(argc, argv, "b:ht", long_options, NULL)) != -1) {
        switch (option) {
            case 'h':
                print_options();
                exit(0);

            case 't':
                no_tob = true;
                break;

            case 'b':
                binary_filenames.push_back(std::string(optarg));
                break;
        }
    }

    if(binary_filenames.size() == 0){
        print_options();
        exit(0);
    }

    seq_bin_files = new SequenceBinaryFiles();
    for(auto const& bin_filename: binary_filenames) {
        seq_bin_files->add_binary_file(bin_filename, 0);
        std::cout << "Added: " << bin_filename << std::endl;
    }

    auto symbols = seq_bin_files->get_symbol_ids();


    std::unique_ptr<influxdb::InfluxDB> influxDBtaq;
    influxDBtaq = influxdb::InfluxDBFactory::Get("http://127.0.0.1:8086/?db=taqdata");
    influxDBtaq->batchOf(10000);
    std::unique_ptr<influxdb::InfluxDB> influxDBio;
    influxDBio = influxdb::InfluxDBFactory::Get("http://127.0.0.1:8086/?db=simdata");
    influxDBio->batchOf(1000);
    char *next_bin_message;
    uint64_t new_current_time_in_seconds = 0;
    uint64_t old_current_time_in_seconds = 0;
    while(seq_bin_files->get_next_message(&next_bin_message)){
        switch(seq_bin_files->get_message_type()){
            case TOB_UPDATE: {
                struct ToBUpdate *t = (struct ToBUpdate*)next_bin_message;
                new_current_time_in_seconds = t->receive_timestamp;
                if(!no_tob){
                    influxDBtaq->write(influxdb::Point{"quote"}
                        .addField("b_prc", t->bid_price)
                        .addField("b_qty", t->bid_qty)
                        .addField("a_prc", t->ask_price)
                        .addField("a_qty", t->ask_qty)                            
                        .addField("rcv_time", static_cast<long long int>(t->receive_timestamp))
                        .addField("send_time", static_cast<long long int>(t->sending_timestamp))
                        .addField("tot_lat", static_cast<long long int>(t->sending_timestamp - t->receive_timestamp))
                        .addField("exc_time", static_cast<long long int>(t->exchange_timestamp))
                        .addField("rcv_lat", static_cast<long long int>(t->receive_timestamp-t->exchange_timestamp))
                        .addTag("exch_id", std::to_string(static_cast<uint8_t>(t->exchange_id)))
                        .addTag("instr_id", std::to_string(static_cast<uint32_t>(t->instrument_id)))
                        .setTimestamp(nsToTimePoint(t->receive_timestamp)));
                }
                strat_calc->update_price(t);
            }
            break;

            case TRADE: {
                struct Trade *t = (struct Trade*)next_bin_message;
                new_current_time_in_seconds = t->receive_timestamp;
                if(!no_tob){
                    influxDBtaq->write(influxdb::Point{"trade"}
                        .addField("prc", t->price)
                        .addField("qty", t->qty)
                        .addField("trad_value", static_cast<double>(t->price * t->qty))
                        .addField("rcv_time", static_cast<long long int>(t->receive_timestamp))
                        .addField("exc_time", static_cast<long long int>(t->exchange_timestamp))
                        .addField("rcv_lat", static_cast<long long int>(t->receive_timestamp-t->exchange_timestamp))
                        .addField("tot_lat", static_cast<long long int>(t->sending_timestamp - t->receive_timestamp))
                        .addTag("exch_id", std::to_string(static_cast<uint8_t>(t->exchange_id)))
                        .addTag("instr_id", std::to_string(static_cast<uint32_t>(t->instrument_id)))
                        .setTimestamp(nsToTimePoint(t->receive_timestamp)));
                }
            }
            break;

            case SIGNAL: {
                struct Signal *s = (struct Signal*)next_bin_message;
                new_current_time_in_seconds = s->receive_timestamp;
                influxDBio->write(influxdb::Point{"signal"}
                    .addField("value", static_cast<double>(s->value))
                    .addTag("type", std::to_string(static_cast<int>(s->type)))
                    .addTag("exch_id", std::to_string(static_cast<uint8_t>(s->exchange_id)))
                    .addTag("instr_id", std::to_string(static_cast<uint32_t>(s->instrument_id)))
                    .setTimestamp(nsToTimePoint(s->receive_timestamp)));
            }
            break;                

            case MSG_NEW_ORDER: {
                struct SendOrder *s = (struct SendOrder*)next_bin_message;
                new_current_time_in_seconds = s->send_timestamp;
                influxDBio->write(influxdb::Point{"order"}
                    .addField("prc", s->price)
                    .addField("qty", s->qty)
                    .addField("req_id", static_cast<int>(s->internal_order_id))
                    .addField("exc_id", static_cast<int>(s->exchange_id))
                    .addField("strat_id", static_cast<int>(s->strategy_id))
                    .addField("order_type", static_cast<int>(s->order_type))
                    .addField("is_buy", static_cast<bool>(s->is_buy))
                    .addTag("msg_type", "New_Order")
                    .addTag("instr_id", std::to_string(static_cast<uint32_t>(s->instrument_id)))
                    .addTag("buy", s->is_buy ? "True" : "False")
                    .setTimestamp(nsToTimePoint(s->send_timestamp)));
                strat_calc->add_order(s);
            }
            break;

            case MSG_CANCEL_ORDER: {
                struct CancelOrder *c = (struct CancelOrder*)next_bin_message;
                new_current_time_in_seconds = c->send_timestamp;
                influxDBio->write(influxdb::Point{"cancel"}
                    .addField("req_id", static_cast<int>(c->internal_order_id))
                    .addField("exc_id", static_cast<int>(c->exchange_id))
                    .addField("strat_id", static_cast<int>(c->strategy_id))
                    .addTag("msg_type", "Cancel_Order")
                    .addTag("instr_id", std::to_string(static_cast<uint32_t>(c->instrument_id)))
                    .setTimestamp(nsToTimePoint(c->send_timestamp)));
                strat_calc->add_cancel(c);
            }
            break;
        
            case MSG_REQUEST_ACK: {
                struct RequestAck *r = (struct RequestAck*)next_bin_message;
                new_current_time_in_seconds = r->send_timestamp;
                std::string temp_order_id = (char *)r->external_order_id;
                influxDBio->write(influxdb::Point{"order"}
                    .addField("req_id", static_cast<int>(r->internal_order_id))
                    .addField("ord_id", temp_order_id)
                    .addField("exc_id", static_cast<int>(r->exchange_id))
                    .addField("strat_id", static_cast<int>(r->strategy_id))
                    .addField("ack_type", static_cast<int>(r->ack_type))
                    .addField("rej_reason", static_cast<int>(r->reject_reason))
                    .addTag("msg_type", "Request_Ack")
                    .addTag("instr_id", std::to_string(static_cast<uint32_t>(r->instrument_id)))
                    .setTimestamp(nsToTimePoint(r->send_timestamp)));
            }
            break;

            case MSG_FILL: {
                struct Fill *f = (struct Fill*)next_bin_message;
                new_current_time_in_seconds = f->send_timestamp;
                std::string temp_order_id = (char *)f->external_order_id;
                influxDBio->write(influxdb::Point{"fill"}
                    .addField("prc", f->fill_price)
                    .addField("qty", f->fill_qty)
                    .addField("req_id", static_cast<int>(f->internal_order_id))
                    .addField("ord_id", temp_order_id)
                    .addField("exc_id", static_cast<int>(f->exchange_id))
                    .addField("strat_id", static_cast<int>(f->strategy_id))
                    .addTag("msg_type", "Order_Fill")
                    .addTag("instr_id", std::to_string(static_cast<uint32_t>(f->instrument_id)))
                    .setTimestamp(nsToTimePoint(f->send_timestamp)));
                strat_calc->add_fill(f);
            }
            break;

            case STRATEGY_INFO_UPDATE: {
                struct StratInfo *s = (struct StratInfo*)next_bin_message;
                std::string inf_name = (char *)s->info_name;
                if (s->union_data_type == DOUBLE_DATA_TYPE){
                    influxDBio->write(influxdb::Point{"stratinfo"}
                        .addField("value", s->value.double_value)
                        .addTag("infoname", inf_name)
                        .setTimestamp(nsToTimePoint(old_current_time_in_seconds)));
                } else if (s->union_data_type == UINT64_DATA_TYPE){
                    influxDBio->write(influxdb::Point{"stratinfo"}
                        .addField("value", static_cast<int>(s->value.uint64_value))
                        .addTag("infoname", inf_name)
                        .setTimestamp(nsToTimePoint(old_current_time_in_seconds)));
                }else if (s->union_data_type == ANYTHING_128_DATA_TYPE){
                    // influxDBio->write(influxdb::Point{"stratinfo"}
                    //     .addField("value", std::to_string(s->value.anything))
                    //     .addTag("infoname", inf_name)
                    //     .setTimestamp(nsToTimePoint(old_current_time_in_seconds)));
                }
            }
            break;

        }
        if((new_current_time_in_seconds - old_current_time_in_seconds) >= 1000000000){
            auto pnl = strat_calc->get_pnl();
            influxDBio->write(influxdb::Point{"PnL"}
                .addField("value", pnl)
                .addTag("PnL", "1")
                .setTimestamp(nsToTimePoint(new_current_time_in_seconds)));

            auto exec_fees = strat_calc->get_execution_fee();
            influxDBio->write(influxdb::Point{"Fees"}
                .addField("value", exec_fees)
                .addTag("ExecFee", "1")
                .setTimestamp(nsToTimePoint(new_current_time_in_seconds)));

            auto open_order_qty = strat_calc->get_open_qty();
            for (const auto& [key, value] : *open_order_qty) {
                influxDBio->write(influxdb::Point{"OpenOrder"}
                    .addField("value", value)
                    .addTag("instr_id", std::to_string(static_cast<uint32_t>(key)))                    
                    .setTimestamp(nsToTimePoint(new_current_time_in_seconds)));
            }

            auto positions = strat_calc->get_positions();
            for (const auto& [key, value] : *positions) {
                influxDBio->write(influxdb::Point{"Position"}
                    .addField("value", value)
                    .addTag("instr_id", std::to_string(static_cast<uint32_t>(key)))                    
                    .setTimestamp(nsToTimePoint(new_current_time_in_seconds)));
            }

            auto exec_qty = strat_calc->get_executed_qty();
            for (const auto& [key, value] : *exec_qty) {
                influxDBio->write(influxdb::Point{"ExecQty"}
                    .addField("value", value)
                    .addTag("instr_id", std::to_string(static_cast<uint32_t>(key)))                    
                    .setTimestamp(nsToTimePoint(new_current_time_in_seconds)));
            }

            old_current_time_in_seconds = new_current_time_in_seconds;
        }
    }
    return(0);
}