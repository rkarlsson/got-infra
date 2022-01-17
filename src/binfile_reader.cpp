#include <getopt.h>
#include <iostream>
#include <charconv>
#include "aeron_types.hpp"
#include "binary_file.hpp"

constexpr long long int nanos_to_seconds = 1000000000;

void convert_epoch_ts_to_text(uint64_t time_stamp, char *msg_buf, int msg_len) {
    struct tm  ts;
    time_t t = static_cast<time_t>(time_stamp/nanos_to_seconds);
    int nanos = time_stamp % nanos_to_seconds;
    ts = *localtime((time_t *) &t);
    char *end_ptr = msg_buf;
    strftime (end_ptr, msg_len, "%F %T", &ts);
    msg_buf[19] = '.';
    end_ptr += 20;
    sprintf(end_ptr, "%09d", nanos);
}


void print_options(){
    std::cout << "Options for binfile_reader:" << std::endl;
    std::cout << "  [-f (--file) <INFILENAME>]                              = Binary inputfile that we read from" << std::endl;
    std::cout << "  [-e (--exchange) <exchange-id>]                         = Only print updates for this exchange ID" << std::endl;
    std::cout << "  [-i (--instrument) <instrument-id>]                     = Only print updates for this instrument ID" << std::endl;
    std::cout << "  [-c (--convert-time)]                                   = Converts timestamps to human readable time" << std::endl;
    std::cout << "  [-n (--num-messages)]                                   = Counts and prints number of messages in file" << std::endl;
    std::cout << "  [-h (--help)]                                           = Prints this message" << std::endl;
}

int main(int argc, char* argv[]) {
    int option;
    std::string inputfilename = "";
    uint32_t instrument_id = 0;
    bool filter_instrument_id = false;
    uint8_t exchange_id = 0;
    bool filter_exchange_id = false;
    bool convert_time = false;
    bool num_messages = false;

    static struct option long_options[] = {
        {"file"         , optional_argument, NULL, 'f'},
        {"exchange"     , optional_argument, NULL, 'e'},
        {"instrument"   , optional_argument, NULL, 'i'},
        {"convert"      , optional_argument, NULL,'c'},
        {"num-messages" , optional_argument, NULL,'n'},
        {"help"         , optional_argument, NULL,'h'}};

    while((option = getopt_long(argc, argv, "f:hce:i:n", long_options, NULL)) != -1) {
        switch (option) {
            case 'h':
                print_options();
                exit(0);

            case 'c':
                convert_time = true;
            break;

            case 'n':
                num_messages = true;
            break;

            case 'f':
                inputfilename = optarg;
            break;

            case 'e':
                exchange_id = (uint8_t) strtol(optarg, NULL, 10);
                filter_exchange_id = true;
            break;

            case 'i':
                instrument_id = (uint32_t) strtol(optarg, NULL, 10);
                filter_instrument_id = true;
            break;

            default:
                // Do nothing - we don't accept anything else
            break;
        }
    }

    uint64_t number_of_messages = 0;
    if(inputfilename!=""){
        // lets open and read it
        auto bin_file = new BinaryFile(inputfilename, ReadOnlyBinary);
        char msg_ptr[1024*1024];
        char time_conv_buf[80];
        char time_conv_buf_recv[80];
        char time_conv_buf_exch[80];
        struct ToBUpdate *tob;
        struct Trade *trade;
        struct Signal *sig;
        struct PLUpdates *plupdate;
        struct PriceLevelDetails *pldetail;
        struct InstrumentClearBook *instrument_clear;
        struct SendOrder *s;
        struct CancelOrder *c;
        struct RequestAck *r;
        struct Fill *f;

        while(bin_file->read_message(msg_ptr)) {
            number_of_messages++;

            struct MessageHeader *msg = (struct MessageHeader *) msg_ptr;
            if(!num_messages){
            switch(msg->msgType){
                case TOB_UPDATE:
                    tob = (struct ToBUpdate*)msg_ptr;
                    if(filter_exchange_id){
                        if(tob->exchange_id != exchange_id)
                            break;
                    }
                    if(filter_instrument_id){
                        if(tob->instrument_id != instrument_id)
                            break;
                    }
                    if(convert_time){
                        convert_epoch_ts_to_text(tob->receive_timestamp, time_conv_buf_recv, 80);
                        convert_epoch_ts_to_text(tob->exchange_timestamp, time_conv_buf_exch, 80);
                        std::cout << "TobUpdate,RcvTime=" << time_conv_buf << ",ExcTime=" << time_conv_buf_exch;
                    } else {
                        std::cout << "TobUpdate,RcvTime=" << std::to_string(static_cast<uint64_t>(tob->receive_timestamp));
                        std::cout << ",ExcTime=" << std::to_string(static_cast<uint64_t>(tob->exchange_timestamp));
                    }
                    std::cout << ",BidPrice=" << std::to_string(static_cast<double>(tob->bid_price));
                    std::cout << ",BidQuantity=" << std::to_string(static_cast<double>(tob->bid_qty));
                    std::cout << ",AskPrice=" << std::to_string(static_cast<double>(tob->ask_price));
                    std::cout << ",AskQuantity=" << std::to_string(static_cast<double>(tob->ask_qty));
                    std::cout << ",InstrumentID=" << std::to_string(static_cast<uint32_t>(tob->instrument_id));
                    std::cout << ",ExchangeID=" << std::to_string(static_cast<uint8_t>(tob->exchange_id)) << std::endl;
                    break;

                case TRADE:
                    trade = (struct Trade*)msg_ptr;
                    if(filter_exchange_id){
                        if(trade->exchange_id != exchange_id)
                            break;
                    }
                    if(filter_instrument_id){
                        if(trade->instrument_id != instrument_id)
                            break;
                    }
                    if(convert_time){
                        convert_epoch_ts_to_text(trade->receive_timestamp, time_conv_buf_recv, 80);
                        convert_epoch_ts_to_text(trade->exchange_timestamp, time_conv_buf_exch, 80);
                        std::cout << "Trade,RcvTime=" << time_conv_buf << ",ExcTime=" << time_conv_buf_exch;
                    } else {
                        std::cout << "Trade,RcvTime=" << std::to_string(static_cast<uint64_t>(trade->receive_timestamp));
                        std::cout << ",ExcTime=" << std::to_string(static_cast<uint64_t>(trade->exchange_timestamp));
                    }
                    std::cout << ",Price=" << std::to_string(static_cast<double>(trade->price));
                    std::cout << ",Quantity=" << std::to_string(static_cast<double>(trade->qty));
                    std::cout << ",InstrumentID=" << std::to_string(static_cast<uint32_t>(trade->instrument_id));
                    std::cout << ",ExchangeID=" << std::to_string(static_cast<uint8_t>(trade->exchange_id));
                    std::cout << ",TradeIDFirst=" << trade->exchange_trade_id_first;
                    std::cout << ",TradeIDLast=" << trade->exchange_trade_id_last << std::endl;
                    break;

                case SIGNAL:
                    sig = (struct Signal*)msg_ptr;
                    if(filter_exchange_id){
                        if(sig->exchange_id != exchange_id)
                            break;
                    }
                    if(filter_instrument_id){
                        if(sig->instrument_id != instrument_id)
                            break;
                    }

                    if(convert_time){
                        convert_epoch_ts_to_text(sig->receive_timestamp, time_conv_buf_recv, 80);
                        convert_epoch_ts_to_text(sig->exchange_timestamp, time_conv_buf_exch, 80);
                        std::cout << "Signal,RcvTime=" << time_conv_buf << ",ExcTime=" << time_conv_buf_exch;
                    } else {
                        std::cout << "Signal,RcvTime=" << std::to_string(static_cast<uint64_t>(sig->receive_timestamp));
                        std::cout << ",ExcTime=" << std::to_string(static_cast<uint64_t>(sig->exchange_timestamp));
                    }
                    std::cout << ",Value=" << std::to_string(static_cast<double>(sig->value));
                    std::cout << ",Type=" << SignalTypeString(sig->type);
                    std::cout << ",Instrument ID=" << std::to_string(static_cast<uint32_t>(sig->instrument_id)) << std::endl;
                    break;

                case PL_UPDATE:
                    plupdate = (struct PLUpdates*)msg_ptr;
                    pldetail = (struct PriceLevelDetails*)(msg_ptr + sizeof(struct PLUpdates));
                    if(filter_exchange_id){
                        if(plupdate->exchange_id != exchange_id)
                            break;
                    }
                    if(filter_instrument_id){
                        if(plupdate->instrument_id != instrument_id)
                            break;
                    }

                    if(convert_time){
                        convert_epoch_ts_to_text(plupdate->receive_timestamp, time_conv_buf_recv, 80);
                        convert_epoch_ts_to_text(plupdate->exchange_timestamp, time_conv_buf_exch, 80);
                        std::cout << "PLUpdate,RcvTime=" << time_conv_buf << ",ExcTime=" << time_conv_buf_exch;
                    } else {
                        std::cout << "PLUpdate,RcvTime=" << std::to_string(static_cast<uint64_t>(plupdate->receive_timestamp));
                        std::cout << ",ExcTime=" << std::to_string(static_cast<uint64_t>(plupdate->exchange_timestamp));
                    }
                    std::cout << ",NumUpdates=" << std::to_string(static_cast<uint32_t>(plupdate->num_of_pl_updates));
                    for(uint32_t i=0; i < plupdate->num_of_pl_updates; i++){
                        std::cout << ",[PL=" << std::to_string(static_cast<double>(pldetail->price_level));
                        std::cout << ",QTY=" << std::to_string(static_cast<double>(pldetail->quantity));
                        std::cout << ",SIDE=" << std::to_string(static_cast<uint8_t>(pldetail->side));
                        std::cout << ",TYPE=";
                        if(pldetail->pl_action_type == UPDATE_PL_ACTION)
                            std::cout << "UPDATE"<< "]";
                        else
                            std::cout << "DELETE"<< "]";
                        pldetail++;
                    }
                    std::cout << std::endl;
                    break;

                case INSTRUMENT_CLEAR_BOOK:
                    instrument_clear = (struct InstrumentClearBook*)msg_ptr;
                    if(filter_exchange_id){
                        if(instrument_clear->exchange_id != exchange_id)
                            break;
                    }
                    if(filter_instrument_id){
                        if(instrument_clear->instrument_id != instrument_id)
                            break;
                    }

                    if(convert_time){
                        convert_epoch_ts_to_text(instrument_clear->sending_timestamp, time_conv_buf_recv, 80);
                        std::cout << "InstrumentClearBook,SendTime=" << time_conv_buf_recv;
                    } else {
                        std::cout << "InstrumentClearBook,SendTime=" << std::to_string(static_cast<uint64_t>(instrument_clear->sending_timestamp));
                    }
                    std::cout << ",Instrument_ID=" << std::to_string(static_cast<uint32_t>(instrument_clear->instrument_id));
                    std::cout << ",Exchange_ID=" << std::to_string(static_cast<uint8_t>(instrument_clear->exchange_id));
                    std::cout << std::endl;
                    break;

                case MSG_NEW_ORDER: 
                    s = (struct SendOrder*)msg_ptr;
                    if(filter_exchange_id){
                        if(s->exchange_id != exchange_id)
                            break;
                    }
                    if(filter_instrument_id){
                        if(s->instrument_id != instrument_id)
                            break;
                    }

                    std::cout << "New Order:" << std::endl;
                    std::cout << "  Price= " << std::to_string(static_cast<double>(s->price)) << std::endl;
                    std::cout << "  Quantity= " << std::to_string(static_cast<double>(s->qty)) << std::endl;
                    std::cout << "  Internal Order ID= " << std::to_string(static_cast<uint64_t>(s->internal_order_id)) << std::endl;
                    std::cout << "  Parent Order ID= " << std::to_string(static_cast<uint64_t>(s->parent_order_id)) << std::endl;
                    std::cout << "  Instrument ID= " << std::to_string(static_cast<uint32_t>(s->instrument_id)) << std::endl;
                    std::cout << "  Exchange ID= " << std::to_string(static_cast<uint8_t>(s->exchange_id)) << std::endl;
                    std::cout << "  Strategy ID= " << std::to_string(static_cast<uint8_t>(s->strategy_id)) << std::endl;
                    std::cout << "  Order Type= " << OrderTypeString(s->order_type) << std::endl;
                    std::cout << "  Is Buy= " << std::to_string(static_cast<bool>(s->is_buy)) << std::endl;
                    if(convert_time){
                        convert_epoch_ts_to_text(s->send_timestamp, time_conv_buf, 80);
                        std::cout << "  SendTime= " << time_conv_buf << std::endl;
                    } else {
                        std::cout << "  SendTime= " << std::to_string(static_cast<uint64_t>(s->send_timestamp)) << std::endl;
                    }
                    break;

                case MSG_CANCEL_ORDER:
                    c = (struct CancelOrder*)msg_ptr;
                    std::cout << "Cancel Order:" << std::endl;
                    std::cout << "  Internal Order ID= " << std::to_string(static_cast<uint64_t>(c->internal_order_id)) << std::endl;
                    std::cout << "  Instrument ID= " << std::to_string(static_cast<uint32_t>(c->instrument_id)) << std::endl;
                    std::cout << "  Exchange ID= " << std::to_string(static_cast<uint8_t>(c->exchange_id)) << std::endl;
                    std::cout << "  Strategy ID= " << std::to_string(static_cast<uint8_t>(c->strategy_id)) << std::endl;
                    if(convert_time){
                        convert_epoch_ts_to_text(c->send_timestamp, time_conv_buf, 80);
                        std::cout << "  SendTime= " << time_conv_buf << std::endl;
                    } else {
                        std::cout << "  SendTime= " << std::to_string(static_cast<uint64_t>(c->send_timestamp)) << std::endl;
                    }
                    break;

                case MSG_REQUEST_ACK:
                    r = (struct RequestAck*)msg_ptr;
                    std::cout << "Request Ack:" << std::endl;
                    std::cout << "  Internal Order ID= " << std::to_string(static_cast<uint64_t>(r->internal_order_id)) << std::endl;
                    std::cout << "  External Order ID= " << r->external_order_id << std::endl;
                    std::cout << "  Instrument ID= " << std::to_string(static_cast<uint32_t>(r->instrument_id)) << std::endl;
                    std::cout << "  Exchange ID= " << std::to_string(static_cast<uint8_t>(r->exchange_id)) << std::endl;
                    std::cout << "  Strategy ID= " << std::to_string(static_cast<uint8_t>(r->strategy_id)) << std::endl;
                    std::cout << "  Ack Type= " << AckTypeString(r->ack_type) << std::endl;
                    std::cout << "  Reject Reason= " << RejectReasonString(r->reject_reason) << std::endl;
                    std::cout << "  Reject Message= " << r->reject_message << std::endl;
                    if(convert_time){
                        convert_epoch_ts_to_text(r->send_timestamp, time_conv_buf, 80);
                        std::cout << "  SendTime= " << time_conv_buf << std::endl;
                    } else {
                        std::cout << "  SendTime= " << std::to_string(static_cast<uint64_t>(r->send_timestamp)) << std::endl;
                    }
                    break;

                case MSG_FILL:
                    f = (struct Fill*)msg_ptr;
                    std::cout << "Fill found:" << std::endl;
                    std::cout << "  Fill Price= " << std::to_string(static_cast<float>(f->fill_price)) << std::endl;
                    std::cout << "  Fill Quantity= " << std::to_string(static_cast<float>(f->fill_qty)) << std::endl;
                    std::cout << "  Leaves Quantity= " << std::to_string(static_cast<float>(f->leaves_qty)) << std::endl;
                    std::cout << "  Internal Order ID= " << std::to_string(static_cast<uint64_t>(f->internal_order_id)) << std::endl;
                    std::cout << "  External Order ID= " << f->external_order_id << std::endl;
                    std::cout << "  Instrument ID= " << std::to_string(static_cast<uint32_t>(f->instrument_id)) << std::endl;
                    std::cout << "  Exchange ID= " << std::to_string(static_cast<uint8_t>(f->exchange_id)) << std::endl;
                    std::cout << "  Strategy ID= " << std::to_string(static_cast<uint8_t>(f->strategy_id)) << std::endl;
                    if(convert_time){
                        convert_epoch_ts_to_text(f->send_timestamp, time_conv_buf, 80);
                        std::cout << "  SendTime= " << time_conv_buf << std::endl;
                    } else {
                        std::cout << "  SendTime= " << std::to_string(static_cast<uint64_t>(f->send_timestamp)) << std::endl;
                    }
                    break;

            }
            }
        }
        std::cout << "Number of messages read: " << std::to_string(number_of_messages) << std::endl;
    }

    return(0);
}