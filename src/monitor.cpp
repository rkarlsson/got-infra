#include "monitor.hpp"

constexpr std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>  nsToTimePoint(uint64_t ts)
{
    auto duration = std::chrono::nanoseconds{ts};

    return std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>{
        std::chrono::duration_cast<std::chrono::system_clock::duration>(duration)
    };
}

fragment_handler_t Monitor::process_io_messages() {
    return
        [&](const AtomicBuffer &buffer, util::index_t offset, util::index_t length, const Header &header) {
            // std::string tmp = std::string(reinterpret_cast<const char *>(buffer.buffer()) + offset, static_cast<std::size_t>(length));
            struct MessageHeader *m = (MessageHeader*)(reinterpret_cast<const char *>(buffer.buffer()) + offset);
            
            switch(m->msgType) {
                case TOB_UPDATE: {
                    struct ToBUpdate *t = (struct ToBUpdate*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "ToBUpdate found:" << std::endl;
                        std::cout << "  RcvTime= " << std::to_string(static_cast<long long int>(t->receive_timestamp)) << std::endl;
                        std::cout << "  ExcTime= " << std::to_string(static_cast<long long int>(t->exchange_timestamp)) << std::endl;
                        std::cout << "  BidPrice= " << std::to_string(static_cast<double>(t->bid_price)) << std::endl;
                        std::cout << "  BidQuantity= " << std::to_string(static_cast<double>(t->bid_qty)) << std::endl;
                        std::cout << "  AskPrice= " << std::to_string(static_cast<double>(t->ask_price)) << std::endl;
                        std::cout << "  AskQuantity= " << std::to_string(static_cast<double>(t->ask_qty)) << std::endl;
                        std::cout << "  Instrument ID= " << std::to_string(static_cast<uint32_t>(t->instrument_id)) << std::endl;
                        std::cout << "  ExchangeID= " << std::to_string(static_cast<uint8_t>(t->exchange_id)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_md->write_message((char *)t, (uint32_t) t->msg_header.msgLength);
                    }

                    
                    if(output_type == INFLUX_OUTPUT_TYPE){
                        influxDBio->write(influxdb::Point{"quote"}
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
                        publish_qty_ratio(t);
                    }
                }
                break;

                case TRADE: {
                    struct Trade *t = (struct Trade*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Trade found:" << std::endl;
                        std::cout << "  RcvTime= " << std::to_string(static_cast<uint64_t>(t->receive_timestamp)) << std::endl;
                        std::cout << "  ExcTime= " << std::to_string(static_cast<uint64_t>(t->exchange_timestamp)) << std::endl;
                        std::cout << "  Price= " << std::to_string(static_cast<double>(t->price)) << std::endl;
                        std::cout << "  Quantity= " << std::to_string(static_cast<double>(t->qty)) << std::endl;
                        std::cout << "  Instrument ID= " << std::to_string(static_cast<uint32_t>(t->instrument_id)) << std::endl;
                        std::cout << "  ExchangeID= " << std::to_string(static_cast<uint8_t>(t->exchange_id)) << std::endl;
                        std::cout << "  TradeIDFirst= " << t->exchange_trade_id_first << std::endl;
                        std::cout << "  TradeIDLast= " << t->exchange_trade_id_last << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_md->write_message((char *)t, (uint32_t) t->msg_header.msgLength);
                    }

                    if(output_type == INFLUX_OUTPUT_TYPE){
                        influxDBio->write(influxdb::Point{"trade"}
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
                    struct Signal *s = (struct Signal*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Signal found:" << std::endl;
                        std::cout << "  RcvTime= " << std::to_string(static_cast<uint64_t>(s->receive_timestamp)) << std::endl;
                        std::cout << "  ExcTime= " << std::to_string(static_cast<uint64_t>(s->exchange_timestamp)) << std::endl;
                        std::cout << "  SeqNr= " << std::to_string(static_cast<uint64_t>(s->sequence_nr)) << std::endl;
                        std::cout << "  Value= " << std::to_string(static_cast<double>(s->value)) << std::endl;
                        std::cout << "  Type= " << SignalTypeString(s->type) << std::endl;
                        std::cout << "  Instrument ID= " << std::to_string(static_cast<uint32_t>(s->instrument_id)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_md->write_message((char *)s, (uint32_t) s->msg_header.msgLength);
                    }

                    if(output_type == INFLUX_OUTPUT_TYPE){
                        influxDBio->write(influxdb::Point{"signal"}
                        .addField("value", static_cast<double>(s->value))
                        .addTag("type", std::to_string(static_cast<int>(s->type)))
                        .addTag("exch_id", std::to_string(static_cast<uint8_t>(s->exchange_id)))
                        .addTag("instr_id", std::to_string(static_cast<uint32_t>(s->instrument_id))));
                    }
                }
                break;                

                case  MSG_PARENT_ORDER: {
                    struct SendParentOrder *s = (struct SendParentOrder*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "New Parent Order found:" << std::endl;
                        std::cout << "  Value= " << std::to_string(static_cast<double>(s->value_USD)) << std::endl;
                        std::cout << "  Parent Order ID= " << std::to_string(static_cast<uint64_t>(s->parent_order_id)) << std::endl;
                        std::cout << "  JSON Config= " << s->json_config << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_oe->write_message((char *)s, (uint32_t) s->msg_header.msgLength);
                    }
                }
                break;

                case MSG_NEW_ORDER: {
                    struct SendOrder *s = (struct SendOrder*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "New Order found:" << std::endl;
                        std::cout << "  Price= " << std::to_string(static_cast<double>(s->price)) << std::endl;
                        std::cout << "  Quantity= " << std::to_string(static_cast<double>(s->qty)) << std::endl;
                        std::cout << "  Internal Order ID= " << std::to_string(static_cast<uint64_t>(s->internal_order_id)) << std::endl;
                        std::cout << "  Parent Order ID= " << std::to_string(static_cast<uint64_t>(s->parent_order_id)) << std::endl;
                        std::cout << "  Instrument ID= " << std::to_string(static_cast<uint32_t>(s->instrument_id)) << std::endl;
                        std::cout << "  Exchange ID= " << std::to_string(static_cast<uint8_t>(s->exchange_id)) << std::endl;
                        std::cout << "  Strategy ID= " << std::to_string(static_cast<uint8_t>(s->strategy_id)) << std::endl;
                        std::cout << "  Order Type= " << OrderTypeString(s->order_type) << std::endl;
                        std::cout << "  Is Buy= " << std::to_string(static_cast<bool>(s->is_buy)) << std::endl;
                    } 

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_oe->write_message((char *)s, (uint32_t) s->msg_header.msgLength);
                    }

                    if(output_type == INFLUX_OUTPUT_TYPE){
                        influxDBio->write(influxdb::Point{"order"}
                        .addField("prc", s->price)
                        .addField("qty", s->qty)
                        .addField("req_id", static_cast<int>(s->internal_order_id))
                        .addField("exc_id", static_cast<int>(s->exchange_id))
                        .addField("strat_id", static_cast<int>(s->strategy_id))
                        .addField("order_type", static_cast<int>(s->order_type))
                        .addField("is_buy", static_cast<bool>(s->is_buy))
                        .addTag("msg_type", "New_Order")
                        .addTag("instr_id", std::to_string(static_cast<uint32_t>(s->instrument_id))));
                    }
                }
                break;

                case MSG_CANCEL_ORDER: {
                    struct CancelOrder *c = (struct CancelOrder*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Cancel Order found:" << std::endl;
                        std::cout << "  Internal Order ID= " << std::to_string(static_cast<uint64_t>(c->internal_order_id)) << std::endl;
                        std::cout << "  Instrument ID= " << std::to_string(static_cast<uint32_t>(c->instrument_id)) << std::endl;
                        std::cout << "  Exchange ID= " << std::to_string(static_cast<uint8_t>(c->exchange_id)) << std::endl;
                        std::cout << "  Strategy ID= " << std::to_string(static_cast<uint8_t>(c->strategy_id)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_oe->write_message((char *)c, (uint32_t) c->msg_header.msgLength);
                    }

                    if(output_type == INFLUX_OUTPUT_TYPE){
                        influxDBio->write(influxdb::Point{"order"}
                        .addField("req_id", static_cast<int>(c->internal_order_id))
                        .addField("exc_id", static_cast<int>(c->exchange_id))
                        .addField("strat_id", static_cast<int>(c->strategy_id))
                        .addTag("msg_type", "Cancel_Order")
                        .addTag("instr_id", std::to_string(static_cast<uint32_t>(c->instrument_id))));                        
                    }
                }
                break;
            
                case MSG_REQUEST_ACK: {
                    struct RequestAck *r = (struct RequestAck*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Request Ack found:" << std::endl;
                        std::cout << "  Internal Order ID= " << std::to_string(static_cast<uint64_t>(r->internal_order_id)) << std::endl;
                        std::cout << "  External Order ID= " << r->external_order_id << std::endl;
                        std::cout << "  Instrument ID= " << std::to_string(static_cast<uint32_t>(r->instrument_id)) << std::endl;
                        std::cout << "  Exchange ID= " << std::to_string(static_cast<uint8_t>(r->exchange_id)) << std::endl;
                        std::cout << "  Strategy ID= " << std::to_string(static_cast<uint8_t>(r->strategy_id)) << std::endl;
                        std::cout << "  Ack Type= " << AckTypeString(r->ack_type) << std::endl;
                        std::cout << "  Reject Reason= " << RejectReasonString(r->reject_reason) << std::endl;
                        std::cout << "  Reject Message= " << r->reject_message << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_oe->write_message((char *)r, (uint32_t) r->msg_header.msgLength);
                    }

                    if(output_type == INFLUX_OUTPUT_TYPE){
                        std::string temp_order_id = (char *)r->external_order_id;
                        influxDBio->write(influxdb::Point{"order"}
                        .addField("req_id", static_cast<int>(r->internal_order_id))
                        .addField("ord_id", temp_order_id)
                        .addField("exc_id", static_cast<int>(r->exchange_id))
                        .addField("strat_id", static_cast<int>(r->strategy_id))
                        .addField("ack_type", static_cast<int>(r->ack_type))
                        .addField("rej_reason", static_cast<int>(r->reject_reason))
                        .addTag("msg_type", "Request_Ack")
                        .addTag("instr_id", std::to_string(static_cast<uint32_t>(r->instrument_id))));
                    }
                }
                break;

                case MSG_FILL: {
                    struct Fill *f = (struct Fill*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Fill found:" << std::endl;
                        std::cout << "  Fill Price= " << std::to_string(static_cast<float>(f->fill_price)) << std::endl;
                        std::cout << "  Fill Quantity= " << std::to_string(static_cast<float>(f->fill_qty)) << std::endl;
                        std::cout << "  Leaves Quantity= " << std::to_string(static_cast<float>(f->leaves_qty)) << std::endl;
                        std::cout << "  Internal Order ID= " << std::to_string(static_cast<uint64_t>(f->internal_order_id)) << std::endl;
                        std::cout << "  External Order ID= " << f->external_order_id << std::endl;
                        std::cout << "  Instrument ID= " << std::to_string(static_cast<uint32_t>(f->instrument_id)) << std::endl;
                        std::cout << "  Exchange ID= " << std::to_string(static_cast<uint8_t>(f->exchange_id)) << std::endl;
                        std::cout << "  Strategy ID= " << std::to_string(static_cast<uint8_t>(f->strategy_id)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_oe->write_message((char *)f, (uint32_t) f->msg_header.msgLength);
                    }

                    if(output_type == INFLUX_OUTPUT_TYPE){
                        std::string temp_order_id = (char *)f->external_order_id;
                        influxDBio->write(influxdb::Point{"order"}
                        .addField("prc", f->fill_price)
                        .addField("qty", f->fill_qty)
                        .addField("req_id", static_cast<int>(f->internal_order_id))
                        .addField("ord_id", temp_order_id)
                        .addField("exc_id", static_cast<int>(f->exchange_id))
                        .addField("strat_id", static_cast<int>(f->strategy_id))
                        .addTag("msg_type", "Order_Fill")
                        .addTag("instr_id", std::to_string(static_cast<uint32_t>(f->instrument_id))));
                    }
                }
                break;

                case INSTRUMENT_INFO_REQUEST: {
                    struct InstrumentInfoRequest *i = (struct InstrumentInfoRequest*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Instrument Info Request found:" << std::endl;
                        std::cout << "  Exchange ID= " << static_cast<int>(i->exchange_id) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)i, (uint32_t) i->msg_header.msgLength);
                    }

                }
                break;

                case INSTRUMENT_INFO_RESPONSE: {
                    struct InstrumentInfoResponse *i = (struct InstrumentInfoResponse*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Instrument Info Response found:" << std::endl;
                        std::cout << "  Exchange Name= " << i->exchange_name << std::endl;
                        std::cout << "  Instrument Name= " << i->instrument_name << std::endl;
                        std::cout << "  Exchange ID= " << std::to_string(static_cast<int>(i->exchange_id)) << std::endl;
                        std::cout << "  Instrument ID= " << std::to_string(static_cast<int>(i->instrument_id)) << std::endl;
                        std::cout << "  Instrument Count= " << std::to_string(static_cast<int>(i->instrument_count)) << std::endl;
                        std::cout << "  Number of Instruments= " << std::to_string(static_cast<int>(i->number_of_instruments)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)i, (uint32_t) i->msg_header.msgLength);
                    }
                }
                break;

                case EXCHANGE_RISK_INFO_REQUEST: {
                    struct ExchangeRiskInfoRequest *e = (struct ExchangeRiskInfoRequest*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Exchange Risk Info Request found:" << std::endl;
                        std::cout << "  Exchange ID= " << std::to_string(static_cast<int>(e->exchange_id)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)e, (uint32_t) e->msg_header.msgLength);
                    }
                }
                break;

                case EXCHANGE_RISK_INFO_RESPONSE: {
                    struct ExchangeRiskInfoResponse *e = (struct ExchangeRiskInfoResponse*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Exchange Risk Info Response found:" << std::endl;
                        std::cout << "  Exchange ID= " << std::to_string(static_cast<uint8_t>(e->exchange_id)) << std::endl;
                        std::cout << "  Max Order Value= " << std::to_string(static_cast<double>(e->max_order_value)) << std::endl;
                        std::cout << "  Max Cumulative Open Order Value= " << std::to_string(static_cast<double>(e->max_cumulative_open_order_value)) << std::endl;
                        std::cout << "  Max Cumulative Daily Traded Value= " << std::to_string(static_cast<double>(e->max_cumulative_daily_traded_value)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)e, (uint32_t) e->msg_header.msgLength);
                    }
                }
                break;

                case STRATEGY_INFO_REQUEST: {
                    struct StrategyInfoRequest *s = (struct StrategyInfoRequest*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Strategy Info Request found:" << std::endl;
                        std::cout << "  Exchange ID= " << std::to_string(static_cast<int>(s->exchange_id)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)s, (uint32_t) s->msg_header.msgLength);
                    }
                }
                break;

                case STRATEGY_INFO_RESPONSE: {
                    struct StrategyInfoResponse *s = (struct StrategyInfoResponse*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Strategy Info Response found:" << std::endl;
                        std::cout << "  Strategy ID= " << std::to_string(static_cast<uint8_t>(s->strategy_id)) << std::endl;
                        std::cout << "  Max Order Value= " << std::to_string(static_cast<double>(s->max_order_value_USD)) << std::endl;
                        std::cout << "  Max Trade Consideration= " << std::to_string(static_cast<double>(s->max_tradeconsideration_USD)) << std::endl;
                        std::cout << "  Num Of Strategies= " << std::to_string(static_cast<uint16_t>(s->number_of_strategies)) << std::endl;
                        std::cout << "  Strategy Count= " << std::to_string(static_cast<uint16_t>(s->strategy_count)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)s, (uint32_t) s->msg_header.msgLength);
                    }
                }
                break;

                case EXCHANGE_ID_REQUEST: {
                    struct ExchangeIDRequest *e = (struct ExchangeIDRequest*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Exchange ID Request found:" << std::endl;
                        std::cout << "  Exchange Name= " << static_cast<std::string>(e->exchange_name) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)e, (uint32_t) e->msg_header.msgLength);
                    }
                }
                break;

                case EXCHANGE_ID_RESPONSE: {
                    struct ExchangeIDResponse *e = (struct ExchangeIDResponse*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Exchange ID Response found:" << std::endl;
                        std::cout << "  Exchange Name= " << static_cast<std::string>(e->exchange_name) << std::endl;                            
                        std::cout << "  Exchange ID= " << std::to_string(static_cast<uint8_t>(e->exchange_id)) << std::endl;
                        std::cout << "  Response Type= " << ExchangeIDResponseTypeString(e->exchange_id_response_type) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)e, (uint32_t) e->msg_header.msgLength);
                    }
                }
                break;

                case CAPTURE_TIMEOUT: {
                    struct CaptureTimeout *c = (struct CaptureTimeout*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Capture Timeout found:" << std::endl;
                        std::cout << "  Exchange ID= " << static_cast<int>(c->exchange_id) << std::endl;
                        std::cout << "  Instrument ID= " << static_cast<int>(c->instrument_id) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)c, (uint32_t) c->msg_header.msgLength);
                    }
                }
                break;

                case CAPTURE_DISCONNECT: {
                    struct CaptureDisconnect *c = (struct CaptureDisconnect*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "Capture Disconnect found:" << std::endl;
                        std::cout << "  Exchange ID= " << static_cast<int>(c->exchange_id) << std::endl;
                        std::cout << "  Instrument ID= " << static_cast<int>(c->instrument_id) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)c, (uint32_t) c->msg_header.msgLength);
                    }
                }
                break;

                case HEARTBEAT_RESPONSE: {
                    struct HeartbeatResponse *h = (struct HeartbeatResponse*)m;
                    if(output_type == INFLUX_OUTPUT_TYPE){
                        influxDBio->write(influxdb::Point{"state"}
                            .addField("status", (int) (h->exchange_id > 0 ? h->exchange_id : 1))
                            .addTag("exch_id", std::to_string(static_cast<uint8_t>(h->exchange_id)))
                            .addTag("svc_type", std::to_string(static_cast<uint8_t>(h->service_type)))
                            .addTag("strat_id", std::to_string(static_cast<uint8_t>(h->strategy_id))));
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)h, (uint32_t) h->msg_header.msgLength);
                    }
                }
                break;

                // case ROLL_OVER_REQUEST: {
                //     struct RolloverRequest *r = (struct RolloverRequest*)m;
                //     if(output_type == STDOUT_OUTPUT_TYPE){
                //         std::cout << "Rollover Request found:" << std::endl;
                //         std::cout << "  Request_Timestamp= " << std::to_string(static_cast<long long int>(r->request_time_stamp)) << std::endl;
                //     }

                //     if(output_type == FILE_OUTPUT_TYPE){
                //         bin_file_risk->write_message((char *)r, (uint32_t) r->msg_header.msgLength);
                //     }
                // }
                // break;

                // case ROLL_OVER_RESPONSE: {
                //     struct RolloverResponse *r = (struct RolloverResponse*)m;
                //     if(output_type == STDOUT_OUTPUT_TYPE){
                //         std::cout << "Rollover Response found:" << std::endl;
                //         std::cout << "  Response_Timestamp= " << std::to_string(static_cast<long long int>(r->response_time_stamp)) << std::endl;
                //     }

                //     if(output_type == FILE_OUTPUT_TYPE){
                //         bin_file_risk->write_message((char *)r, (uint32_t) r->msg_header.msgLength);
                //     }
                // }
                // break;

                case ACCOUNT_INFO_UPDATE: {
                    struct AccountInfoResponse *a = (struct AccountInfoResponse *)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "AccountInfoResponse found:" << std::endl;
                        std::cout << " Exchange ID   = " << std::to_string(static_cast<uint8_t>(a->exchange_id)) << std::endl;
                        std::cout << " Instrument ID = " << std::to_string(static_cast<uint32_t>(a->instrument_id)) << std::endl; 
                        std::cout << " Asset ID      = " << std::to_string(static_cast<uint32_t>(a->asset_id)) << std::endl;
                        std::cout << " Value         = " << std::to_string(static_cast<double>(a->value)) << std::endl;
                        std::cout << " Type          = " << AccountInfoTypeString(a->account_info_type) << std::endl;
                        std::cout << " Reason        = " << AccountUpdateReasonString(a->account_update_reason) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)a, (uint32_t) a->msg_header.msgLength);
                    }
                }
                break;

                case ACCOUNT_INFO_REQUEST: {
                    struct AccountInfoRequest *a = (struct AccountInfoRequest*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "AccountInfoRequest found:" << std::endl;
                        std::cout << " Exchange ID   = " << std::to_string(static_cast<uint8_t>(a->exchange_id)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)a, (uint32_t) a->msg_header.msgLength);
                    }
                }
                break;

                case MARGIN_INFO_REQUEST: {
                    struct MarginInfoRequest *m = (struct MarginInfoRequest*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "MarginInfoRequest found:" << std::endl;
                        std::cout << " Collateral Asset ID  = " << std::to_string(static_cast<uint32_t>(m->collateral_asset_id)) << std::endl;
                        std::cout << " Exchange ID  = " << std::to_string(static_cast<uint8_t>(m->exchange_id)) << std::endl;
                        std::cout << " Info Type  = " << std::to_string(static_cast<uint8_t>(m->info_type_reason)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)m, (uint32_t) m->msg_header.msgLength);
                    }
                }
                break;

                case MARGIN_INFO_RESPONSE: {
                    struct MarginInfoResponse *m = (struct MarginInfoResponse*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "MarginInfoResponse found:" << std::endl;
                        std::cout << " Total Cross Collateral = " << std::to_string(static_cast<double>(m->total_cross_collateral)) << std::endl;
                        std::cout << " Total Borrowed = " << std::to_string(static_cast<double>(m->total_borrowed)) << std::endl;
                        std::cout << " Loan Asset ID  = " << std::to_string(static_cast<uint32_t>(m->loan_asset_id)) << std::endl;
                        std::cout << " Collateral Asset ID  = " << std::to_string(static_cast<uint32_t>(m->collateral_asset_id)) << std::endl;
                        std::cout << " Locked Amount = " << std::to_string(static_cast<double>(m->locked_amount)) << std::endl;
                        std::cout << " Loan Amount = " << std::to_string(static_cast<double>(m->loan_amount)) << std::endl;
                        std::cout << " Rate = " << std::to_string(static_cast<double>(m->rate)) << std::endl;
                        std::cout << " Margin Call Collateral Rate = " << std::to_string(static_cast<double>(m->margin_call_collateral_rate)) << std::endl;
                        std::cout << " Liquidation Collateral Rate = " << std::to_string(static_cast<double>(m->liquidation_collateral_rate)) << std::endl;
                        std::cout << " Current Collateral Rate = " << std::to_string(static_cast<double>(m->current_collateral_rate)) << std::endl;
                        std::cout << " Interest = " << std::to_string(static_cast<double>(m->interest)) << std::endl;
                        std::cout << " Exchange ID = " << std::to_string(static_cast<uint8_t>(m->exchange_id)) << std::endl;
                        std::cout << " Margin Info Type = " << std::to_string(static_cast<uint8_t>(m->margin_info_type)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)m, (uint32_t) m->msg_header.msgLength);
                    }
                }
                break;

                case MARGIN_BORROW_REQUEST: {
                    struct MarginBorrowRequest *m = (struct MarginBorrowRequest*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "MarginBorrorwRequest found:" << std::endl;
                        std::cout << " Borrow Amount = " << std::to_string(static_cast<double>(m->borrow_amount)) << std::endl;
                        std::cout << " Borrow Asset ID  = " << std::to_string(static_cast<uint32_t>(m->borrow_asset_id)) << std::endl;
                        std::cout << " Collateral Amount = " << std::to_string(static_cast<double>(m->collateral_amount)) << std::endl;
                        std::cout << " Collateral Asset ID  = " << std::to_string(static_cast<uint32_t>(m->collateral_asset_id)) << std::endl;
                        std::cout << " Internal Borrow ID  = " << std::to_string(static_cast<uint32_t>(m->internal_borrow_id)) << std::endl;
                        std::cout << " Strategy ID  = " << std::to_string(static_cast<uint8_t>(m->strategy_id)) << std::endl;
                        std::cout << " Exchange ID  = " << std::to_string(static_cast<uint8_t>(m->exchange_id)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)m, (uint32_t) m->msg_header.msgLength);
                    }
                }
                break;

                case MARGIN_BORROW_RESPONSE: {
                    struct MarginBorrowResponse *m = (struct MarginBorrowResponse*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "MarginBorrowResponse found:" << std::endl;
                        std::cout << " Sum Borrowed = " << std::to_string(static_cast<double>(m->sum_borrowed)) << std::endl;
                        std::cout << " Internal ID  = " << std::to_string(static_cast<uint32_t>(m->internal_borrow_id)) << std::endl;
                        std::cout << " External ID  = " << std::to_string(static_cast<uint64_t>(m->external_borrow_id)) << std::endl;
                        std::cout << " Strategy ID = " << std::to_string(static_cast<uint8_t>(m->strategy_id)) << std::endl;
                        std::cout << " Exchange ID  = " << std::to_string(static_cast<uint8_t>(m->exchange_id)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)m, (uint32_t) m->msg_header.msgLength);
                    }
                }
                break;                

                case MARGIN_TRANSFER_REQUEST: {
                    struct MarginTransferRequest *m = (struct MarginTransferRequest*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "MarginTransferRequest found:" << std::endl;
                        std::cout << " Transfer Sum = " << std::to_string(static_cast<double>(m->transfer_sum)) << std::endl;
                        std::cout << " Internal ID  = " << std::to_string(static_cast<uint32_t>(m->internal_transfer_id)) << std::endl;
                        std::cout << " Asset ID  = " << std::to_string(static_cast<uint32_t>(m->asset_id)) << std::endl;
                        std::cout << " From Exchange ID  = " << std::to_string(static_cast<uint8_t>(m->from_exchange_id)) << std::endl;
                        std::cout << " To Exchange ID  = " << std::to_string(static_cast<uint8_t>(m->to_exchange_id)) << std::endl;
                        std::cout << " Strategy ID = " << std::to_string(static_cast<uint8_t>(m->strategy_id)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)m, (uint32_t) m->msg_header.msgLength);
                    }
                }
                break;

                case MARGIN_TRANSFER_RESPONSE: {
                    struct MarginTransferResponse *m = (struct MarginTransferResponse*)m;
                    if(output_type == STDOUT_OUTPUT_TYPE){
                        std::cout << "MarginTransferResponse found:" << std::endl;
                        std::cout << " Transfer Sum = " << std::to_string(static_cast<double>(m->transferred_sum)) << std::endl;
                        std::cout << " Internal ID  = " << std::to_string(static_cast<uint32_t>(m->internal_transfer_id)) << std::endl;
                        std::cout << " External ID  = " << std::to_string(static_cast<uint64_t>(m->external_transfer_id)) << std::endl;
                        std::cout << " From Exchange ID  = " << std::to_string(static_cast<uint8_t>(m->from_exchange_id)) << std::endl;
                        std::cout << " To Exchange ID  = " << std::to_string(static_cast<uint8_t>(m->to_exchange_id)) << std::endl;
                        std::cout << " Strategy ID = " << std::to_string(static_cast<uint8_t>(m->strategy_id)) << std::endl;
                    }

                    if(output_type == FILE_OUTPUT_TYPE){
                        bin_file_risk->write_message((char *)m, (uint32_t) m->msg_header.msgLength);
                    }
                }
                break;

                default:
                    break;
            }
        };
}

void Monitor::publish_qty_ratio(ToBUpdate *tob_update) {
    double ratio_to_publish;

    if (tob_update->ask_price >= tob_update->bid_price)
        ratio_to_publish = (static_cast<double>(tob_update->ask_price / tob_update->bid_price) - 1);
    else
        ratio_to_publish = (static_cast<double>(tob_update->bid_price / tob_update->ask_price) * (-1)) + 1;

    influxDBio->write(influxdb::Point{"signal"}
        .addField("value", ratio_to_publish)
        .addTag("type", std::to_string(static_cast<int>(1)))
        .addTag("instr_id", std::to_string(static_cast<uint32_t>(tob_update->instrument_id))));
}


std::string Monitor::get_current_date_as_string(int offset) {
    std::string outputstring;
    char buf[100];
    struct timeval time_now{};
    gettimeofday(&time_now, nullptr);
    time_now.tv_sec += (offset * 3600 *24);
    struct tm tstruct = *localtime(&time_now.tv_sec);
    strftime(buf, sizeof(buf), "%Y%m%d", &tstruct);
    outputstring = buf;
    return(outputstring);
}

Monitor::Monitor(   uint8_t order_env, 
                    uint8_t _output_type,
                    std::string _out_directory) {

    output_type = _output_type;
    out_directory = _out_directory;

    if (order_env == PRODUCTION_ENV){
        influxDBio = influxdb::InfluxDBFactory::Get("http://172.31.39.99:8086/?db=cryptodata");
    }
    else if (order_env == UAT_ENV){
        influxDBio = influxdb::InfluxDBFactory::Get("http://172.31.39.99:8086/?db=cryptodata_uat");
    }

    if(output_type == FILE_OUTPUT_TYPE) {
        std::string current_date = get_current_date_as_string(0);
        bin_file_md = std::unique_ptr<BinaryFile>(new BinaryFile(out_directory + "/" + current_date + "_md.bin", WriteOnlyBinary));
        bin_file_oe = std::unique_ptr<BinaryFile>(new BinaryFile(out_directory + "/" + current_date + "_oe.bin", WriteOnlyBinary));
        bin_file_risk = std::unique_ptr<BinaryFile>(new BinaryFile(out_directory + "/" + current_date + "_rs.bin", WriteOnlyBinary));                
    }

    influxDBio->batchOf(1000);

    io_fh = std::bind(&Monitor::process_io_messages, this);     
    from_aeron_io = new from_aeron(AERON_IO, io_fh);
}

