#include "new_monitor.hpp"

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
                }
                break;

                case TRADE: {
                    struct Trade *t = (struct Trade*)m;
                }
                break;

                case SIGNAL: {
                    struct Signal *s = (struct Signal*)m;
                }
                break;                

                case  MSG_PARENT_ORDER: {
                    struct SendParentOrder *s = (struct SendParentOrder*)m;
                }
                break;

                case MSG_NEW_ORDER: {
                    struct SendOrder *s = (struct SendOrder*)m;
                }
                break;

                case MSG_CANCEL_ORDER: {
                    struct CancelOrder *c = (struct CancelOrder*)m;
                }
                break;
            
                case MSG_REQUEST_ACK: {
                    struct RequestAck *r = (struct RequestAck*)m;
                }
                break;

                case MSG_FILL: {
                    struct Fill *f = (struct Fill*)m;
                }
                break;

                case INSTRUMENT_INFO_REQUEST: {
                    struct InstrumentInfoRequest *i = (struct InstrumentInfoRequest*)m;
                }
                break;

                case INSTRUMENT_INFO_RESPONSE: {
                    struct InstrumentInfoResponse *i = (struct InstrumentInfoResponse*)m;
                }
                break;

                case EXCHANGE_RISK_INFO_REQUEST: {
                    struct ExchangeRiskInfoRequest *e = (struct ExchangeRiskInfoRequest*)m;
                }
                break;

                case EXCHANGE_RISK_INFO_RESPONSE: {
                    struct ExchangeRiskInfoResponse *e = (struct ExchangeRiskInfoResponse*)m;
                }
                break;

                case STRATEGY_INFO_REQUEST: {
                    struct StrategyInfoRequest *s = (struct StrategyInfoRequest*)m;
                }
                break;

                case STRATEGY_INFO_RESPONSE: {
                    struct StrategyInfoResponse *s = (struct StrategyInfoResponse*)m;
                }
                break;

                case EXCHANGE_ID_REQUEST: {
                    struct ExchangeIDRequest *e = (struct ExchangeIDRequest*)m;
                }
                break;

                case EXCHANGE_ID_RESPONSE: {
                    struct ExchangeIDResponse *e = (struct ExchangeIDResponse*)m;
                }
                break;

                case CAPTURE_TIMEOUT: {
                    struct CaptureTimeout *c = (struct CaptureTimeout*)m;
                }
                break;

                case CAPTURE_DISCONNECT: {
                    struct CaptureDisconnect *c = (struct CaptureDisconnect*)m;
                }
                break;

                case HEARTBEAT_RESPONSE: {
                    struct HeartbeatResponse *h = (struct HeartbeatResponse*)m;
                }
                break;

                case ROLL_OVER_REQUEST: {
                    struct RolloverRequest *r = (struct RolloverRequest*)m;
                }
                break;

                case ROLL_OVER_RESPONSE: {
                    struct RolloverResponse *r = (struct RolloverResponse*)m;
                }
                break;

                case ACCOUNT_INFO_UPDATE: {
                    struct AccountInfoResponse *a = (struct AccountInfoResponse *)m;
                }
                break;

                case ACCOUNT_INFO_REQUEST: {
                    struct AccountInfoRequest *a = (struct AccountInfoRequest*)m;
                }
                break;

                case MARGIN_INFO_REQUEST: {
                    struct MarginInfoRequest *m = (struct MarginInfoRequest*)m;
                }
                break;

                case MARGIN_INFO_RESPONSE: {
                    struct MarginInfoResponse *m = (struct MarginInfoResponse*)m;
                }
                break;

                case MARGIN_BORROW_REQUEST: {
                    struct MarginBorrowRequest *m = (struct MarginBorrowRequest*)m;
                }
                break;

                case MARGIN_BORROW_RESPONSE: {
                    struct MarginBorrowResponse *m = (struct MarginBorrowResponse*)m;
                }
                break;                

                case MARGIN_TRANSFER_REQUEST: {
                    struct MarginTransferRequest *m = (struct MarginTransferRequest*)m;
                }
                break;

                case MARGIN_TRANSFER_RESPONSE: {
                    struct MarginTransferResponse *m = (struct MarginTransferResponse*)m;
                }
                break;

                default:
                    break;
            }
        };
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


    if(output_type == FILE_OUTPUT_TYPE) {
        std::string current_date = get_current_date_as_string(0);
        bin_file_md = std::unique_ptr<BinaryFile>(new BinaryFile(out_directory + "/" + current_date + "_md.bin", WriteOnlyBinary));
        bin_file_oe = std::unique_ptr<BinaryFile>(new BinaryFile(out_directory + "/" + current_date + "_oe.bin", WriteOnlyBinary));
        bin_file_risk = std::unique_ptr<BinaryFile>(new BinaryFile(out_directory + "/" + current_date + "_rs.bin", WriteOnlyBinary));                
    }

    io_fh = std::bind(&Monitor::process_io_messages, this);     
    from_aeron_io = new from_aeron(AERON_IO, io_fh);
}

