#include <iostream>
#include <algorithm>
#include <cctype>
#include <string>
#include <list>
#include <unordered_map>
#include <map>
#include <getopt.h>
#include <chrono>
#include <thread>
#include "logger.hpp"
#include "aeron_types.hpp"
#include "to_aeron.hpp"
#include "merged_orderbook.hpp"
#include "PLUpdates.h"

static const int FRAGMENTS_LIMIT = 10;

void print_options(){
    std::cout << "Options for md_client:" << std::endl;
    std::cout << "  -E (--environment) <PROD|UAT>                           = Sets to Prod or UAT config (need one of them)" << std::endl;
    std::cout << "  [-h (--help)]                                           = Prints this message" << std::endl;
}

uint64_t get_current_ts() {
  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);
  return((t.tv_sec*1000000000L)+t.tv_nsec);
}

void get_snapshot_for_instrument(MergedOrderbook *pl_book, uint32_t instrument_id, to_aeron *to_aeron_ss) {
    bool                                finished = false;
    DepthSnapshotRequest                snap_request;

    aeron::Context                      snap_context;
    std::shared_ptr<Aeron>              snap_aeron = Aeron::connect(snap_context);
    std::int64_t                        snap_channel_id = snap_aeron->addSubscription("aeron:ipc", AERON_SS);
    std::shared_ptr<Subscription>       snap_subscription = snap_aeron->findSubscription(snap_channel_id);

    // First clear the orderbook
    pl_book->clear_orderbook();
    snap_request.msg_header = {sizeof(DepthSnapshotRequest), DEPTH_SNAPSHOT_REQUEST, 1};
    snap_request.instrument_id = instrument_id;

    while (!snap_subscription) {
      snap_subscription = snap_aeron->findSubscription(snap_channel_id);
    }

    // Fragment handler lambda, putting it here so I can easily pass extra variables..:)
    auto snap_fragment_lambda = [&pl_book, &instrument_id, &finished](const AtomicBuffer &buffer, util::index_t offset, util::index_t length, const Header &header) {
        struct MessageHeader *m = (MessageHeader*)(reinterpret_cast<const char *>(buffer.buffer()) + offset);
        if(m->msgType == sbe::PLUpdates::SBE_TEMPLATE_ID) {
            sbe::PLUpdates *pl = new sbe::PLUpdates((char *) m, 1024*1024);
            if(pl->instrument_id() == instrument_id){
                // Process update
                pl_book->process_update(pl);
                // Check if this is the last message from the series
                if(pl->update_flags() & PL_UPDATE_LAST_MSG_IN_SERIES){
                    // Set this to true so the loop has finished
                    finished = true;
                }
            }
        }
    };

    to_aeron_ss->send_data((char *) &snap_request, sizeof(DepthSnapshotRequest));

    while(finished == false) {
      const int fragmentsRead = snap_subscription->poll(snap_fragment_lambda, FRAGMENTS_LIMIT);
      usleep(10);
    }

}


// -----------------------------------------------------------------------
// Main thread for MD_CLIENT
// -----------------------------------------------------------------------
int main(int argc, char** argv) {
    // LogWorker                           *log_worker;
    // Logger                              *logger;
    // char                                snapshot_buffer[1024*1024];
    aeron::Context                          context;
    std::string                             environment_name = "PROD";
    std::map<uint32_t, MergedOrderbook *>   pl_books;
    uint64_t                                msg_counter = 0;
    uint64_t                                timestamp = 0;
    to_aeron                                *to_aeron_ss;

    static struct option long_options[] = {
    {"environment"    , optional_argument, NULL, 'E'},
    {"help"           , optional_argument, NULL, 'h'}};

    int ch;
    while((ch = getopt_long(argc, argv, "E:h", long_options, NULL)) != -1) {
        std::string instrument_string;
        std::stringstream instr_stringstream;
        std::string cell;

        switch (ch) {
            case 'h':
                print_options();
                exit(0);

            case 'E':
            // environment_given = true;
            environment_name = optarg;
            break;

            default:
            break;
        }
    }

    to_aeron_ss = new to_aeron(AERON_SS);

    std::shared_ptr<Aeron> aeron = Aeron::connect(context);
    std::int64_t channel_id = aeron->addSubscription("aeron:ipc", AERON_IO);
    std::shared_ptr<Subscription> subscription = aeron->findSubscription(channel_id);
    while (!subscription) {
      subscription = aeron->findSubscription(channel_id);
    }

    to_aeron_ss = new to_aeron(AERON_SS);
    timestamp = get_current_ts();

    // Fragment handler lambda, putting it here so I can easily pass extra variables..:)
    auto fragment_lambda = [&pl_books, &msg_counter, &timestamp, &to_aeron_ss](const AtomicBuffer &buffer, util::index_t offset, util::index_t length, const Header &header) {
        struct MessageHeader *m = (MessageHeader*)(reinterpret_cast<const char *>(buffer.buffer()) + offset);
        
        switch(m->msgType) {
            case TOB_UPDATE: {
                struct ToBUpdate *tob_update = (struct ToBUpdate*)m;
                auto item = pl_books.find(tob_update->instrument_id);
                if(item != pl_books.end()){
                    // we have the symbol orderbook..
                    item->second->process_update(tob_update);
                } else {
                    auto new_book = new MergedOrderbook();
                    get_snapshot_for_instrument(new_book, tob_update->instrument_id, to_aeron_ss);
                    new_book->process_update(tob_update);
                    pl_books[tob_update->instrument_id] = new_book;
                }
                msg_counter++;

            }
            break;
            
            case sbe::PLUpdates::SBE_TEMPLATE_ID: {
                sbe::PLUpdates *pl = new sbe::PLUpdates((char *) m, 1024*1024);
                // struct PLUpdates *pl_update = (struct PLUpdates*)m;

                auto item = pl_books.find(pl->instrument_id());
                if(item != pl_books.end()){
                    // we have the symbol orderbook..
                    item->second->process_update(pl);
                } else {
                    auto new_book = new MergedOrderbook();
                    get_snapshot_for_instrument(new_book, pl->instrument_id(), to_aeron_ss);
                    new_book->process_update(pl);
                    pl_books[pl->instrument_id()] = new_book;
                }
                msg_counter++;
            }
            break;

            case TRADE: {
                struct Trade *trade = (struct Trade*)m;
                msg_counter++;
            }
            break;

            case INSTRUMENT_CLEAR_BOOK: {
                struct InstrumentClearBook *instr_clear = (struct InstrumentClearBook*)m;
                auto item = pl_books.find(instr_clear->instrument_id);
                if(item != pl_books.end()){
                    // we have the symbol orderbook..
                    item->second->clear_orderbook();
                }
            }
            break;
        }
        if(msg_counter > 10000){
            auto new_ts = get_current_ts();
            auto messages_per_second = (msg_counter*1.0) / ((new_ts*1.0 - timestamp)/1000000000);
            std::cout << "Messagerate: " << std::to_string(messages_per_second) << std::endl;
            timestamp = new_ts;
            msg_counter = 0;
        }
    };

    while(1) {
      const int fragmentsRead = subscription->poll(fragment_lambda, FRAGMENTS_LIMIT);
      usleep(10);
    }

}