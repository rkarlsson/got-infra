#include "aeron_types.hpp"
#include <unistd.h>
#include "to_aeron.hpp"

void start_heartbeat(uint8_t exchange_id, uint8_t service_type) {    
    std::thread hb_thread([exchange_id, service_type]() {
        to_aeron *to_aeron_io;
        to_aeron_io = new to_aeron(AERON_IO);
        HeartbeatResponse hb_msg;
        hb_msg.msg_header = {sizeof(HeartbeatResponse), HEARTBEAT_RESPONSE, 1};
        hb_msg.exchange_id = exchange_id;
        hb_msg.service_type = service_type;
        hb_msg.strategy_id = 0;

        // Now start the loop that will snapshot every x seconds
        while(1){
            to_aeron_io->send_data((char *)&hb_msg, sizeof(HeartbeatResponse));
            sleep(1);
        }

    });
    hb_thread.detach();
}