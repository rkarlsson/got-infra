#include "wsock_ftx.hpp"
#include "logger.hpp"

int main() {
    Logger              *logger;
    Logger              *subscription_logger;
    LogWorker           *log_worker;
    bool stdout_only = true;
    log_worker = new LogWorker("svc_md_ftx", "FTX", "PROD", stdout_only);
    logger = log_worker->get_new_logger("base");
    subscription_logger = log_worker->get_new_logger("base");
 
    WSock *wsocket = new WSock(logger, subscription_logger, 1800, 50);
    wsocket->add_subscription_request("wss://ftx.com/ws/");

    sleep(1);

    std::string inst = "BTC/USD";
//    std::string msg = "\n{\"op\": \"subscribe\",\n\"channel\":\"orderbook\",\n\"market\":\""+inst+"\"}\n";
//    wsocket->send_data(msg.data(), msg.size());
//    msg = "\n{\"op\": \"subscribe\",\n\"channel\":\"trades\",\n\"market\":\""+inst+"\"}\n";
//    wsocket->send_data(msg.data(), msg.size());
    std::string msg = "\n{\"op\": \"subscribe\",\n\"channel\":\"ticker\",\n\"market\":\""+inst+"\"}\n";
    wsocket->send_data(msg.data(), msg.size());
 
    for(;;) {
	std::string_view message_to_print;
        message_to_print = wsocket->get_next_message_from_websocket();
	std::cout << message_to_print << std::endl;
    }

}
