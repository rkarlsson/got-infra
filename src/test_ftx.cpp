#include "wsock_ftx.hpp"
#include "logger.hpp"

int main() {
    Logger              *logger;
    Logger              *subscription_logger;
 
    WSock *wsocket = new WSock(logger, subscription_logger, 1800, 50);
    wsocket->add_subscription_request("wss://ftx.com/ws");
}
