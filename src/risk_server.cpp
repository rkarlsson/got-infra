#include <cmath> 
#include "risk_server.hpp"

// =================================================================================
void RiskServer::init_strategies() {
    // Initialise the vectors for all the strategies
    for(auto const& strategy: strategies) {
        if(open_orders_per_strategy.count(strategy.first) == 0)
            open_orders_per_strategy[strategy.first] = new OrderDetailsMapT();
    }
}

// =================================================================================
void RiskServer::new_instrument_callback(struct InstrumentInfoResponse *instrument) {
    // Initialise the exchange status map with entries for all known exchanges
    if(exchange_status_map.count(instrument->exchange_id) == 0)
        exchange_status_map[instrument->exchange_id] = new ExchangeStatus();

    // Initialise the reference price map for all known instruments
    if(ref_price_map.count(instrument->instrument_id) == 0)
        ref_price_map[instrument->instrument_id] = new ReferencePrice();
    if(open_orders_per_instrument.count(instrument->instrument_id) == 0)
        open_orders_per_instrument[instrument->instrument_id] = new OrderDetailsMapT();
    
    if(position_map_futures.count(instrument->instrument_id) == 0) {
        // Add new one to the map
        position_map_futures[instrument->instrument_id] = new PositionDetails();
        //initialise the array with 0 values
        for(int i=0; i < 4; i++){
            position_map_futures[instrument->instrument_id]->position[i] = 0.0;
            position_map_futures[instrument->instrument_id]->last_updated[i] = 0;
        }
    }
}

// =================================================================================
void RiskServer::position_update_callback(uint32_t instrument_id, uint32_t asset_id, double position_value) {
    uint64_t recv_ts = get_current_ts_ns();
    
    // Instrument update
    if((instrument_id != 0) && (asset_id == 0)){

        if(position_map_futures.count(instrument_id) == 0) {
            add_instrument_to_position_map_futures(instrument_id);
        }

        logger->msg(INFO, "Found new position value in database for Instrument: " + refdb->get_symbol_name_from_id(instrument_id) + 
                            " - updating svc_risk with new value from database for reconcilisation purposes. Old value: " + 
                            std::to_string(position_map_futures[instrument_id]->position[DATABASE_POSITION]) +
                            " New value: " + std::to_string(position_value));

        position_map_futures[instrument_id]->position[DATABASE_POSITION] = position_value;
        position_map_futures[instrument_id]->last_updated[DATABASE_POSITION] = recv_ts;
    }

    // Asset update
    else if ((instrument_id == 0) && (asset_id != 0)){

        if(position_map_assets.count(asset_id) == 0) {
            add_asset_to_position_map_assets(asset_id);
        }

        logger->msg(INFO, "Found new position value in database for Asset: " + refdb->get_asset_code(asset_id) + 
                            " - updating svc_risk with new value from database for reconcilisation purposes. Old value: " + 
                            std::to_string(position_map_assets[asset_id]->position[DATABASE_POSITION]) +
                            " New value: " + std::to_string(position_value));

        position_map_assets[asset_id]->position[DATABASE_POSITION] = position_value;
        position_map_assets[asset_id]->last_updated[DATABASE_POSITION] = recv_ts;
    }
}

// =================================================================================
bool RiskServer::is_exchange_live(uint8_t exchange_id) {
    if(exchange_status_map.count(exchange_id) > 0){
        return(exchange_status_map[exchange_id]->is_live);
    } else {
        return(false);
    }
}

// =================================================================================
bool RiskServer::is_strategy_live(uint8_t strategy_id) {
    if(strategies.count(strategy_id) > 0) {
        return(true);
    } else {
        return(false);
    }
}

// =================================================================================
uint64_t RiskServer::get_current_ts_ns() {
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return((t.tv_sec*1000000000L)+t.tv_nsec);
}

// // =================================================================================
// void RiskServer::rollover_database(struct RolloverRequest *rollover_request) {
//     // Reques rollover from database
//     trading_db->rollover_day(rollover_request->request_time_stamp);

//     // Tell back the bus that it has acked and taken care of
//     struct timespec t;
//     struct RolloverResponse rollover_response;
//     clock_gettime(CLOCK_REALTIME, &t);
//     uint64_t recv_ts = (t.tv_sec*1000000000L)+t.tv_nsec;    
//     rollover_response.msg_header.msgType = ROLL_OVER_RESPONSE;
//     rollover_response.msg_header.msgLength = sizeof(struct RolloverRequest);
//     rollover_response.msg_header.protoVersion = 1;
//     rollover_response.response_time_stamp = recv_ts;
//     to_aeron_io->send_data((char *)&rollover_response, sizeof(RolloverResponse));
// }

// =================================================================================
void RiskServer::recalculate_risk(uint32_t internal_order_id, uint32_t instrument_id, bool order_removed) {
    // TODO WRITE RISK UPDATE LOGIC
    if(internal_order_id == 0){
        // This is a reference price update - we need to recalculate all orders in the instrument map
    } else {
        // This is a order update or new order - we need to calculate just that order
    }
}

// =================================================================================
void RiskServer::close_order_in_datastructure(struct RequestAck *request_ack) {
    // Check if order is in the open_orders - otherwise - do nothing
    if(open_orders.count(request_ack->internal_order_id)){
        // Move order to closed
        closed_orders[request_ack->internal_order_id] = open_orders[request_ack->internal_order_id];
        open_orders.erase(request_ack->internal_order_id);

        // Remove from open strategy orders
        // if(open_orders_per_strategy[request_ack->strategy_id]->count(request_ack->internal_order_id)){
        //     open_orders_per_strategy[request_ack->strategy_id]->erase(request_ack->internal_order_id);
        // }

        // Remove from open instrument map orders
        if(open_orders_per_instrument[request_ack->instrument_id]->count(request_ack->internal_order_id)){
            open_orders_per_instrument[request_ack->instrument_id]->erase(request_ack->internal_order_id);
        }
    }
    recalculate_risk(0, request_ack->instrument_id, false);
}

// =================================================================================
void RiskServer::add_order_to_datastructure(struct SendOrder *new_order, uint64_t time_stamp) {
    // Add order to the order map for risk tracking
    auto new_order_detail = new OrderDetails();
    new_order_detail->order_new_ts = time_stamp;
    new_order_detail->initial_qty = new_order->qty;
    new_order_detail->limit_price = new_order->price;
    new_order_detail->instrument_id = new_order->instrument_id;
    new_order_detail->strategy_id = new_order->strategy_id;
    new_order_detail->is_buy = new_order->is_buy;

    // Add it to the different maps
    open_orders[new_order->internal_order_id] = new_order_detail;

    if (!open_orders_per_strategy.count(new_order->strategy_id)) {
        open_orders_per_strategy[new_order->strategy_id] = new OrderDetailsMapT();
    }
    open_orders_per_strategy[new_order->strategy_id]->insert(
            std::pair<uint32_t,struct OrderDetails *>(new_order->internal_order_id, new_order_detail));
    if (!open_orders_per_instrument.count(new_order->instrument_id)) {
        open_orders_per_instrument[new_order->instrument_id] = new OrderDetailsMapT();
    }
    open_orders_per_instrument[new_order->instrument_id]->insert(
            std::pair<uint32_t,struct OrderDetails *>(new_order->internal_order_id, new_order_detail));
    recalculate_risk(new_order->internal_order_id, new_order->instrument_id, false);
}

// =================================================================================
void RiskServer::request_live_exchange_positions() {
    for(auto const& exchange: exchange_status_map) {
        if(exchange.second->is_live){
            logger->msg(INFO, "Requesting Position Update for exchange id: " + std::to_string(exchange.first) + " (" + refdb->get_exchange_name(exchange.first) + ")");
            AccountInfoRequest acct_request = {MessageHeaderT{sizeof(AccountInfoRequest), ACCOUNT_INFO_REQUEST, 0}, exchange.first, POSITION_RISK_REQUEST};
            to_aeron_io->send_data((char *)&acct_request, sizeof(acct_request)); 
        }
    }
}

// =================================================================================
void RiskServer::risk_new_parent_order(struct SendParentOrder *new_parent_order) {
    uint64_t recv_ts = get_current_ts_ns();

    std::string json_config = std::string(new_parent_order->json_config);
    trading_db->NewParentOrder(   recv_ts, 
                            std::to_string(new_parent_order->parent_order_id),
                            json_config);

    // Exchange isn't live - sending reject for now. Could be made more intelligent but useful for now
    logger->msg(INFO, "New Parent OrderID added: " + std::to_string(new_parent_order->parent_order_id));
    RequestAck r;
    r.msg_header = MessageHeaderT{sizeof(RequestAck), MSG_REQUEST_ACK, 0};
    r.internal_order_id = new_parent_order->parent_order_id;
    r.external_order_id[0] = '\0';
    r.instrument_id = 0;
    r.exchange_id = 0;
    r.ack_type = PARENT_REQUEST_ACK;
    r.reject_reason = UNKNOWN_REJECT;
    r.strategy_id = 0;
    r.send_timestamp = get_current_ts_ns();
    to_aeron_io->send_data((char*)&r, sizeof(r));
}

// =================================================================================
void RiskServer::risk_modify_parent_order(struct ModifyParentOrder *modify_parent_order) {
    uint64_t recv_ts = get_current_ts_ns();

    std::string json_config = std::string(modify_parent_order->json_config);
    trading_db->ModifyParentOrder(   recv_ts, 
                            std::to_string(modify_parent_order->parent_order_id),
                            json_config);

    // Exchange isn't live - sending reject for now. Could be made more intelligent but useful for now
    logger->msg(INFO, "Modify Parent OrderID: " + std::to_string(modify_parent_order->parent_order_id));
    RequestAck r;
    r.msg_header = MessageHeaderT{sizeof(RequestAck), MSG_REQUEST_ACK, 0};
    r.internal_order_id = modify_parent_order->parent_order_id;
    r.external_order_id[0] = '\0';
    r.instrument_id = 0;
    r.exchange_id = 0;
    r.ack_type = PARENT_MODIFY_ACK;
    r.reject_reason = UNKNOWN_REJECT;
    r.strategy_id = 0;
    r.send_timestamp = get_current_ts_ns();
    to_aeron_io->send_data((char*)&r, sizeof(r));
}

// =================================================================================
void RiskServer::risk_cancel_parent_order(struct CancelParentOrder *cancel_parent_order) {
    uint64_t recv_ts = get_current_ts_ns();

    trading_db->CancelParentOrder(  recv_ts, 
                                    std::to_string(cancel_parent_order->parent_order_id));

    // Exchange isn't live - sending reject for now. Could be made more intelligent but useful for now
    logger->msg(INFO, "Cancel Parent OrderID: " + std::to_string(cancel_parent_order->parent_order_id));
    RequestAck r;
    r.msg_header = MessageHeaderT{sizeof(RequestAck), MSG_REQUEST_ACK, 0};
    r.internal_order_id = cancel_parent_order->parent_order_id;
    r.external_order_id[0] = '\0';
    r.instrument_id = 0;
    r.exchange_id = 0;
    r.ack_type = PARENT_CANCEL_ACK;
    r.reject_reason = UNKNOWN_REJECT;
    r.strategy_id = 0;
    r.send_timestamp = get_current_ts_ns();
    to_aeron_io->send_data((char*)&r, sizeof(r));
}

// =================================================================================
void RiskServer::risk_new_order(struct SendOrder *new_order) {
    uint64_t recv_ts = get_current_ts_ns();

    // if(is_exchange_live(new_order->exchange_id)) {
        trading_db->NewOrder(recv_ts, new_order);

        if(! is_strategy_live(new_order->strategy_id)) {
            // TODO - This should be rejected - we need to add this to risk rejects in exchange linehandler
        }
        add_order_to_datastructure(new_order, recv_ts);
    // } else {
    //     // Exchange isn't live - sending reject for now. Could be made more intelligent but useful for now
    //     logger->msg(INFO, "Rejecting order id: " + std::to_string(new_order->internal_order_id) + " as exchange id: " + std::to_string(new_order->exchange_id) + " is not available");
    //     RequestAck r;
    //     r.msg_header = MessageHeaderT{sizeof(RequestAck), MSG_REQUEST_ACK, 0};
    //     r.internal_order_id = new_order->internal_order_id;
    //     r.external_order_id[0] = '\0';
    //     r.instrument_id = new_order->instrument_id;
    //     r.exchange_id = new_order->exchange_id;
    //     r.ack_type = REQUEST_REJECT;
    //     r.reject_reason = EXCHANGE_NOT_AVAILABLE;
    //     r.strategy_id = new_order->strategy_id;
    //     r.send_timestamp = get_current_ts_ns();
    //     to_aeron_io->send_data((char*)&r, sizeof(r));
    // }
}

// =================================================================================
void RiskServer::risk_request_ack(struct RequestAck *request_ack) {
    uint64_t recv_ts = get_current_ts_ns();

    switch(request_ack->ack_type) {
        case REQUEST_ACK:
            // We ignore these - just used to ensure internal components are visible
            break;

        case EXCHANGE_ACK:
            // This is now a real risk
            trading_db->NewOrderAck(   
                    recv_ts, 
                    std::to_string(request_ack->internal_order_id),
                    (char *)request_ack->external_order_id);
            break;

        case EXCHANGE_CANCEL_ACK:
            trading_db->OrderClose(   
                    recv_ts, 
                    std::to_string(request_ack->internal_order_id));
            close_order_in_datastructure(request_ack);
            break;

        case REQUEST_REJECT: 
            {
                std::string _rej_msg;
                switch(request_ack->reject_reason) {
                    case ORDER_FOR_WRONG_ENVIRONMENT: // Same as for risk
                    case RISK_REJECT:{
                        std::string ret_msg = request_ack->reject_message;
                        _rej_msg = "Risk Reject: " + ret_msg;
                        }
                        break;

                    case INSTRUMENT_NOT_VALID:
                        _rej_msg = "Instrument Not Valid";
                        break;

                    case EXCHANGE_NOT_AVAILABLE:
                        _rej_msg = "Exchange Not Available";
                        break;

                    case NOT_THIS_EXCHANGE:
                        _rej_msg = "Wrong Exchange ID for Instrument (not this exchange)";
                        break;

                    default:
                        break;
                }
                trading_db->OrderRejected(   
                    recv_ts, 
                    std::to_string(request_ack->internal_order_id),
                    _rej_msg);
                close_order_in_datastructure(request_ack);
            }
            break;

        case EXCHANGE_REJECT:
            trading_db->OrderRejected(   
                    recv_ts, 
                    std::to_string(request_ack->internal_order_id),
                    request_ack->reject_message);
            close_order_in_datastructure(request_ack);
            break;

        default:
            break;
    }
}

// =================================================================================
void RiskServer::add_instrument_to_position_map_futures(uint32_t instrument_id) {
    if(position_map_futures.count(instrument_id) == 0) {
        position_map_futures[instrument_id] = new PositionDetails();
        position_map_futures[instrument_id]->position[EXCHANGE_POSITION] = 0.0;
        position_map_futures[instrument_id]->position[DATABASE_POSITION] = 0.0;
        position_map_futures[instrument_id]->position[CALCULATED_POSITION] = 0.0;
        position_map_futures[instrument_id]->last_updated[EXCHANGE_POSITION] = 0;
        position_map_futures[instrument_id]->last_updated[DATABASE_POSITION] = 0;
        position_map_futures[instrument_id]->last_updated[CALCULATED_POSITION] = 0;
    }
}

// =================================================================================
void RiskServer::add_asset_to_position_map_assets(uint32_t asset_id) {
    if(position_map_assets.count(asset_id) == 0) {
        position_map_assets[asset_id] = new PositionDetails();
        position_map_assets[asset_id]->position[EXCHANGE_POSITION] = 0.0;
        position_map_assets[asset_id]->position[DATABASE_POSITION] = 0.0;
        position_map_assets[asset_id]->position[CALCULATED_POSITION] = 0.0;
        position_map_assets[asset_id]->last_updated[EXCHANGE_POSITION] = 0;
        position_map_assets[asset_id]->last_updated[DATABASE_POSITION] = 0;
        position_map_assets[asset_id]->last_updated[CALCULATED_POSITION] = 0;
    }
}

// =================================================================================
void RiskServer::risk_fill(struct Fill *fill_details) {
    uint64_t recv_ts = get_current_ts_ns();
    std::string execID = fill_details->exchange_trade_id;

    auto instr_ptr = refdb->get_symbol_from_id(fill_details->instrument_id);

    // This is a spot (asset) - we need to only make use of the Base asset code and create a position in there    
    if(instr_ptr->instrument_type == SPOT_INSTRUMENT_TYPE) {
        // get base asset id from instrument
        if(position_map_assets.count(instr_ptr->base_asset_id) == 0) {
            add_asset_to_position_map_assets(instr_ptr->base_asset_id);
        }
        // need to take "side" in consideration
        position_map_assets[instr_ptr->base_asset_id]->position[CALCULATED_POSITION] += fill_details->fill_qty;
        position_map_assets[instr_ptr->base_asset_id]->last_updated[CALCULATED_POSITION] = recv_ts;
    }
    
    // This is a derivatives instrument fill
    else {
        if(position_map_futures.count(fill_details->instrument_id) == 0) {
            add_instrument_to_position_map_futures(fill_details->instrument_id);
        }
        // need to take "side" in consideration
        // if(fill_details->internal_order_id)
        position_map_futures[fill_details->instrument_id]->position[CALCULATED_POSITION] += fill_details->fill_qty;
        position_map_futures[fill_details->instrument_id]->last_updated[CALCULATED_POSITION] = recv_ts;
    }

    trading_db->OrderFill(
            recv_ts, 
            std::to_string(fill_details->internal_order_id),
            execID,
            fill_details->fill_qty, 
            fill_details->leaves_qty,
            fill_details->fill_price);
    // TODO - add fill to internal datastructure

}

// =================================================================================
void RiskServer::risk_trading_fee(struct TradingFee *fee_details) {

    trading_db->TransactionFee(
            std::to_string(fee_details->internal_order_id),
            std::string(fee_details->exchange_trade_id),
            fee_details->commission_fee, 
            fee_details->asset_id,
            fee_details->fee_type);

}

// =================================================================================
void RiskServer::risk_tob_update(struct ToBUpdate *tob_update) {
    // uint64_t recv_ts = get_current_ts_ns();
    // double change_amount;
    // double ref_price;
}

// =================================================================================
void RiskServer::risk_md_trade_update(struct Trade *trade_update) {
    uint64_t recv_ts = get_current_ts_ns();
    ref_price_map[trade_update->instrument_id]->last_trade_price = trade_update->price;
    ref_price_map[trade_update->instrument_id]->last_trade_update_ts = recv_ts;
}

// =================================================================================
void RiskServer::risk_hb_update(struct HeartbeatResponse *hb_update) {
    uint64_t recv_ts = get_current_ts_ns();

    // We only track execution services
    if(hb_update->service_type == EXECUTION_SERVICE) {
        if(exchange_status_map.count(hb_update->exchange_id) == 0) {
            exchange_status_map[hb_update->exchange_id] = new ExchangeStatus();
            logger->msg(INFO, "First time we see heartbeats from this exchange: " + refdb->get_exchange_name(hb_update->exchange_id));
            exchange_status_map[hb_update->exchange_id]->is_live = true;
            exchange_status_map[hb_update->exchange_id]->lastHeartBeat_ts = recv_ts;
        } else {
            uint64_t time_since_last_heartbeat = recv_ts - exchange_status_map[hb_update->exchange_id]->lastHeartBeat_ts;
            if(time_since_last_heartbeat > 5000000000) {
                logger->msg(INFO, "Last heartbeat seen was more than 5 seconds ago, now live again: " + refdb->get_exchange_name(hb_update->exchange_id));
                exchange_status_map[hb_update->exchange_id]->reported_as_dead = false;
            }
            // UPDATE EXCHANGE_STATUS
            exchange_status_map[hb_update->exchange_id]->is_live = true;
            exchange_status_map[hb_update->exchange_id]->lastHeartBeat_ts = recv_ts;
        }
    }
}

// =================================================================================
void RiskServer::risk_position_update(struct AccountInfoResponse *acct_update) {
    uint64_t recv_ts = get_current_ts_ns();

    // INSTRUMENT UPDATE
    if(acct_update->instrument_id != 0 and acct_update->asset_id == 0) {
        if(position_map_futures.count(acct_update->instrument_id) == 0) {
            add_instrument_to_position_map_futures(acct_update->instrument_id);
        }
        position_map_futures[acct_update->instrument_id]->position[EXCHANGE_POSITION] = acct_update->value;
        position_map_futures[acct_update->instrument_id]->last_updated[EXCHANGE_POSITION] = recv_ts;

    // ASSET UPDATE
    } else if(acct_update->instrument_id == 0 and acct_update->asset_id != 0) {
        if(position_map_assets.count(acct_update->asset_id) == 0) {
            add_asset_to_position_map_assets(acct_update->asset_id);
        }
        position_map_assets[acct_update->asset_id]->position[EXCHANGE_POSITION] = acct_update->value; // no add here as it is a final value for reconciliation
        position_map_assets[acct_update->asset_id]->last_updated[EXCHANGE_POSITION] = recv_ts;        
    }
}

// =================================================================================
void RiskServer::risk_restart_component(ComponentRestartRequest *restart_request) {
    // Logg the request
    std::string req_string = "Restart request of component: " + std::to_string(restart_request->component_id);
    req_string += " for exchange id: " + std::to_string(restart_request->exchange_id);
    req_string += " with reason: " + std::string(restart_request->restart_reason);
    logger->msg(INFO, req_string);

    // Send an ack - we accept the request
    ComponentRestartResponse restart_response;
    restart_response.msg_header = MessageHeaderT{sizeof(ComponentRestartResponse), COMPONENT_RESTART_RESPONSE, 1};
    restart_response.exchange_id = restart_request->exchange_id;
    restart_response.component_id = restart_request->component_id;
    restart_response.restart_ack_type = RESTART_ACCEPTED;
    to_aeron_io->send_data((char*)&restart_response, sizeof(restart_response));

    // now actually restart the component
    // system("/usr/bin/python3 mrsync.py -m /tmp/targets.list -s /tmp/sourcedata -t /tmp/targetdata");
}

// =================================================================================
void RiskServer::heartbeat_request_thread() {
    std::thread heartbeat_thread([this]() {
        uint64_t current_ts;
        to_aeron *to_aeron_io_hn;
        to_aeron_io_hn = new to_aeron(AERON_IO);

        while(1) {
            HeartbeatRequest h = {MessageHeaderT{sizeof(HeartbeatRequest), HEARTBEAT_REQUEST, 0}};
            to_aeron_io_hn->send_data((char *)&h, sizeof(h));
            sleep(1);
            // Check if any timestamps are older than 5 seconds and report then as dead
            current_ts = get_current_ts_ns();
            for (auto& exchange_iterator: exchange_status_map) {
                uint64_t time_since_last_heartbeat = current_ts - exchange_iterator.second->lastHeartBeat_ts;
                if ((time_since_last_heartbeat > 5000000000) &&
                    (! exchange_iterator.second->reported_as_dead)) {
                        logger->msg(INFO, "Last exchange heartbeat seen was more than 5 seconds ago: " + refdb->get_exchange_name(exchange_iterator.first));
                         exchange_iterator.second->reported_as_dead = true;
                         exchange_iterator.second->is_live = false;
                }
            }
        }
    });
    heartbeat_thread.detach();
}

// =================================================================================
void RiskServer::position_reconciliation_thread(Logger *logger) {
    std::thread position_reconciliation_thread([this, logger]() {
        logger->msg(INFO, "Starting reconciliation thread");
        while(1) {
            request_live_exchange_positions();
            trading_db->get_all_future_positions();
            trading_db->get_all_asset_positions();
            sleep(2);
            uint64_t current_ts = get_current_ts_ns();
            // For each value in position_map - compare the different values and report when 
            // timestamp for all 3 are more than a second old and values are differing
            for (auto& position_iterator: position_map_futures) {
                // Do stuff
                if( ((current_ts - position_iterator.second->last_updated[EXCHANGE_POSITION]) > 1000000000) &&
                    ((current_ts - position_iterator.second->last_updated[DATABASE_POSITION]) > 1000000000) &&
                    ((current_ts - position_iterator.second->last_updated[CALCULATED_POSITION]) > 1000000000)){
                        // All timestamps are more than a second old, good stuff
                        std::string exchange_name = refdb->get_exchange_name_from_symbol_id(position_iterator.first);
                        std::string symbol_id = std::to_string(position_iterator.first);
                        if( (abs(position_iterator.second->position[EXCHANGE_POSITION]) - abs(position_iterator.second->position[DATABASE_POSITION])) > 0.0000001){
                            logger->msg(INFO, "WARNING: Exchange reported position is different to database position for Instrument: " + 
                            refdb->get_symbol_name_from_id(position_iterator.first) + " (" + symbol_id + " @ " + exchange_name + ") - Exchange-position: " +
                            std::to_string(position_iterator.second->position[EXCHANGE_POSITION]) +
                            " Database position: " + std::to_string(position_iterator.second->position[DATABASE_POSITION]));
                        }
                        if( (abs(position_iterator.second->position[CALCULATED_POSITION]) - abs(position_iterator.second->position[DATABASE_POSITION])) > 0.0000001){
                            logger->msg(INFO, "WARNING: Calculated position is different to database position for Instrument: " + 
                            refdb->get_symbol_name_from_id(position_iterator.first)  + " (" + symbol_id + " @ " + exchange_name + ") - Calculated-position: " +
                            std::to_string(position_iterator.second->position[CALCULATED_POSITION]) +
                            " Database position: " + std::to_string(position_iterator.second->position[DATABASE_POSITION]));
                        }

                    }
            }
            for (auto& position_iterator: position_map_assets) {
                // Do stuff
                if( ((current_ts - position_iterator.second->last_updated[EXCHANGE_POSITION]) > 1000000000) &&
                    ((current_ts - position_iterator.second->last_updated[DATABASE_POSITION]) > 1000000000) &&
                    ((current_ts - position_iterator.second->last_updated[CALCULATED_POSITION]) > 1000000000)){
                        // All timestamps are more than a second old, good stuff
                        std::string asset_id = std::to_string(position_iterator.first);
                        if( (abs(position_iterator.second->position[EXCHANGE_POSITION]) - abs(position_iterator.second->position[DATABASE_POSITION])) > 0.0000001){
                            logger->msg(INFO, "WARNING: Exchange reported position is different to database position for Asset: " + 
                            refdb->get_asset_code(position_iterator.first) + " (" + asset_id + ") - Exchange-position: " +
                            std::to_string(position_iterator.second->position[EXCHANGE_POSITION]) +
                            " Database position: " + std::to_string(position_iterator.second->position[DATABASE_POSITION]));
                        }
                        if( (abs(position_iterator.second->position[CALCULATED_POSITION]) - abs(position_iterator.second->position[DATABASE_POSITION])) > 0.0000001){
                            logger->msg(INFO, "WARNING: Calculated position is different to database position for Asset: " + 
                            refdb->get_asset_code(position_iterator.first)  + " (" + asset_id + ") - Calculated-position: " +
                            std::to_string(position_iterator.second->position[CALCULATED_POSITION]) +
                            " Database position: " + std::to_string(position_iterator.second->position[DATABASE_POSITION]));
                        }

                    }
            }

            sleep(118);
        }
    });       
    position_reconciliation_thread.detach();
}

// =================================================================================
fragment_handler_t RiskServer::get_aeron_msg() {
return
    [&](const AtomicBuffer &buffer, util::index_t offset, util::index_t length, const Header &header) {
        // std::string tmp = std::string(reinterpret_cast<const char *>(buffer.buffer()) + offset, static_cast<std::size_t>(length));
        char *msg_ptr = reinterpret_cast<char *>(buffer.buffer()) + offset;
        struct MessageHeader *m = (MessageHeader*)msg_ptr;

            switch(m->msgType) {
            case MSG_PARENT_ORDER: {
                struct SendParentOrder *s = (struct SendParentOrder*)msg_ptr;
                risk_new_parent_order(s);
                }
                break;

            case MSG_MODIFY_PARENT_ORDER: {
                struct ModifyParentOrder *s = (struct ModifyParentOrder*)msg_ptr;
                risk_modify_parent_order(s);
                }
                break;

            case MSG_CANCEL_PARENT_ORDER: {
                struct CancelParentOrder *s = (struct CancelParentOrder*)msg_ptr;
                risk_cancel_parent_order(s);
                }
                break;

            case MSG_NEW_ORDER: {
                struct SendOrder *s = (struct SendOrder*)msg_ptr;
                risk_new_order(s);
                if  (exchange_status_map[s->exchange_id]->is_live) {
                    uint64_t time_diff = get_current_ts_ns() - exchange_status_map[s->exchange_id]->lastHeartBeat_ts;
                    if(time_diff > 5000000000) {
                        // TODO - CHECK HOW LONG AGO A HEARTBEAT WAS FOR THIS EXCHANGE
                        // IF MORE THAN 5 SECONDS - SEND SOFT REJECT TO REQUESTOR SO THEY ARE AWARE
                    }
                }
                }
                break;

            case MSG_REQUEST_ACK: {
                struct RequestAck *r = (struct RequestAck*)msg_ptr;
                risk_request_ack(r);
                }
                break;

            case MSG_FILL: {
                struct Fill *f = (struct Fill*)msg_ptr;
                risk_fill(f);
                }
                break;
            
            case TRADING_FEE: {
                struct TradingFee *t = (struct TradingFee*)msg_ptr;
                risk_trading_fee(t);
                }
                break;

            case TOB_UPDATE: {
                struct ToBUpdate *t = (struct ToBUpdate*)msg_ptr;
                risk_tob_update(t);
                }
                break;

            case TRADE: {
                struct Trade *t = (struct Trade*)msg_ptr;
                risk_md_trade_update(t);
                }
                break;

            case HEARTBEAT_RESPONSE: {
                    struct HeartbeatResponse *h = (struct HeartbeatResponse *)msg_ptr;
                    risk_hb_update(h);
                }
                break;

            // case ROLL_OVER_REQUEST: {
            //         struct RolloverRequest *r = (struct RolloverRequest *)msg_ptr;
            //         rollover_database(r);
            //     }
            //     break;

            case ACCOUNT_INFO_UPDATE: {
                    struct AccountInfoResponse *account_info = (struct AccountInfoResponse *)msg_ptr;
                    uint64_t recv_ts = get_current_ts_ns();

                    if(account_info->account_update_reason == FUNDING_FEE){
                        // logger->msg(INFO, "Received funding_fee update - calling trading_db->AccountInfoUpdate with it");                                    
                        // trading_db->AccountInfoUpdate(recv_ts, account_info);
                    }


                    switch(account_info->account_info_type) {
                        case INSTRUMENT_POSITION:
                            // logger->msg(INFO, "Received position update for: " + refdb->get_symbol_name_from_id(account_info->instrument_id) + " (" + std::to_string(account_info->instrument_id) + ")");        
                            // Here we should update our view of what the exchange position is - we need a structure for this
                            risk_position_update(account_info);
                            break;

                        // These 2 are the same, handled within trading_db call
                        case WALLET_BALANCE:
                        case CROSS_MARGIN_BALANCE:
                            // trading_db->AccountInfoUpdate(recv_ts, account_info);
                            break;

                        default:
                            break;
                    }
                }
                break;

            case INSTRUMENT_INFO_REQUEST: {
                    struct InstrumentInfoRequest *instrument_info_request = (InstrumentInfoRequest*)msg_ptr;
                    std::vector<InstrumentInfoResponse *> instr_vector;

                    if(instrument_info_request->exchange_id == 0) {
                        // Treating this as a - check the database for any updates and the publish to all exchanges
                        if(refdb->get_all_instrument_from_db()) {
                            // We had updates in the database (new symbols or symbols set to live)
                            // For every live exchange - send all instruments
                            for (auto& exchange_iterator: exchange_status_map) {
                                if(exchange_iterator.second->is_live){
                                    instr_vector = refdb->get_all_symbols_for_exchange(exchange_iterator.first);
                                    for (size_t inst=0; inst < instr_vector.size(); inst++) {
                                        to_aeron_io->send_data((char*)instr_vector[inst], sizeof(InstrumentInfoResponse));
                                    }
                                }
                            }
                        }
                    } else {
                        // This is a normal request - just send the existing instruments for this exchange id
                        logger->msg(INFO, "Got a InstrumentInfoRequest for ExchangeID: " + std::to_string(instrument_info_request->exchange_id));
                        instr_vector = refdb->get_all_symbols_for_exchange(instrument_info_request->exchange_id);
                        logger->msg(INFO, "Found this many symbols for the exchange: " + std::to_string(instr_vector.size()));
                        for (size_t inst=0; inst < instr_vector.size(); inst++) {
                            to_aeron_io->send_data((char*)instr_vector[inst], sizeof(InstrumentInfoResponse));
                        }
                    }
                }
                break;

            case STRATEGY_INFO_REQUEST: {
                    struct StrategyInfoResponse sr;
                    sr.msg_header = MessageHeaderT{sizeof(StrategyInfoResponse), STRATEGY_INFO_RESPONSE, 0};
                    sr.number_of_strategies = strategies.size();
                    int strat_count = 0;
                    for (auto strat_k_v : strategies) {
                        sr.strategy_count               = (uint16_t)strat_count++;
                        sr.strategy_id                  = (uint8_t) strat_k_v.second->strategy_id;
                        sr.is_live                      = (bool) strat_k_v.second->is_live;
                        sr.max_order_value_USD          = (double) strat_k_v.second->max_order_value_USD;
                        sr.max_tradeconsideration_USD   = (double) strat_k_v.second->max_tradeconsideration_USD;
                        to_aeron_io->send_data((char*)&sr, sizeof(sr));
                    }
                }
                break;

            case EXCHANGE_ID_REQUEST:{
                    struct ExchangeIDRequest *e = (ExchangeIDRequest*)msg_ptr;
                    struct ExchangeIDResponse er;
                    er.msg_header = MessageHeaderT{sizeof(ExchangeIDResponse), EXCHANGE_ID_RESPONSE, 0};
                    std::string ex_name = e->exchange_name;
                    er.exchange_name[0] = '\0';
                    er.exchange_id = 0;
                    er.exchange_id_response_type = EXCHANGE_NOT_FOUND;

                    if(refdb->exchange_name_exist(ex_name)){
                        memcpy(&er.exchange_name, ex_name.c_str(), ex_name.length() + 1);
                        er.exchange_id = refdb->get_exchange_id(ex_name);
                        er.exchange_id_response_type = EXCHANGE_FOUND;
                    }
                    to_aeron_io->send_data((char*)&er, sizeof(er));
                }
                break;

            case MARGIN_TRANSFER_RESPONSE: {
                    struct MarginTransferResponse *transfer_response = (struct MarginTransferResponse *)msg_ptr;
                    uint64_t transfer_ts = get_current_ts_ns();

                    trading_db->MarginTransfer( transfer_ts,
                                                transfer_response->strategy_id,
                                                transfer_response->from_exchange_id,
                                                transfer_response->to_exchange_id,
                                                transfer_response->transferred_sum,
                                                transfer_response->asset_id,
                                                std::to_string(transfer_response->external_transfer_id));
                }
                break;

            case COMPONENT_RESTART_REQUEST: {
                    struct ComponentRestartRequest *restart_request = (struct ComponentRestartRequest *)msg_ptr;
                    risk_restart_component(restart_request);
                }
                break;

    };

    };
}


// =================================================================================
RiskServer::RiskServer(std::string app_environment) {
    log_worker = new LogWorker("svc_risk", app_environment, app_environment);
    logger = log_worker->get_new_logger("base");

    // Setup reference data
    std::function<void(struct InstrumentInfoResponse *)> add_instr_func = 
                std::bind(&RiskServer::new_instrument_callback, this, std::placeholders::_1);

    std::function<void(uint32_t instrument_id, uint32_t asset_id, double position_value)> position_update_func = 
                std::bind(&RiskServer::position_update_callback, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

    refdb = new RefDB(app_environment, log_worker->get_new_logger("refdb"));
    refdb->set_instrument_callback(add_instr_func);
    refdb->get_all_instrument_from_db();
    refdb->get_all_exchanges_from_db();
    refdb->get_all_assets_from_db();

    // Setup the trading database interface
    trading_db = new TradingDB(app_environment, log_worker->get_new_logger("tradingdb"));
    // trading_db->get_strategy_info(strategies);
    trading_db->set_position_callback(position_update_func);

    // Setup datastructures for strategies
    init_strategies();

    heartbeat_request_thread();

    // Get all positions in the database and upadte our internal structures with it
    // trading_db->get_all_future_positions();
    // trading_db->get_all_asset_positions();

    // as we are starting up - I will copy all database positions as our calculated positions as well
    // Once up and running - all updates to calculated will come from "fills"
    for (auto& position_iterator: position_map_futures) {
        position_iterator.second->position[CALCULATED_POSITION] = position_iterator.second->position[DATABASE_POSITION];
    }
    for (auto& position_iterator: position_map_assets) {
        position_iterator.second->position[CALCULATED_POSITION] = position_iterator.second->position[DATABASE_POSITION];
    }

    // position_reconciliation_thread(log_worker->get_new_logger("reconciliation"));

    to_aeron_io = new to_aeron(AERON_IO);
    io_fh = std::bind(&RiskServer::get_aeron_msg, this);     
    from_aeron_io = new from_aeron(AERON_IO, io_fh);
} 
