#include "strat_calculator.hpp"

StratCalculator::StratCalculator() {
   
}

void StratCalculator::add_order(struct SendOrder *order) {
    auto new_order_details = new OrderDetails();
    new_order_details->is_buy = order->is_buy;
    new_order_details->init_qty = order->qty;
    new_order_details->leave_qty = order->qty;
    new_order_details->price = order->price;
    order_info[order->internal_order_id] = new_order_details;

    if(open_order_qty_map.count(order->instrument_id)){
        open_order_qty_map[order->instrument_id] += order->qty; 
    } else {
        open_order_qty_map[order->instrument_id] = order->qty;
    }
}

void StratCalculator::add_fill(struct Fill *fill) {
    if(order_info.count(fill->internal_order_id)){
        // Update "leaves qty" for order (decrease by fill_qty)
        order_info[fill->internal_order_id]->leave_qty -= fill->fill_qty;

        // Update open ordervalue (decrease with filled amount)
        double exec_value = fill->fill_qty * fill->fill_price;
        if(open_order_qty_map.count(fill->instrument_id)){
            open_order_qty_map[fill->instrument_id] -= fill->fill_qty; 
        }

        // Update execution fees
        double fee = (exec_fees[fill->exchange_id] * fill->fill_qty * fill->fill_price);
        total_execution_fees += fee;
        if(execution_fee_value_map.count(fill->instrument_id)){
            execution_fee_value_map[fill->instrument_id] += fee;
        } else {
            execution_fee_value_map[fill->instrument_id] = fee;
        }

        // Update executed value (total and per instrument)
        if(executed_qty_map.count(fill->instrument_id)){
            executed_qty_map[fill->instrument_id] += fill->fill_qty;
        } else {
            executed_qty_map[fill->instrument_id] = fill->fill_qty;
        }

        // Update PnL - Reverse as PnL is more seen as the "cash" account as well
        if(order_info[fill->internal_order_id]->is_buy){
            PnL -= exec_value;
        } else {
            PnL += exec_value;
        }

        // Update position
        if(position_value_map.count(fill->instrument_id) == 0){
            position_value_map[fill->instrument_id] = 0.0;
        }
        if(order_info[fill->internal_order_id]->is_buy){
            position_value_map[fill->instrument_id] += fill->fill_qty;
        } else {
            position_value_map[fill->instrument_id] -= fill->fill_qty;
        }

        // Do this last so I keep all orderdetails if needed in any above
        if(order_info[fill->internal_order_id]->leave_qty <= 0.0){
            // leaves qty has gone to 0 - we should remove the order from the map
            order_info.erase(fill->internal_order_id);
        }

    } else {
        // No order for the fill, something has gone terribly wrong
        std::cout << "Found no order for fill, stopping.." << std::endl;
        exit(0);
    }
}

void StratCalculator::add_cancel(struct CancelOrder *cancel) {
    if(order_info.count(cancel->internal_order_id)){
        // Update open ordervalue (decrease with leaves qty)
        if(open_order_qty_map.count(cancel->instrument_id)){
            open_order_qty_map[cancel->instrument_id] -= order_info[cancel->internal_order_id]->leave_qty;
        }
        order_info.erase(cancel->internal_order_id);
    }
}

bool StratCalculator::is_fill_buy(struct Fill *fill) {
    return(order_info[fill->internal_order_id]->is_buy);
}

void StratCalculator::update_price(struct ToBUpdate *tob){
    mid_price_map[tob->instrument_id] = (tob->ask_price + tob->bid_price)/2;
}


// Here I need to calculate the position * current_price and add to the curr_PnL
// Problem is I don't have the curr_price. Instead I'll provide access to get
// current position for all instruments via another call and then external app can
// calculate the "final" PnL.
double StratCalculator::get_pnl() {
    double curr_PnL = PnL - total_execution_fees;
    
    // Loop over all positions and multiply with mid price
    for (const auto& [key, value] : position_value_map) {
        curr_PnL += value * mid_price_map[key];
    }
    return(curr_PnL);
}

double StratCalculator::get_execution_fee() {
    return(total_execution_fees);
}

std::map<uint32_t, double> *StratCalculator::get_open_qty() {
    return(&open_order_qty_map);
}

std::map<uint32_t, double> *StratCalculator::get_executed_qty() {
    return(&executed_qty_map);
}

double StratCalculator::get_executed_qty(uint32_t instrument_id) {
    return(executed_qty_map[instrument_id]);
}

std::map<uint32_t, double> *StratCalculator::get_positions() {
    return(&position_value_map);
}

double StratCalculator::get_position(uint32_t instrument_id) {
    return(position_value_map[instrument_id]);
}
