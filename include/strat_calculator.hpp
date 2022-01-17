#pragma once

#include <iostream>
#include <cstdint>
#include "aeron_types.hpp"
#include <vector>
#include <map>


struct OrderDetails {
    double  init_qty;
    double  leave_qty;
    double  price;
    bool    is_buy;
};

typedef std::map<uint64_t, struct OrderDetails *> OrderInfoMap;
typedef std::map<uint8_t, double> ExecutionFeeMap;
typedef std::map<uint32_t, double> InstrumentValueMap;


class StratCalculator {
    private:
        ExecutionFeeMap exec_fees = {   {(uint8_t)16, (double)(0.075/100)}, // Binance spot
                                        {(uint8_t)17, (double) (0.02/100)}}; // Binance futures
        double total_execution_fees = 0.0;
        InstrumentValueMap execution_fee_value_map;

        std::map<uint32_t, double> executed_qty_map;
        
        std::map<uint32_t, double> open_order_qty_map;

        InstrumentValueMap mid_price_map;
        std::map<uint32_t, double> position_value_map;

        double PnL = 0.0;
        OrderInfoMap order_info;

    public:
        StratCalculator();
        void add_order(struct SendOrder *order);
        void add_fill(struct Fill *fill);
        void add_cancel(struct CancelOrder *cancel);
        void update_price(struct ToBUpdate *tob);
        double get_pnl();
        std::map<uint32_t, double> *get_executed_qty();
        double get_executed_qty(uint32_t instrument_id);
        double get_execution_fee();
        std::map<uint32_t, double> *get_open_qty();
        std::map<uint32_t, double> *get_positions();
        double get_position(uint32_t instrument_id);
        bool is_fill_buy(Fill *fill);
};
