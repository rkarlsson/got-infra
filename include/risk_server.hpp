#pragma once

#include <unistd.h>

#include "aeron_types.hpp"
#include "from_aeron.hpp"
#include "to_aeron.hpp"
#include "refdb.hpp"
#include "tradingdb.hpp"
#include "logger.hpp"


//*************************************************************
// Position details
//*************************************************************
enum POSITION_TYPE {
    UNKNOWN_POSITION_TYPE   = 0,
    EXCHANGE_POSITION       = 1,
    DATABASE_POSITION       = 2,
    CALCULATED_POSITION     = 3
};

struct PositionDetails {
    double position[4];
    uint64_t last_updated[4];
};

//*************************************************************
// Fill details
//*************************************************************
struct FillDetails {
  uint64_t  fill_ts;
  double    fill_qty;
  double    fill_price;
  uint32_t  order_id_filled;
  uint8_t   strategy_id;
  char      fill_id[20];
};

struct OrderDetails {
  uint64_t  order_new_ts = 0;
  uint64_t  order_finished_ts = 0;
  double    initial_qty = 0.0;
  double    leaves_qty = 0.0;
  double    limit_price = 0.0;
  uint32_t  instrument_id = 0;
  uint8_t   strategy_id = 0;
  bool      is_buy = true;
  std::vector<FillDetails *> fill_details;
};
typedef std::unordered_map<uint32_t, struct OrderDetails *> OrderDetailsMapT;

struct ReferencePrice {
  uint64_t  last_quote_update_ts = 0;
  uint64_t  last_trade_update_ts = 0;
  double    bid_price = 0.0;
  double    ask_price = 0.0;
  double    last_trade_price = 0.0;
};

struct ExchangeStatus {
  uint64_t  lastHeartBeat_ts = 0;
  bool      is_live = false;
  bool      reported_as_dead = false;
};

class RiskServer{
  private:
    LogWorker *log_worker;
    Logger *logger;

    RefDB *refdb;
    //Order maps for different types of work and views
    OrderDetailsMapT open_orders;
    OrderDetailsMapT closed_orders;
    std::unordered_map<uint8_t, OrderDetailsMapT *> open_orders_per_strategy;
    std::unordered_map<uint32_t, OrderDetailsMapT *> open_orders_per_instrument;
    std::vector<struct FillDetails *> all_fills;
    
    // ReferencePrice Map to instrumentid
    std::unordered_map<uint32_t, struct ReferencePrice *> ref_price_map;

    // Exchange details
    std::unordered_map<uint8_t, struct ExchangeStatus *> exchange_status_map;

    // Position details - used for reconciliation between exchange/tradingdb and whats seen on aeron
    std::unordered_map<uint32_t, struct PositionDetails *> position_map_futures;
    std::unordered_map<uint32_t, struct PositionDetails *> position_map_assets;

    // StrategyDetails
    StrategyDetailsT strategies;

    // Trading Database interface
    TradingDB *trading_db;

    // Consume and handle all messages from aeron
    fragment_handler_t get_aeron_msg();
    from_aeron *from_aeron_io;
    std::function<fragment_handler_t()> io_fh;
    to_aeron *to_aeron_io;

    // Methods to init and work on internal datastructures
    void init_strategies();
    void close_order_in_datastructure(struct RequestAck *request_ack);
    void add_order_to_datastructure(struct SendOrder *new_order, uint64_t time_stamp);
    void recalculate_risk(uint32_t internal_order_id, uint32_t instrument_id, bool order_removed);

    // Helpers
    bool is_exchange_live(uint8_t exchange_id);
    bool is_strategy_live(uint8_t strategy_id);
    uint64_t get_current_ts_ns();

    // Mostly database mgmt
    void risk_new_order(struct SendOrder *new_order);
    void risk_new_parent_order(struct SendParentOrder *new_parent_order);
    void risk_modify_parent_order(struct ModifyParentOrder *modify_parent_order);
    void risk_cancel_parent_order(struct CancelParentOrder *cancel_parent_order);
    void risk_request_ack(struct RequestAck *request_ack);
    void risk_fill(struct Fill *fill_details);
    void risk_trading_fee(struct TradingFee *fee_details);
    void risk_tob_update(struct ToBUpdate *tob_update);
    void risk_md_trade_update(struct Trade *trade_update);
    void risk_hb_update(struct HeartbeatResponse *hb_update);
    void risk_position_update(struct AccountInfoResponse *acct_update);
    void risk_restart_component(ComponentRestartRequest *restart_request);
        
    // Rolls the database
    // void rollover_database(struct RolloverRequest *rollover_request);

    // This one gets called everytime refdb finds a new symbol. Initialise internal datastructures where needed
    void new_instrument_callback(struct InstrumentInfoResponse *);
    
    // This one gets called everytime tradingdb finds a new position change in the database
    void position_update_callback(uint32_t instrument_id, uint32_t asset_id, double position_value);

    void add_instrument_to_position_map_futures(uint32_t instrument_id);
    void add_asset_to_position_map_assets(uint32_t asset_id);

    // Get all exchange risk managers to get the latest positions and declare them on the risk bus
    // This is useful to request periodically for our reconciliation process
    void request_live_exchange_positions();

  public:
    // Constructor
    RiskServer(std::string app_environment);

    // Heartbeat request thread
    void heartbeat_request_thread();

    void position_reconciliation_thread(Logger *logger);

};
