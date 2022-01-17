#pragma once

#include "aeron_types.hpp"
#include "from_aeron.hpp"
#include "binary_file.hpp"
#include "InfluxDB.h"
#include "InfluxDBFactory.h"
#include "Point.h"

#include <cstdint>
#include <functional>
#include <unistd.h>
#include <unordered_map>
#include <sys/time.h>

struct ToBPrice {
    double bid_price;
    double ask_price;
};
typedef ToBPrice ToBPriceT;

enum OutputType {
    UNKNOWN_OUTPUT_TYPE = 0,
    FILE_OUTPUT_TYPE = 1,
    INFLUX_OUTPUT_TYPE = 2,
    STDOUT_OUTPUT_TYPE = 3
};


class Monitor {
    private:
        from_aeron *from_aeron_io;
        std::function<fragment_handler_t()> io_fh;

        //Default values is write nothing to stdout - all to influx
        uint8_t output_type;
        std::string out_directory;

        std::unique_ptr<influxdb::InfluxDB> influxDBio;

        std::unique_ptr<BinaryFile> bin_file_md;
        std::unique_ptr<BinaryFile> bin_file_oe;
        std::unique_ptr<BinaryFile> bin_file_risk;

        fragment_handler_t process_io_messages();

        void publish_qty_ratio(ToBUpdate *tob_update);

        std::string get_current_date_as_string(int offset);

    public:
        Monitor(uint8_t order_env, uint8_t output_type, std::string output_directory);
};