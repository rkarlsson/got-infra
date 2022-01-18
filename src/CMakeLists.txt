add_library(filewriter STATIC "" file_writer.cpp)
add_library(simdjson STATIC "" simdjson.cpp)
add_library(binancemd STATIC "" binance_md_process.cpp)
add_library(tardismd STATIC "" tardis_processor.cpp)
add_library(rawfile STATIC "" raw_file.cpp)
add_library(wsock2 STATIC "" wsock2.cpp)
target_link_libraries(wsock2 filewriter)
# target_compile_options(wsock2 -g)
add_library(wsock STATIC "" wsock.cpp)
target_link_libraries(wsock filewriter mergedorderbook)

# SVC_MD_BINANCE - covers all binance right now use the new websocket library
###################################################
add_executable(svc_md_binance svc_md_binance.cpp )
target_link_libraries(svc_md_binance 
    binancemd 
    refdb 
    aeron_library 
    simdjson 
    filewriter 
    binfile 
    logger 
    wsock 
    gzlib 
    connector 
    wolfssl
    ${EXTERNAL_LIBRARIES}
)
# target_compile_options(svc_md_binance PUBLIC -g)

# SVC_MD_KRAKEN - covers all kraken right now use the new websocket library
###################################################
add_executable(svc_md_kraken svc_md_kraken.cpp kraken_md_process.cpp )
target_link_libraries(svc_md_kraken refdb aeron_library simdjson filewriter logger wsock2 connector wolfssl ${EXTERNAL_LIBRARIES})
# target_compile_options(svc_md_kraken PUBLIC -g)

# SVC_OE_BINANCE - Order entry trade-adapter for all binance markets
###################################################
add_executable(svc_oe_binance svc_oe_binance.cpp binance_trade_adapter.cpp base_trade_adapter.cpp )
target_link_libraries(svc_oe_binance aeron_library simdjson config_db logger wsock connector wolfssl ${EXTERNAL_LIBRARIES})
# target_compile_options(svc_oe_binance PUBLIC -g)

# SVC_MONITOR - Listens to all messages and writes to influx/stdout/binary file
###################################################
add_executable(svc_monitor svc_monitor.cpp monitor.cpp )
target_link_libraries(svc_monitor aeron_library binfile gzlib InfluxDB ${EXTERNAL_LIBRARIES})

# SVC_MON_PROMETHEUS - Listens to all messages and provides data for prometheus to query
###################################################
if(PROMETHEUS_CPP_ENABLE_PUSH)
    add_executable(svc_mon_prometheus svc_mon_prometheus.cpp new_monitor.cpp )
    target_link_libraries(svc_mon_prometheus aeron_library binfile gzlib prometheus-cpp::push ${EXTERNAL_LIBRARIES})
endif()

if(PROMETHEUS_CPP_ENABLE_PULL)
    add_executable(svc_mon_prometheus svc_mon_prometheus.cpp new_monitor.cpp )
    target_link_libraries(svc_mon_prometheus aeron_library binfile gzlib prometheus-cpp::pull ${EXTERNAL_LIBRARIES})
endif()


# SVC_RISK - Risk management
###################################################
add_executable(svc_risk svc_risk.cpp risk_server.cpp )
target_link_directories(svc_risk 
    PUBLIC submodules/mysql-connector-cpp/
    PUBLIC submodules/aeron/lib 
    PUBLIC lib 
    PUBLIC submodules/wolfssl/build
)
target_link_libraries(svc_risk aeron_library refdb trading_db logger connector ${EXTERNAL_LIBRARIES})
# target_compile_options(svc_risk PUBLIC -g)  # same as PRIVATE + INTERFACE

# SEND_OE_MSG - Order sender
###################################################
add_executable(send_oe_msg send_oe_msg.cpp)
target_link_libraries(send_oe_msg aeron_library aeron_client ${PTHREAD_LIB})
# target_compile_options(send_oe_msg PUBLIC -g)  # same as PRIVATE + INTERFACE

# BINFILE_READER - Can read the binary files created by svc_monitor
###################################################
add_executable(binfile_reader binfile_reader.cpp)
target_link_libraries(binfile_reader aeron_library binfile gzlib ${Z_LIB})

# BIN_TO_INFLUX_LOADER - Loads messages from binfiles in sequence and loads to temporary influx database
###################################################
add_executable(bin_to_influx_loader bin_to_influx_loader.cpp strat_calculator.cpp)
target_link_libraries(bin_to_influx_loader aeron_library binfile sequencebinfile gzlib ${Z_LIB} InfluxDB)

# BINFILE_MERGER - Reads arbitrary number of binfiles and merges them into a single output file
###################################################
add_executable(binfile_merger binfile_merger.cpp)
target_link_libraries(binfile_merger aeron_library binfile sequencebinfile gzlib ${Z_LIB})

# CONVERT_MD_BINANCE - Reads JSON files and writes binary files
###################################################
add_executable(convert_md_binance convert_md_binance.cpp)
target_link_libraries(convert_md_binance aeron_library binancemd simdjson binfile rawfile gzlib mergedorderbook ${Z_LIB})

# MD_CLIENT - consumes data from aeron and checks that we have no gaps. Able to snapshot depth
###################################################
add_executable(md_client md_client.cpp)
target_link_libraries(md_client aeron_library mergedorderbook aeron_client ${PTHREAD_LIB} )

# CONVERT_MD_TARDIS - Reads TARDIS CSV files and outputs vinary files
###################################################
add_executable(convert_md_tardis convert_md_tardis.cpp)
target_link_libraries(convert_md_tardis aeron_library binfile rawfile gzlib tardismd ${Z_LIB} )

# Orderbook tester - Reads arbitrary number of binfiles and keeps track on orderbook updates and can print orderbook details in output file
###################################################
add_executable(orderbook_tester orderbook_tester.cpp)
target_link_libraries(orderbook_tester aeron_library binfile sequencebinfile mergedorderbook gzlib ${Z_LIB})

# SVC_SIG_GENERATOR - Signal generator
###################################################
# add_executable(svc_sig_generator svc_sig_generator.cpp test_signal.cpp base_signal.cpp trade_signal.cpp)
# target_link_libraries(svc_sig_generator aeron_library binfile sequencebinfile moving_average gzlib ${Z_LIB} ${EXTERNAL_LIBRARIES})