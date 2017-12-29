cmake_minimum_required(VERSION 2.8)
set(SRC_LIST main.cpp kafka_consumer.cpp kafka_producer.cpp)
add_definitions(-g -fPIC -std=c++11)
set(KUNLIB /home/panghao/kunlib)
include_directories(${KUNLIB}/include/librdkafka 
${KUNLIB}/include/zookeeper  
${KUNLIB}/include/hiredis 
${KUNLIB}/include)
#add_library(tele SHARED ${SRC_LIST})
#set_target_properties(store PROPERTIES VERSION 1.1)
LINK_DIRECTORIES(${KUNLIB}/libs  ${KUNLIB}/libs/zookeeper)
add_executable(fetch_market_redis ${SRC_LIST})
target_link_libraries(fetch_market_redis  zookeeper_mt hiredis z  ssl crypto rdkafka glog)
