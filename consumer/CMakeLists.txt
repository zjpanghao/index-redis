cmake_minimum_required(VERSION 2.8)
set(SRC_LIST main.cpp kafka_consumer.cpp kafka_producer.cpp)
add_definitions(-g -fPIC -std=c++11)
include_directories(/home/panghao/include/librdkafka /home/panghao/include/zookeeper /usr/local/include/librdkafka /home/panghao/include/hiredis /home/panghao/include)
#add_library(tele SHARED ${SRC_LIST})
#set_target_properties(store PROPERTIES VERSION 1.1)
LINK_DIRECTORIES(/home/panghao/libs  /home/panghao/libs/zookeeper /usr/local/lib)
add_executable(fetch_market_redis ${SRC_LIST})
target_link_libraries(fetch_market_redis  zookeeper_mt hiredis z jsoncpp ssl crypto rdkafka glog)
