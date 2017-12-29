#ifndef INCLUDE_KAFKA_CONSUMER_H
#define INCLUDE_KAFKA_CONUMER_H
#include "kafka_common.h"
#include "rdkafka.h"
#include <string>

class KafkaConsumer {
 public:
  virtual ~KafkaConsumer() {
  }

  int Init(std::string brokers, std::string topic, std::string group); 
 
  int StartAll(); 
  int StartAll(int nums); 
  
  void set_partition(int partition) {
    partition_ = partition;
  }

 protected:
  virtual void ProcessMessage(const char *buf, int len);
 private:
  void ConsumerThd(int partition);

  static void Logger(const rd_kafka_t *rk,
                     int level,
                     const char *fac, 
                     const char *buf);
  int partition_{1};
  KafkaControl kafka_control_{NULL, NULL};
};
#endif
