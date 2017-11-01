#ifndef KAFKA_CONTROL_H
#define KAFKA_CONTROL_H
#include "rdkafka.h"
struct KafkaControl {
  rd_kafka_t *control;
  rd_kafka_topic_t *topic;
};
#endif
