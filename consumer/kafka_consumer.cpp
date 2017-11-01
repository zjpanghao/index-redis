#include "kafka_consumer.h"
#include "sys/time.h"
#include "error.h"
#include "rdkafka.h"
#include <glog/logging.h>
#include <thread>

int KafkaConsumer::Init(std::string brokers, std::string topic, std::string group) {
    char errstr[512];
    rd_kafka_topic_conf_t *topic_conf;
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set_log_cb(conf, Logger);
    if (RD_KAFKA_CONF_OK != rd_kafka_conf_set(conf, "group.id", group.c_str(), NULL, 0))
      return -2;
    rd_kafka_conf_set(conf, "queued.min.messages", "1000000", NULL, 0);	
    /* Create Kafka handle */
    if (!(kafka_control_.control = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
	                    errstr, sizeof(errstr)))) {
      fprintf(stderr, "%% Failed to create new consumer: %s\n",
				errstr);
      return -1;
    }
    /* Add brokers */
    if (rd_kafka_brokers_add(kafka_control_.control, brokers.c_str()) == 0) {
      return -2;
    }
    topic_conf = rd_kafka_topic_conf_new();
    /* Create topic */
    kafka_control_.topic = rd_kafka_topic_new(kafka_control_.control, topic.c_str(), topic_conf);
    topic_conf = NULL; /* Now owned by topic */
    rd_kafka_dump(stdout, kafka_control_.control);
    return 0;    
}  
 
int KafkaConsumer::StartAll(int nums) {
    /* Start consuming */
  for (int partition = 0; partition < partition_; partition++) {
    if (rd_kafka_consume_start(kafka_control_.topic, partition, -2000 - nums/*RD_KAFKA_OFFSET_END*/) == -1){
      fprintf(stderr, "%% Failed to start consuming: %s\n",
      rd_kafka_err2str(rd_kafka_errno2err(errno)));
      return -1;
    }
    std::thread consumer(&KafkaConsumer::ConsumerThd, this, partition);
    consumer.detach();
  }
  return 0;
}

int KafkaConsumer::StartAll() {
    /* Start consuming */
  for (int partition = 0; partition < partition_; partition++) {
    if (rd_kafka_consume_start(kafka_control_.topic, partition, RD_KAFKA_OFFSET_END) == -1){
      fprintf(stderr, "%% Failed to start consuming: %s\n",
      rd_kafka_err2str(rd_kafka_errno2err(errno)));
      return -1;
    }
    std::thread consumer(&KafkaConsumer::ConsumerThd, this, partition);
    consumer.detach();
  }
  return 0;
}

void KafkaConsumer::ConsumerThd(int partition) {
  KafkaControl *kafka_control = &kafka_control_;
  rd_kafka_topic_t *topic = kafka_control->topic;
  rd_kafka_t *control = kafka_control->control;
  while (true) {
    rd_kafka_poll(control, 0);
    rd_kafka_message_t *rkmessage;
    rkmessage = rd_kafka_consume(topic, partition, 1000);
    if (!rkmessage) {
      // printf("timeout\n");
      continue;
    }
    if (rkmessage->err) {
      if (rkmessage->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
        LOG(ERROR) << rkmessage->err;
      //  rd_kafka_message_destroy(rkmessage);
	continue;
    }

    ProcessMessage((const char*)rkmessage->payload, (int)rkmessage->len);
    rd_kafka_message_destroy(rkmessage);
  }
}

void KafkaConsumer::ProcessMessage(const char*value, int len) {

}

void KafkaConsumer::Logger(const rd_kafka_t *rk,
                     int level,
                     const char *fac, 
                     const char *buf) {
struct timeval tv;
	gettimeofday(&tv, NULL);
	fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
		(int)tv.tv_sec, (int)(tv.tv_usec / 1000),
		level, fac, rk ? rd_kafka_name(rk) : NULL, buf);
}
