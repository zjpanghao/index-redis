#include <pthread.h>
#include <map>
#include <memory>
#include <glog/logging.h>
#include "kafka_consumer.h"
#include "kafka_producer.h"
#include <set>
#include <string>
#include <iostream>
#include <thread>
#include <future>
#include "redis_pool.h"
#include "zookeeper/zookeeper.h"
#include "election.h"
// #include "zookeeper/zookeeper_log.h"

static void InitGlog(const char *name) {
  google::InitGoogleLogging(name);
  google::SetLogDestination(google::INFO,"log/teleloginfo");
  google::SetLogDestination(google::WARNING,"log/telelogwarn");
  google::SetLogDestination(google::GLOG_ERROR,"log/telelogerror");
  FLAGS_logbufsecs = 0;
}
static int count = 0;
std::set<std::string>  code_set; 
 KafkaProducer *producer_g;
class MarketKafkaConsumer : public KafkaConsumer {
  struct InfoData{
    std::string now;
  };
  public:
   MarketKafkaConsumer(RedisPool *pool) : pool_(pool) {

   }
   

   bool Split(const std::string &s, std::string delim, std::vector<std::string> *elems) { 
    int old_pos = 0;   
    int pos = 0;   
    std::vector<std::string> origin_str;  
    while (true) {     
      pos = s.find(delim, old_pos);     
      if (pos == -1)       
        break;     
      int len = pos - old_pos;     
      if (len) {       
        elems->push_back(s.substr(old_pos, len));     
      } else {       
        return false;     
      }     
      old_pos = pos + delim.length();   
    }   
    int len = static_cast<int>(s.size()) - old_pos;   
    if (len > 0) {     
      elems->push_back(s.substr(old_pos, len));  
    }   
    return true; 
  }

  std::string GetStr(const std::vector<std::string> &elems) {
       std::string tmp;
       for (auto &info : elems) {
         tmp += info;
         tmp += " ";
       }
       return tmp;
  }

   bool ParseInfoData(const std::string &info_str, InfoData *data) {
     std::vector<std::string> elems;
     Split(info_str, " ", &elems);
     if (elems.size() < 12)
       return false;
     data->now = elems[2];
     return true;
   }


   void ProcessMessage(const char *buf, int len) override{
     static const  std::string  hash = "quant";
     count_++;
     if (len < 100) {
       return;
     }
     if (!pool_)
       return;
     std::string stock_info(buf, len);
     std::vector<std::string> infos;
     Split(stock_info, "\n", &infos);
     std::shared_ptr<RedisControl> control = pool_->GetControl();
     if (control == NULL) {
       LOG(INFO) << "null redis ptr" << std::endl;
       return;
     }
     static const std::set<std::string> index_codes{"000300.SH", "000001.SH", "399001.SZ", "399006.SZ"};
   
     for (auto &info : infos) {
       if (info.length() < 100)
         continue;
       std::vector<std::string> elems;
       
       Split(info, " ", &elems);
       if (elems.size() < 12)
         continue;
       std::string code = elems[0];
       std::string time = elems[1];
       std::string now = elems[2];
       if (time.length() < 5) {
         continue;
       }

       int time_int = atoi(time.c_str());
       bool r = index_codes.count(code) && time_int >= 92500;
       if (!r)
         continue; 
       LOG(INFO) << code << " " << info; 
       std::string key = "index:" + code;
       int min = time_int / 100;
       if (min >= 925 && time_int <= 930) {
         std::string old_info;
         control->GetHashValue(hash, key, &old_info);
         struct InfoData data;
         r = ParseInfoData(old_info, &data); 
         if (r && data.now == now) {
           continue;
         }
       }
       control->SetHashValue(hash, key, info);
     }
     pool_->ReturnControl(control);
   }

  private:
   RedisPool *pool_;
   int count_{0};
};

int start_consumer(const char *servers, RedisPool *pool, int nums) {
  MarketKafkaConsumer *consumer = new MarketKafkaConsumer(pool);
  int rc = consumer->Init(servers, "east_wealth", "group_test_count_113");
  if (rc < 0)
    return rc;
  consumer->set_partition(1);
  if(0 != (rc = consumer->StartAll(nums)))
    return -1;
  return 0;
}

int start_consumer(const char *servers, const std::string &group, RedisPool *pool) {
  MarketKafkaConsumer *consumer = new MarketKafkaConsumer(pool);
  int rc = consumer->Init(servers, "east_wealth", group.c_str());
  if (rc < 0)
    return rc;
  consumer->set_partition(1);
  if(0 != (rc = consumer->StartAll()))
    return -1;
  return 0;
}

void producer_simulate_task(KafkaProducer *producer) {
  LOG(INFO) << "PT start";
  std::vector<std::string> values(50, "100000");
  values[0] = "000001.SZ";
  auto GetStr = [](const std::vector<std::string> &values) {
    std::string tmp;
    for (auto &value : values) {
      tmp += value;
      tmp += " ";
    }
    return tmp;
  };
  while (1) {
    time_t now = time(NULL);
    struct tm current;
    localtime_r(&now, &current);
    char buf[64];
    snprintf(buf, sizeof(buf), "%d%02d%02d", current.tm_hour, current.tm_min, current.tm_sec);
    values[1] = buf;
    std::string send_value = GetStr(values);
    // LOG(INFO) << send_value;
    producer->Send((char*)send_value.c_str(), send_value.length(), 0);
    sleep(5);
  }

}

int start_producer(const char *servers) {
  producer_g = new KafkaProducer();
  int rc = producer_g->Init(servers, "east_wealth", "east_wealth_count");
  if (rc < 0) {
    return rc;
  }
  
  std::thread  t(producer_simulate_task, producer_g);
  t.detach();
  return 0; 
}

int main(int argc, char*argv[]) {
  InitGlog(argv[0]);
  std::string group = "kafka_group";
  if (argc >= 2) {
    group = argv[1];
  }
#if 1
  ElectionControl election;
  const char *server = "192.168.1.74:2181";
  if (election.Init(server, 2000) == false) {
    LOG(ERROR) << "Init election failed!";
    return -1;
  }
  const std::string node_name = "/quant_index_redis";
  while (election.Election(node_name) == false) {
    LOG(INFO) << "SLEEPING";
    sleep(10);
  }

  LOG(INFO) << ("Election OK");
#endif
  RedisPool pool("192.168.1.72", 7481, 10, 20, "3", "ky_161019");
  const char *servers = {"192.168.1.74:9092"};
  LOG(INFO) << servers;
#if 0
  if (start_producer(servers) < 0) {
    LOG(ERROR) << "start producer error";
    return -1;
  }
#endif
  if (start_consumer(servers, group, &pool) < 0) {
    LOG(ERROR) << "start consumer error";
    return -1;
  }
  while (1) {
    sleep(10000);
  }
  return 0;
}
