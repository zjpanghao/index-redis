#ifndef INCLUDE_REDIS_POOL_H
#define INCLUDE_REDIS_POOL_H
#include <mutex>
#include <memory>
#include <string>
#include <vector>
#include <hiredis.h>
#include <string.h>
#include <glog/logging.h>
#include "redis_cmd.h"
    class RedisControl {
     public:
      RedisControl(std::string ip, int port, std::string db, std::string password) : idle_(true) {
        struct timeval ta = {2, 0};
        context_ = (redisContext*)redisConnectWithTimeout(ip.c_str(), port, ta);
        cmd_.set_context(context_);
        Auth(password);
        Select(db);
      }
      ~RedisControl() {
        if (context_)
          redisFree(context_);
      }

      bool idle() {
        return idle_;
      }
      
      void set_idle(bool idle) {
        idle_ = idle;
      }

      bool CheckValid() {
        return cmd_.CheckValid();
      }
      
      bool SetValue(const std::string &key, const std::string &value) {
        return cmd_.SetValue(key, value);
      }

      bool SetHashValue(const std::string &hash, const std::string &key, const std::string &value) {
        return cmd_.SetHashValue(hash, key, value);
      }
      
      bool Select(std::string db) {
        return cmd_.Select(db);
      }
      bool Auth(std::string password) {
        return cmd_.Auth(password);
      }

      bool GetValue(const std::string key , std::string *value) {
        return cmd_.GetValue(key, value);
      }

      bool GetHashValue(const std::string &hash ,const std::string &key , std::string *value) {
        return cmd_.GetHashValue(hash, key, value);
      }

      time_t last_access() {
        return last_access_;
      }
      
      void set_last_access(time_t now) {
        last_access_ = now;
      }

     private:
      redisContext *context_;
      RedisCmd   cmd_;
      bool idle_;
      time_t last_access_;
};

    class RedisPool {
     public:
      RedisPool(const std::string &ip, 
                int port, 
                int normal_size, 
                int max_size,
                std::string db,
                std::string password) 
          : normal_size_(normal_size), 
            max_size_(max_size),
            ip_(ip),
            port_(port),
            db_(db),
            password_(password) {
        Init();
      }

      std::shared_ptr<RedisControl> GetControl() {
        std::lock_guard<std::mutex> lock(lock_);
        for (auto &control : context_pool_) {
          if (control->idle() && control->CheckValid()) {
            control->set_idle(false);
            return control;
          }
        } 

        if (context_pool_.size() < max_size_) {
          std::shared_ptr<RedisControl> control(new RedisControl(ip_, port_, db_, password_));
          if (control->CheckValid()) {
            control->set_idle(false);
            context_pool_.push_back(control);
            return control;
          }
        }
        return NULL;
      }

      void ReturnControl(std::shared_ptr<RedisControl> control) {
        std::lock_guard<std::mutex> lock(lock_);
        control->set_idle(true);
        control->set_last_access(time(NULL));
      }
      

      int GetActive() {
        int num = 0;
        for (auto &control : context_pool_) {
          if (!control->idle()) {
            num++;
          }
        }
        return num;
      }

      void Reep() {
        std::lock_guard<std::mutex> lock(lock_);
        int nums = context_pool_.size() - normal_size_ - GetActive();
        auto it = context_pool_.begin(); 
        time_t now = time(NULL);
        while (nums >0 && it != context_pool_.end()) {
          if ((*it)->idle() && 
              (!(*it)->CheckValid() || 
               now - (*it)->last_access() > 60 )
              ) {
            context_pool_.erase(it++);
            nums--;
          } else {
            it++;
          }
        }
      }

      void ReepThd() {
        while (1) {
          Reep();
          sleep(30);
        }
      }

      void Init() {
        for (int i = 0; i < normal_size_; i++) { 
          std::shared_ptr<RedisControl> control(new RedisControl(ip_, port_, db_, password_));
          if (control->CheckValid())
            context_pool_.push_back(control);
        }
        std::thread reap_thd(&RedisPool::ReepThd, this);
        reap_thd.detach();
      }

     private:
      std::mutex  lock_;
      std::vector<std::shared_ptr<RedisControl> > context_pool_;
      int normal_size_;
      int max_size_;
      std::string ip_;
      int port_;
      std::string db_;
      std::string password_;
    };

#endif
