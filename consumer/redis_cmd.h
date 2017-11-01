#ifndef INCLUDE_REDIS_CMD_H
#define INCLUDE_REDIS_CMD_H
class RedisCmd  {
 public:
  RedisCmd() {
    context_ = NULL;
  }
  
  void set_context(redisContext *context) {
    context_ = context;
  }

  bool Select(std::string db) {
    redisReply* reply;
    bool r = false;
    reply = (redisReply*)redisCommand(context_,"SELECT %s", db.c_str());
    if(reply == NULL) {
      return false;
    }
    return true;
  }

  bool Auth(std::string password) {
    redisReply* reply;
    bool r = false;
    reply = (redisReply*)redisCommand(context_,"AUTH %s", password.c_str());
    if(reply == NULL) {
      return false;
    }
    if (reply->str && strncmp(reply->str, "OK", 2) == 0) {
      return true;
    }
    return false;
  }

  bool CheckValid() {
    bool rc = false;
    if (!context_) {
      return rc;
    }
    redisReply* reply;
    reply = (redisReply*)redisCommand(context_,"PING");
    if(reply == NULL) {
      return rc;
    }
    if(strncmp(reply->str,"PONG", 3) == 0){
      rc = true;
    }
    freeReplyObject(reply);
    return rc;
  }

  bool GetValue(const std::string key , std::string *value) {
        redisReply* reply;
        bool r = false;
        reply = (redisReply*)redisCommand(context_,"GET %s", key.c_str());
        if(reply == NULL) {
          return r;
        }
        r = true;
        if (reply->len)
          value->assign(reply->str, reply->len);
        freeReplyObject(reply);
        return r;
 }

  bool GetHashValue(const std::string &hash, const std::string &key, std::string *value) {
        redisReply* reply;
        bool r = false;
        reply = (redisReply*)redisCommand(context_,"HGET %s %s", hash.c_str(), key.c_str());
        if(reply == NULL) {
          return r;
        }
        LOG(INFO) << "hget " << hash << " " << key; 
        r = true;
        if (reply->str) {
          LOG(INFO) << reply->str;
          if (reply->len)
            value->assign(reply->str, reply->len);
        }
        freeReplyObject(reply);
        return r;
}
      
 bool SetValue(const std::string &key, const std::string &value) {
        redisReply* reply;
        bool r = false;
        reply = (redisReply*)redisCommand(context_,"SET %s %s", key.c_str(), value.c_str());
        if(reply == NULL) {
          return r;
        }
        LOG(INFO) << "_____set " << key << " " << value; 
        if (strncmp(reply->str, "OK", 2) == 0)
          r = true;
        freeReplyObject(reply);
        return r;
 }

 bool SetHashValue(const std::string &hash, const std::string &key, const std::string &value) {
        redisReply* reply;
        bool r = false;
        reply = (redisReply*)redisCommand(context_,"HSET %s %s %s", hash.c_str(), key.c_str(), value.c_str());
        if(reply == NULL) {
          return r;
        }
        LOG(INFO) << "hset " << hash << " " << key << " " << value; 
        if (reply->str) {
          LOG(INFO) << reply->str;
          if (strncmp(reply->str, "OK", 2) == 0)
            r = true;
          else
            LOG(ERROR) << reply->str;
          LOG(INFO) << "SET OK";
        }
        freeReplyObject(reply);
        LOG(INFO) << "SET OK";
        return r;
}

 private:
  redisContext *context_;
};
#endif
