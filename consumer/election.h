#ifndef ELECTION_H
#define ELECTION_H
#include <string>
class Zookeeper {
 public:
   Zookeeper(std::string servers, int timeout) :
       servers_(servers),
       timeout_(timeout) {
   }
   
   bool Init() {
     handle_  = zookeeper_init(servers_.c_str(), NULL , timeout_, 0, 0, 0);
     return handle_ != NULL;
   }

   bool Election(const std::string &node_name) {
     char host_name[64];
     gethostname(host_name,sizeof(host_name));
     if (!handle_)
       return false;
     int ret = zoo_create(handle_, node_name.c_str(), host_name, strlen(host_name),
	     &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, NULL, 0);
     return ret == 0;
   }

  private:
   zhandle_t *handle_;
   std::string servers_;
   int timeout_;
};

class ElectionControl {
 public:
  ElectionControl() : keeper_(NULL) {
  }

  bool Init(const char *servers, int timeout) {
    keeper_ = new Zookeeper(servers, timeout);
    return keeper_->Init();
  }

  bool Election(const std::string &name) {
    return keeper_->Election(name);
  }

 private:
  Zookeeper  *keeper_;
};
#endif
