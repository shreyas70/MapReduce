#ifndef MAPPERSERVER_H
#define MAPPERSERVER_H

#include<mutex>

class MapperServer
{
    private:

    // std::atomic<int> slots;
    // std::atomic<bool> slot_var_locked;
    std::mutex slot_lock;
    int slots;
    bool take_slot();
    void release_slot();
    void process_map_request(int sock_desc);
    
    public:
    
    void start_mapper_server(int port_number);


};

#endif