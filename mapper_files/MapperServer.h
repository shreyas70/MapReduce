#ifndef MAPPERSERVER_H
#define MAPPERSERVER_H

#include<mutex>

class MapperServer
{
    private:

    std::mutex slot_lock;
    int slots;
    bool take_slot();
    void release_slot();
    int available_slots();
    void give_heart_beats(int sock_desc);
    void process_map_request(int sock_desc);
    
    public:
    
    void start_mapper_server(int port_number);


};

#endif