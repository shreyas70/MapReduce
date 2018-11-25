#ifndef MAPPERSERVER_H
#define MAPPERSERVER_H

#include<mutex>
#include<string>
#include<utility>
#include<vector>
#include<list>
#include<queue>
#define WORD_COUNT 0
#define INVERTED_INDEX 1

#include "DummyReducerClient.h"

class JobRequest
{
    private:

    std::string job_id;
    int job_type;
    std::vector<std::pair<DummyReducerClient, std::string>> file_map;

    public:
    // JobRequest(std::string job_id, std::string job_type);
    void set_job_id(std::string job_id);
    void set_job_type(std::string job_type);
    std::string get_job_id();
    int get_job_type();
    void link_file_to_reducer(DummyReducerClient r, std::string file_path);
    std::vector<std::pair<DummyReducerClient, std::string>> get_file_map();
};

class MapperServer
{
    private:

    std::string ip_address;
    int port_number;
    std::vector<DummyReducerClient> reducer_instances;
    std::mutex queue_lock;
    int pending_queue_size;
    std::queue<JobRequest *> * pending_queue;
    void add_job_to_pending_queue(JobRequest * jr);
    JobRequest * get_next_job();
    bool is_pending_queue_empty();
    std::mutex slot_lock;
    int slots;
    bool take_slot();
    void release_slot();
    int available_slots();
    void give_heart_beats(int sock_desc);
    void process_map_request(int sock_desc);
    void dispatch();
    void receive_heart_beats(DummyReducerClient r);
    
    public:
    
    // MapperServer(std::string mapper_ip, std::vector<std::string> reducer_ips);
    void set_mapper_ip(std::string mapper_ip);
    void set_reducer_info(std::vector<std::string> reducer_ips);
    void start_mapper_server();


};

#endif