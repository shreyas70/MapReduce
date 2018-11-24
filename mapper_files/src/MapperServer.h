#ifndef MAPPERSERVER_H
#define MAPPERSERVER_H

#include<mutex>
#include<string>
#include<utility>
#include<vector>
#include<queue>
#define WORD_COUNT 0
#define INVERTED_INDEX 1

class Reducer
{
    private:
    std::string ip_address;
    int port_number;

    public:
    Reducer(std::string ip_address, int port_number);
    std::string get_ip_address();
    int get_port_number();
};

class JobRequest
{
    private:

    std::string job_id;
    int job_type;
    std::vector<std::pair<Reducer, std::string>> file_map;

    public:
    // JobRequest(std::string job_id, std::string job_type);
    void set_job_id(std::string job_id);
    void set_job_type(std::string job_type);
    std::string get_job_id();
    void link_file_to_reducer(Reducer r, std::string file_path);
};

class MapperServer
{
    private:

    std::string ip_address;
    int port_number;
    std::vector<Reducer> reducer_instances;
    std::mutex queue_lock;
    std::queue<JobRequest> pending_queue;
    void add_job_to_pending_queue(JobRequest jr);
    std::mutex slot_lock;
    int slots;
    bool take_slot();
    void release_slot();
    int available_slots();
    void give_heart_beats(int sock_desc);
    void process_map_request(int sock_desc);
    
    public:
    
    // MapperServer(std::string mapper_ip, std::vector<std::string> reducer_ips);
    void set_mapper_ip(std::string mapper_ip);
    void set_reducer_info(std::vector<std::string> reducer_ips);
    void start_mapper_server();


};

#endif