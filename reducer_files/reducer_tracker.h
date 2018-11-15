#ifndef _REDUCER_SERVER_H_
#define _REDUCER_SERVER_H_

#include <string>
#include <map>
#include <set>

class ReducerTracker
{
    std::map<int, std::set<std::string>> job_files_map;

public:
        
    int port;
    int sock;
    std::string ip_addr;
    std::string log_path;

    ReducerTracker(): ip_addr(""), port(-1), sock(-1), log_path("") {}

    void log_print(std::string msg);
    void job_file_add(int job_id, std::string file_path);
    int num_job_files(int job_id);
    std::set<std::string>& job_files_get(int job_id);
};


#endif
