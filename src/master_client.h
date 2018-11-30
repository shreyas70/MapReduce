#ifndef _MASTER_CLIENT_H_
#define _MASTER_CLIENT_H_

#include <string>
#include <map>
#include <set>
#include <vector>
#include "utilities.h"


class MasterClient
{
    int m_sock;

public:
    int m_port;
    std::string m_ip_addr;

    MasterClient(): m_sock (FAILURE), m_ip_addr("") {}

    int sock_get() { return m_sock; }
    bool connection_exists() { return m_sock != FAILURE; }

    int connect_as_mapper(std::string ip_address, int port_number);
    int connect_as_reducer(std::string ip_address, int port_number);
    int connect_as_client(std::string ip_address, int port_number, Problem problem, std::string file_path);

    int get_request(std::string&);
    void job_completed(int job_id, int chunk_id, std::vector<std::string> reducer_files);
    //void job_completed_reducer(std::string job_id, int category, std::string output_file);

    void job_failure_mapper(int job_id, int chunk_id);
    void job_failure_reducer(int job_id, int category);
};

#endif
