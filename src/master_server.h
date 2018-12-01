#ifndef _MASTER_SERVER_H_
#define _MASTER_SERVER_H_

#include <string>
#include <map>
#include <set>
#include <vector>

#include "utilities.h"
#include <sys/socket.h> 
#include <netinet/in.h> 
#include <arpa/inet.h>
#include <vector>

#include "Mapper.h"
#include "Reducer.h"

#define MAX_CONNECTIONS 100

struct Chunk
{
    int chunk_id;
    int job_id;
    int mapper_sock;
    std::vector<int> start_line_vec;
    std::vector<int> num_lines_vec;

    Chunk (int id, int jid, int msock):
        chunk_id(id), job_id(jid), mapper_sock(msock) {}
};


struct Job
{
    static int job_count;
    int job_id;
    Problem problem_id;
    int client_socket;
    int num_mappers; //initital number of mappers
    int num_reducers; //initial number of reducers
    int num_successful_reductions;
    std::vector<std::string> input_filenames;
    std::vector<Reducer*> reducer_of_category;
    Chunk** chunks;
    std::vector<std::vector<std::string>> category_files;

    Job(int sock, int nmappers, int nreducers):
        client_socket(sock), num_mappers(nmappers), num_reducers(nreducers)
    {
        job_id = Job::GenerateJobId();
        for (int i = 0; i < num_reducers; ++i)
            category_files.push_back(std::vector<std::string>());

        num_successful_reductions = 0;
        chunks = new Chunk*[num_mappers];
    }

    static int GenerateJobId()
    {
        return job_count++;
    }
};

int Job::job_count = 0;


class Master
{
    // std::map<int, std::set<std::string>> m_job_files_map;
    std::string m_log_path;
    std::list<Mapper*> mapper_list;
    std::list<Reducer*> reducer_list;
    std::map<int, Job*> jobs_map; //job_id->job
    std::map<int, std::set<std::pair<int, int>>> mapper_chunks_map; //socket -> (jobid, chunkid)
    std::map<int, std::set<std::pair<int, int>>> reducer_category_map; //socket -> (jobid, categoryid)

public:
    int m_port;
    int m_sock;
    std::string m_ip_addr;
    struct sockaddr_in m_sock_address;

    Master(std::string ip = "", int p = -1): m_log_path(""), m_port(p), m_sock(-1), m_ip_addr(ip) {}

    void                    run();
    void                    log_print(std::string msg);

    int                     request_handler(int sock, std::string req_str, Opcode &opcode);
    void                    response_handler(int sock, std::string response_str);
    int                     client_request_handler(int client_sock, std::string req_str);

    // Setter functions
    void                    log_path_set(std::string path) { m_log_path = path; }

    // str should be of the form "<IP Address>:<Port No.>"
    void                    ip_addr_set(std::string str) { util_ip_port_split (str, m_ip_addr, m_port); }
};


#endif
