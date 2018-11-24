#ifndef _REDUCER_SERVER_H_
#define _REDUCER_SERVER_H_

#include <string>
#include <map>
#include <set>
#include "utilities.h"

class ReducerTracker
{
    int sock;

public:

    int port;
    std::string ip_addr;

    ReducerTracker(std::string ip = "", int p = -1): sock(-1), port(p), ip_addr(ip)
    {
        sock = util_connection_make(ip_addr, port);
    }

    int sock_get() { return sock; }

    int request_send(Problem problem, int job_id, std::string file_path);
};


#endif
