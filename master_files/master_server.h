#ifndef _MASTER_SERVER_H_
#define _MASTER_SERVER_H_

#include <string>
#include <map>
#include <set>
#include "utilities.h"

class MasterTracker
{
    int sock;

public:
    int port;
    std::string ip_addr;

    MasterTracker(std::string ip = "", int p = -1): ip_addr(ip), port(p), sock(-1)
    {
        sock = util_connection_make(ip_addr, port);
    }

    int sock_get() { return sock; }

    int request_send(Problem problem, std::string file_path);
};


#endif
