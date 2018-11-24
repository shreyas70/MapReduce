#include <iostream>
#include <string>
#include <cstring>
#include <cstdint>

#include <sys/socket.h>
#include <cstdlib>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "master_server.h"

using namespace std;

string working_dir;

int main(int argc, char* argv[])
{
    string m_ip_addr;
    int m_port;

    working_dir = getenv("PWD");
    if(working_dir != "/")
        working_dir = working_dir + "/";

    util_ip_port_split(argv[1], m_ip_addr, m_port);

    MasterTracker master (m_ip_addr, m_port);

    if(FAILURE == master.sock_get())
        return FAILURE;

    string file_path = util_abs_path_get(argv[2]);
    master.request_send(Problem::WORD_COUNT, file_path);

    return 0;
}
