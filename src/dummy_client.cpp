#include <iostream>
#include <string>
#include <cstring>
#include <cstdint>

#include <sys/socket.h>
#include <cstdlib>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "master_client.h"

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

    MasterClient m;

    // if(FAILURE == master.sock_get())
    //     return FAILURE;

    string file_path = util_abs_path_get(argv[2]);
    
    // master.request_send(Problem::WORD_COUNT, file_path);
    m.connect_as_client(m_ip_addr, m_port,Problem::WORD_COUNT, file_path);
    // master.connect_as_reducer();
    // master.connect_as_mapper();

    while(1);

    return 0;
}
