#include <iostream>
#include <string>
#include <cstring>
#include <cstdint>

#include <sys/socket.h>
#include <cstdlib>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "reducer_server.h"

using namespace std;

string working_dir;

int main(int argc, char* argv[])
{
    int job_id;
    cin >> job_id;

    string r_ip_addr;
    int r_port;

    working_dir = getenv("PWD");
    if(working_dir != "/")
        working_dir = working_dir + "/";

    util_ip_port_split(argv[2], r_ip_addr, r_port);

    ReducerTracker reducer (r_ip_addr, r_port);

    if(FAILURE == reducer.sock_get())
        return FAILURE;

    string file_path = util_abs_path_get(argv[3]);
    reducer.request_send(Problem::WORD_COUNT, job_id, file_path);

    return 0;
}
