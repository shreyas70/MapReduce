#include <string>
#include <cstdio>
#include <sys/socket.h>

#include "includes.h"
#include "reducer_server.h"

using namespace std;

int ReducerTracker::request_send(Problem problem, int job_id, string file_path)
{
    string job_id_str = to_string(job_id);
    string problem_str = to_string((int)problem);
    string req = problem_str + "$" + job_id_str + "$" + file_path;
    int sz = req.length();
    send(sock, &sz, sizeof(sz), 0);
    send(sock, req.c_str(), req.length(), 0); 
    return SUCCESS;
}




