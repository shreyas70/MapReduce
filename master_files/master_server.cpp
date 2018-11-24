#include <string>
#include <cstdio>
#include <sys/socket.h>

#include "includes.h"
#include "master_server.h"

using namespace std;

int MasterTracker::request_send(Problem problem, string file_path)
{
    string problem_str = to_string((int)problem);
    string req = problem_str + "$" + file_path;
    int sz = req.length();
    send(sock, &sz, sizeof(sz), 0);
    send(sock, req.c_str(), req.length(), 0); 
    return SUCCESS;
}

