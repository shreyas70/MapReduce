#include <iostream>
#include <string>
#include <cstring>
#include <cstdint>

#include <sys/socket.h>
#include <cstdlib>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "common.h"

using namespace std;


string working_dir;

int make_connection(string ip, uint16_t port)
{
    struct sockaddr_in serv_addr;
    int sock = 0;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        cout << "Socket connection error!!" << endl;
        return FAILURE;
    }

    memset(&serv_addr, '0', sizeof(serv_addr));

    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(port);

    // Convert IPv4 and IPv6 addresses from text to binary form 
    if(inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr)<=0)
    {
        cout << "Invalid address/ Address not supported" << endl;
        return FAILURE;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        cout << "Connection with tracker failed!!" << endl;
        return FAILURE;
    }

    return sock;
}

int send_request(int sock, int req, string str)
{
    string req_op = to_string(req);
    str = req_op + "$" + str;
    int sz = str.length();
    send(sock, &sz, sizeof(sz), 0);
    send(sock, str.c_str(), str.length(), 0);
    return SUCCESS;
}

int main(int argc, char* argv[])
{
    string r_ip_addr;
    int r_port;

    working_dir = getenv("PWD");
    if(working_dir != "/")
        working_dir = working_dir + "/";

    ip_and_port_split(argv[2], r_ip_addr, r_port);

    int sock = make_connection(r_ip_addr, r_port);
    if(FAILURE == sock)
        return FAILURE;

    string file_path = abs_path_get(argv[3]);
    send_request(sock, 123, file_path);

    while(1);

    return 0;
}
