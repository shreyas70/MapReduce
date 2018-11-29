#include<iostream>
#include<string>
#include<string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include "DummyMaster.h"

using namespace std;


void DummyMaster::connect_as_mapper(string ip_address, int port_number)
{
    
    struct sockaddr_in server_ip;
    struct hostent * server;
    int sock = socket(AF_INET,SOCK_STREAM,0);
    if(sock < 0)
    {
        cout<<"\nError while creating socket\n";
    }
    server = gethostbyname(ip_address.c_str());
    if(server==NULL)
    {
        cout<<"\nNo such host identified\n";
    }
    bzero((char *) &server_ip, sizeof(server_ip));
    server_ip.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *) &server_ip.sin_addr.s_addr, server->h_length);
    server_ip.sin_port = htons(port_number);
    if(connect(sock,(struct sockaddr *) &server_ip, sizeof(server_ip)) < 0)
    {
        cout<<"\nConnection to server failed\n";
    }

    string sys_type = "MAPPER";
    int write_bytes = sys_type.length();
    send(sock, &write_bytes, sizeof(write_bytes), 0);
    send(sock, sys_type.c_str(), sys_type.length(), 0);
    this->sock_id = sock;
}

string DummyMaster::get_request()
{
    int read_size;
    read(this->sock_id, &read_size, sizeof(read_size));
    char buff[read_size+1];
    bzero(buff,read_size+1);
    read(this->sock_id, buff, read_size);
    string reply = buff;

    return reply;
}

void DummyMaster::confirm()
{
    // int socket_fd = *(this->sock);

    string ok = "OK";
    int write_bytes = ok.length();
    send(this->sock_id, &write_bytes, sizeof(write_bytes), 0);
    send(this->sock_id, ok.c_str(), ok.length(), 0);
}


void DummyMaster::job_completed(string job_id, vector<string> reducer_files)
{
    // int socket_fd = *(this->sock);

    string reply_string = job_id;
    for(int i=0; i<reducer_files.size(); i++)
    {
        reply_string = reply_string + "$" + reducer_files[i];
    }

    int write_bytes = reply_string.length();
    send(this->sock_id, &write_bytes, sizeof(write_bytes), 0);
    send(this->sock_id, reply_string.c_str(), reply_string.length(), 0);
}
