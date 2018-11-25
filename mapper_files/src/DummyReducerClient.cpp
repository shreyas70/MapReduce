#include <iostream>
#include <string.h>
#include <vector>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <thread>
#include "DummyReducerClient.h"

using namespace std;

void DummyReducerClient::connect_to_reducer(string reducer_ip, int reducer_port)
{
    this->reducer_ip = reducer_ip;
    this->reducer_port = reducer_port;
    struct sockaddr_in server_ip;
    struct hostent * server;
    int sock = socket(AF_INET,SOCK_STREAM,0);
    if(sock < 0)
    {
        cout<<"\nError while creating socket\n";
        this->heart_beat_socket = -1;
        this->reducer_socket = -1;
    }
    server = gethostbyname(reducer_ip.c_str());
    if(server==NULL)
    {
        cout<<"\nNo such host identified\n";
        this->heart_beat_socket = -1;
        this->reducer_socket = -1;
    }
    bzero((char *) &server_ip, sizeof(server_ip));
    server_ip.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *) &server_ip.sin_addr.s_addr, server->h_length);
    server_ip.sin_port = htons(reducer_port);
    if(connect(sock,(struct sockaddr *) &server_ip, sizeof(server_ip)) < 0)
    {
        cout<<"\nConnection to server failed\n";
        this->heart_beat_socket = -1;
        this->reducer_socket = -1;
    }
    int hb_sock = initiate_heart_beats();
    this->heart_beat_socket = hb_sock;
    this->reducer_socket = sock;
}

int DummyReducerClient::initiate_heart_beats()
{
    string reducer_ip = this->reducer_ip;
    int reducer_port = this->reducer_port;
    struct sockaddr_in server_ip;
    struct hostent * server;
    int sock = socket(AF_INET,SOCK_STREAM,0);
    if(sock < 0)
    {
        cout<<"\nError while creating socket\n";
        return -1;
    }
    server = gethostbyname(reducer_ip.c_str());
    if(server==NULL)
    {
        cout<<"\nNo such host identified\n";
        return -1;
    }
    bzero((char *) &server_ip, sizeof(server_ip));
    server_ip.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *) &server_ip.sin_addr.s_addr, server->h_length);
    server_ip.sin_port = htons(reducer_port);
    if(connect(sock,(struct sockaddr *) &server_ip, sizeof(server_ip)) < 0)
    {
        cout<<"\nConnection to server failed\n";
        return -1;
    }
    write(sock, "initiate_heart_beats", 20);
    return sock;
}

string DummyReducerClient::receive_heart_beat()
{
    int hb_sock = this->heart_beat_socket;
    char heart_beat[10];
    bzero(heart_beat, 10);
    read(hb_sock, heart_beat, 10);
    string hb = heart_beat;
    return hb;
}

void DummyReducerClient::reply_to_heart_beat()
{
    int hb_sock = this->heart_beat_socket;
    write(hb_sock, "NULL", 4);
}

// DummyReducerClient::~DummyReducerClient()
// {
//     close(this->heart_beat_socket);
//     close(this->reducer_socket);
// }

int DummyReducerClient::get_heart_beat_socket()
{
    return this->heart_beat_socket;
}