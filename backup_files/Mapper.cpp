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
#include "Mapper.h"

using namespace std;

void Mapper::connect_to_mapper(string mapper_ip, int mapper_port)
{
    struct sockaddr_in server_ip;
    struct hostent * server;
    int sock = socket(AF_INET,SOCK_STREAM,0);
    if(sock < 0)
    {
        cout<<"\nError while creating socket\n";
        this->heart_beat_socket = -1;
        this->mapper_socket = -1;
    }
    server = gethostbyname(mapper_ip.c_str());
    if(server==NULL)
    {
        cout<<"\nNo such host identified\n";
        this->heart_beat_socket = -1;
        this->mapper_socket = -1;
    }
    bzero((char *) &server_ip, sizeof(server_ip));
    server_ip.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *) &server_ip.sin_addr.s_addr, server->h_length);
    server_ip.sin_port = htons(mapper_port);
    if(connect(sock,(struct sockaddr *) &server_ip, sizeof(server_ip)) < 0)
    {
        cout<<"\nConnection to server failed\n";
        this->heart_beat_socket = -1;
        this->mapper_socket = -1;
    }
    int hb_sock = initiate_heart_beats(mapper_ip, mapper_port);
    this->heart_beat_socket = hb_sock;
    this->mapper_socket = sock;
}

int Mapper::initiate_heart_beats(string mapper_ip, int mapper_port)
{
    struct sockaddr_in server_ip;
    struct hostent * server;
    int sock = socket(AF_INET,SOCK_STREAM,0);
    if(sock < 0)
    {
        cout<<"\nError while creating socket\n";
        return -1;
    }
    server = gethostbyname(mapper_ip.c_str());
    if(server==NULL)
    {
        cout<<"\nNo such host identified\n";
        return -1;
    }
    bzero((char *) &server_ip, sizeof(server_ip));
    server_ip.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *) &server_ip.sin_addr.s_addr, server->h_length);
    server_ip.sin_port = htons(mapper_port);
    if(connect(sock,(struct sockaddr *) &server_ip, sizeof(server_ip)) < 0)
    {
        cout<<"\nConnection to server failed\n";
        return -1;
    }
    write(sock, "initiate_heart_beats", 20);
    return sock;
}

string Mapper::receive_heart_beat()
{
    int hb_sock = this->heart_beat_socket;
    char heart_beat[10];
    bzero(heart_beat, 10);
    read(hb_sock, heart_beat, 10);
    string hb = heart_beat;
    return hb;
}

void Mapper::reply_to_heart_beat()
{
    int hb_sock = this->heart_beat_socket;
    write(hb_sock, "NULL", 4);
}

void Mapper::initiate_word_count_request(string job_id, string file_path, off_t offset, size_t piece_size)
{
    int sock = this->mapper_socket;
    char req_string[255];
    string repl_string;
    write(sock, "initiate_word_count", 19);
    bzero(req_string, 255);
    read(sock, req_string, 255);
    repl_string = req_string;

    cout<<"Reply string : "<<repl_string<<endl;

    if(!repl_string.compare("OK"))
    {
        string reply_string = job_id+"$"+file_path+"$"+to_string(offset)+"$"+to_string(piece_size);
        write(sock, reply_string.c_str(), reply_string.length());
        bzero(req_string, 255);
        read(sock, req_string, 255);
        repl_string = req_string;
        if(repl_string.compare("OK"))
        {
            cout<<"\n\nFailed to initiate word count job!\n\n";
        }
    }
    else
    {
        cout<<"\nFailed to initiate connection with Mapper!";
    }
}

void Mapper::initiate_inverted_index_request(string job_id, vector<string> file_paths, vector<off_t> offsets, vector<size_t> piece_sizes)
{
    int sock = this->mapper_socket;
    char req_string[255];
    string repl_string;
    write(sock, "initiate_inverted_index", 23);
    bzero(req_string, 255);
    read(sock, req_string, 255);
    repl_string = req_string;

    cout<<"Reply string : "<<repl_string<<endl;

    if(!repl_string.compare("OK"))
    {
        string reply_string = job_id;
        for(int i=0; i<file_paths.size(); i++)
        {
            reply_string = reply_string+"$"+file_paths[i]+"$"+to_string(offsets[i])+"$"+to_string(piece_sizes[i]);
        }
        write(sock, reply_string.c_str(), reply_string.length());
        bzero(req_string, 255);
        read(sock, req_string, 255);
        repl_string = req_string;
        if(repl_string.compare("OK"))
        {
            cout<<"\n\nFailed to initiate word count job!\n\n";
        }
    }
    else
    {
        cout<<"\nFailed to initiate connection with Mapper!";
    }
}

int Mapper::get_available_slots()
{
    int sock = this->mapper_socket;
    char req_string[255];
    string repl_string;
    bzero(req_string, 255);
    write(sock, "get_available_slots", 19);
    read(sock, req_string, 255);
    repl_string = req_string;

    cout<<"Reply string : "<<repl_string<<endl;

    int av_slots = stoi(repl_string);
    return av_slots;
}
