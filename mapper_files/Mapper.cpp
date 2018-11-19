#include<iostream>
#include<string.h>
#include<string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <thread>
#include "Mapper.h"

using namespace std;

Mapper::Mapper(string mapper_ip, int mapper_port)
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

int Mapper::initiate_word_count_request(string job_id, string file_path, off_t offset, size_t piece_size)
{
    int sock = this->mapper_socket;
    char req_string[255];
    string repl_string;
    bzero(req_string, 255);
    write(sock, "initiate_word_count", 19);
    read(sock, req_string, 255);
    repl_string = req_string;

    cout<<"Reply string : "<<repl_string<<endl;

    if(!repl_string.compare("job_id"))
    {
        write(sock, job_id.c_str(), job_id.length());
        bzero(req_string, 255);
        read(sock, req_string, 255);
        repl_string = req_string;

        cout<<"Reply string : "<<repl_string<<endl;

        if(!repl_string.compare("file_path"))
        {
            write(sock, file_path.c_str(), file_path.length());
            bzero(req_string, 255);
            read(sock, req_string, 255);
            repl_string = req_string;

            cout<<"Reply string : "<<repl_string<<endl;

            if(!repl_string.compare("offset"))
            {
                string offset_string = to_string(offset);
                write(sock, offset_string.c_str(), offset_string.length());
                bzero(req_string, 255);
                read(sock, req_string, 255);
                repl_string = req_string;

                cout<<"Reply string : "<<repl_string<<endl;

                if(!repl_string.compare("size"))
                {
                    string size_string = to_string(piece_size);
                    write(sock, size_string.c_str(), size_string.length());
                    bzero(req_string, 255);
                    read(sock, req_string, 255);
                    repl_string = req_string;

                    cout<<"Reply string : "<<repl_string<<endl;

                    if(!repl_string.compare("OK"))
                    {
                        return sock;
                    }
                    else
                    {
                        cout<<"\nFailed to initiate connection with Mapper!";
                        close(sock);
                        return -1;
                    }
                }
                else
                {
                    cout<<"\nFailed to initiate connection with Mapper!";
                    close(sock);
                    return -1;
                }
            }
            else
            {
                cout<<"\nFailed to initiate connection with Mapper!";
                close(sock);
                return -1;
            }
        }
        else
        {
            cout<<"\nFailed to initiate connection with Mapper!";
            close(sock);
            return -1;
        }
    }
    else
    {
        cout<<"\nFailed to initiate connection with Mapper!";
        close(sock);
        return -1;
    }
    return -1;

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