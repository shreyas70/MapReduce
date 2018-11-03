#include<iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <string>
#include <thread>
#include "MapperServer.h"

using namespace std;

void MapperServer::process_map_request(int sock_desc)
{
    int client_socket = sock_desc;
    char req_string[255];
    string connection_status = "open";
    while(!connection_status.compare("open"))
    {
        bzero(req_string, 255);
        read(client_socket, req_string, 255);
        string req_type = req_string;

        cout<<"\nRequest type: "<<req_type<<endl;

        bzero(req_string, 255);
        if(!req_type.compare("initiate_word_count"))
        {
            write(client_socket, "job_id", 6);
            read(client_socket, req_string, 255);
            string job_id = req_string;

            cout<<"\nJOB ID: "<<job_id<<endl;

            bzero(req_string, 255);
            write(client_socket, "file_path", 9);
            read(client_socket, req_string, 255);
            string file_path = req_string;

            cout<<"\nFile path: "<<file_path<<endl;

            bzero(req_string, 255);
            write(client_socket, "offset", 6);
            read(client_socket, req_string, 255);
            string file_offset_string = req_string;
            off_t file_offset = stoi(file_offset_string);

            cout<<"\nFile offset: "<<file_offset<<endl;

            bzero(req_string, 255);
            write(client_socket, "size", 4);
            read(client_socket, req_string, 255);
            string size_string = req_string;
            size_t file_size = stoi(size_string);

            cout<<"\nFile size: "<<file_size<<endl;

            write(client_socket, "OK", 2);
        }
        else
        {
            write(client_socket, "invalid request",15);
        }
        connection_status = "close";
    }
    
}

void MapperServer::start_mapper_server(int port_number)
{
    int init_socket, client_socket;
    struct sockaddr_in mapper_server_address, mapper_client_address;
    socklen_t file_client_length;
    init_socket = socket(AF_INET, SOCK_STREAM, 0);
    if(init_socket < 0)
    {
        cout<<"Process: "<<getpid()<<" "<<"Error while creating file socket\n";
        exit(1);
    }
    bzero((char *)(&mapper_server_address), sizeof(mapper_server_address));
    mapper_server_address.sin_family = AF_INET;
    mapper_server_address.sin_addr.s_addr = INADDR_ANY;
    mapper_server_address.sin_port = htons(port_number);
    if( bind(init_socket, (struct sockaddr *) &mapper_server_address, sizeof(mapper_server_address)) < 0 )
    {
        cout<<"Process: "<<getpid()<<" "<<"Unable to bind to file server address!\n";
        exit(1);
    }
    listen(init_socket,5);
    file_client_length = sizeof(mapper_client_address);
    int thread_count = 0;
    thread all_threads[50];
    while( (client_socket = accept(init_socket, (struct sockaddr *) &mapper_client_address, &file_client_length)) )
    {
        cout<<"\n\nNew connection received\n\n";
        all_threads[thread_count] =  thread(&MapperServer::process_map_request,this,client_socket);
        thread_count++;
    
    }
    for(int i=0; i<thread_count; i++)
    {
        all_threads[i].join();
    }
    
    close(init_socket);
    //close(client_socket);
    cout<<"\n\n"<<getpid()<<" exiting peacefully\n"<<"\n\n";
}