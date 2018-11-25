#include <iostream>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <thread>

#include "DummyReducerServer.h"

using namespace std;

DummyReducerServer::DummyReducerServer(string ip_address, int port_number)
{
    this->ip_address = ip_address;
    this->port_number = port_number;
}

void DummyReducerServer::give_heart_beats(int sock_desc)
{
    int client_socket = sock_desc;
    while(true)
    {
        int av_slots = 3;
        string av_slots_string = to_string(av_slots);
        write(client_socket, av_slots_string.c_str(), av_slots_string.length());
        char job_id[255];
        bzero(job_id, 255);
        read(client_socket, job_id, 255);

        string job_string = job_id;
        if(job_string.compare("NULL"))
        {
            /*Code to scrap off job*/
            cout<<"JOB "<<job_id<<" scrapped off!"<<endl;   
        }

        this_thread::sleep_for(2s);
    }
}

void DummyReducerServer::process_reduce_request(int client_socket)
{
    char req_string[255];
    bzero(req_string, 255);
    read(client_socket, req_string, 255);
    string request_string = req_string;
    cout<<"\nREQUEST STRING : "<<request_string<<endl;
    if(!request_string.compare("initiate_heart_beats"))
    {
        thread hb = thread(&DummyReducerServer::give_heart_beats, this, client_socket);
        hb.detach();
    }
}

void DummyReducerServer::start_server()
{
    int port_number = this->port_number;
    int init_socket, client_socket;
    struct sockaddr_in reducer_server_address, reducer_client_address;
    socklen_t file_client_length;
    init_socket = socket(AF_INET, SOCK_STREAM, 0);
    if(init_socket < 0)
    {
        cout<<"Process: "<<getpid()<<" "<<"Error while creating file socket\n";
        exit(1);
    }
    bzero((char *)(&reducer_server_address), sizeof(reducer_server_address));
    reducer_server_address.sin_family = AF_INET;
    reducer_server_address.sin_addr.s_addr = INADDR_ANY;
    reducer_server_address.sin_port = htons(port_number);
    if( bind(init_socket, (struct sockaddr *) &reducer_server_address, sizeof(reducer_server_address)) < 0 )
    {
        cout<<"Process: "<<getpid()<<" "<<"Unable to bind to file server address!\n";
        exit(1);
    }
    listen(init_socket,10);
    file_client_length = sizeof(reducer_client_address);
    int thread_count = 0;
    thread all_threads[50];
    
    while( (client_socket = accept(init_socket, (struct sockaddr *) &reducer_client_address, &file_client_length)) )
    {
        cout<<"\n\nNew connection received\n\n";
        all_threads[thread_count] =  thread(&DummyReducerServer::process_reduce_request,this,client_socket);
        thread_count++;
    
    }
    cout<<"\nEXITING : CLIENT SOCKET : "<<client_socket<<endl;
    for(int i=0; i<thread_count; i++)
    {
        all_threads[i].join();
    }
}
