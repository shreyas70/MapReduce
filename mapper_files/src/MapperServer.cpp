#include<iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <thread>
#include "MapperServer.h"
#include "WordCount.h"
#include "InvertedIndex.h"
#include "Utility.h"

using namespace std;

void MapperServer::set_mapper_ip(string mapper_ip)
{
    vector<string> mapper_ip_split = split_string(mapper_ip, ':');
    this->ip_address = mapper_ip_split[0];
    this->port_number = stoi(mapper_ip_split[1]);
}

void MapperServer::set_reducer_info(vector<string> reducer_ips)
{
    for(int i=0; i<reducer_ips.size(); i++)
    {
        string reducer_ip = reducer_ips[i];
        vector<string> reducer_ip_split = split_string(reducer_ip, ':');
        Reducer r = Reducer(reducer_ip_split[0], stoi(reducer_ip_split[1]));
        this->reducer_instances.push_back(r);
    }
}

Reducer::Reducer(string ip_address, int port_number)
{
    this->ip_address = ip_address;
    this->port_number = port_number;
}

string Reducer::get_ip_address()
{
    return this->ip_address;
}

int Reducer::get_port_number()
{
    return this->port_number;
}

JobRequest::JobRequest(string job_id, string job_type)
{
    this->job_id = job_id;
    if(!job_type.compare("WORD_COUNT"))
    {
        this->job_type = WORD_COUNT;
    }
    else if(!job_type.compare("INVERTED_INDEX"))
    {
        this->job_type = INVERTED_INDEX;
    }
}

string JobRequest::get_job_id()
{
    return this->job_id;
}

void JobRequest::link_file_to_reducer(Reducer r, string file_path)
{
    pair<Reducer, string> link = make_pair(r, file_path);
    this->file_map.push_back(link);
}

bool MapperServer::take_slot()
{
    lock_guard<mutex> lock(slot_lock);
    // cout<<"\nThread "<<this_thread::get_id()<<" Obtained lock on slot variable!"<<endl;
    // cout<<"\nThread "<<this_thread::get_id()<<" Current value of slots = "<<slots<<endl;
    if(slots<=0)
    {
        return false;
    }
    slots--;
    // cout<<"\nThread "<<this_thread::get_id()<<" Slots value changed to = "<<slots<<endl;
    return true;
}

void MapperServer::release_slot()
{
    lock_guard<mutex> lock(slot_lock);
    // cout<<"\nThread "<<this_thread::get_id()<<" Obtained lock on slot variable!"<<endl;
    // cout<<"\nThread "<<this_thread::get_id()<<" Current value of slots = "<<slots<<endl;
    slots++;
    // cout<<"\nThread "<<this_thread::get_id()<<" Slots value changed to = "<<slots<<endl;
}

int MapperServer::available_slots()
{
    lock_guard<mutex> lock(slot_lock);
    return slots;
}

void MapperServer::give_heart_beats(int sock_desc)
{
    int client_socket = sock_desc;
    while(true)
    {
        int av_slots = available_slots();
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
            if(!take_slot())
            {
                write(client_socket, "UNAVAILABLE", 11);
                return;
            }

            write(client_socket, "OK", 2);
            read(client_socket, req_string, 255);
            string request_string = req_string;
            cout<<"\n\nRequest string is : "<<request_string<<endl;
            WordCount wc = WordCount(request_string);
            // cout<<"\nJOB ID = "<<wc.get_job_id()<<endl;
            // cout<<"\nFILE PATH = "<<wc.get_file_path()<<endl;
            // cout<<"\nOFFSET = "<<wc.get_offset()<<endl;
            // cout<<"\nPIECE SIZE = "<<wc.get_piece_size()<<endl;

            write(client_socket, "OK", 2);
            string output_file_path = wc.start_job();

            release_slot();


            /*adding job to job queue for sending to reducer*/
            FILE * output_file = fopen(output_file_path.c_str(), "r");
            string file_dir = "output_files/";
            string job_id = wc.get_job_id();
            string reducer_file1 = file_dir+job_id+"_reducer1.txt";
            string reducer_file2 = file_dir+job_id+"_reducer2.txt";
            string reducer_file3 = file_dir+job_id+"_reducer3.txt";
            string reducer_file4 = file_dir+job_id+"_reducer4.txt";
            int r_wd1 = open(reducer_file1.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
            int r_wd2 = open(reducer_file2.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
            int r_wd3 = open(reducer_file3.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
            int r_wd4 = open(reducer_file4.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
            
            char buff[100];
            bzero(buff, 100);
            while( fscanf(output_file, "%s", buff)!=EOF )
            {
                string word = buff;
                bzero(buff, 100);
                fscanf(output_file, "%s", buff);
                string count = buff;
                if((word[0]>=65 && word[0]<=71) || (word[0]>=97 && word[0]<=103))
                {
                    write(r_wd1, word.c_str(), word.length());
                    write(r_wd1, " ", 1);
                    write(r_wd1, count.c_str(), count.length());
                    write(r_wd1, "\n", 1);
                }
                else if((word[0]>=72 && word[0]<=78) || (word[0]>=104 && word[0]<=110))
                {
                    write(r_wd2, word.c_str(), word.length());
                    write(r_wd2, " ", 1);
                    write(r_wd2, count.c_str(), count.length());
                    write(r_wd2, "\n", 1);
                }
                else if((word[0]>=79 && word[0]<=85) || (word[0]>=111 && word[0]<=117))
                {
                    write(r_wd3, word.c_str(), word.length());
                    write(r_wd3, " ", 1);
                    write(r_wd3, count.c_str(), count.length());
                    write(r_wd3, "\n", 1);
                }
                else
                {
                    write(r_wd4, word.c_str(), word.length());
                    write(r_wd4, " ", 1);
                    write(r_wd4, count.c_str(), count.length());
                    write(r_wd4, "\n", 1);
                }
            }
            fclose(output_file);
            close(r_wd1);
            close(r_wd2);
            close(r_wd3);
            close(r_wd4);

        }
        else if(!req_type.compare("initiate_inverted_index"))
        {
            if(!take_slot())
            {
                write(client_socket, "UNAVAILABLE", 11);
                return;
            }
            write(client_socket, "OK", 2);
            read(client_socket, req_string, 255);
            string request_string = req_string;
            cout<<"\n\nRequest string is : "<<request_string<<endl;
            InvertedIndex ii = InvertedIndex(request_string);
            write(client_socket, "OK", 2);
            ii.start_job();
            
            release_slot();
        }
        else if(!req_type.compare("initiate_heart_beats"))
        {
            thread hb = thread(&MapperServer::give_heart_beats,this,client_socket);
            hb.detach();
        }
        else if(!req_type.compare("get_available_slots"))
        {
            int av_slots = available_slots();
            string av_slots_string = to_string(av_slots);
            write(client_socket, av_slots_string.c_str(), av_slots_string.length());
        }
        else
        {
            write(client_socket, "invalid request",15);
        }
        connection_status = "close";
    }
    
}

void MapperServer::start_mapper_server()
{
    int port_number = this->port_number;
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
    listen(init_socket,10);
    file_client_length = sizeof(mapper_client_address);
    int thread_count = 0;
    thread all_threads[50];
    slots = 3;
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
    cout<<"\n\n"<<getpid()<<" exiting peacefully\n"<<"\n\n";
}