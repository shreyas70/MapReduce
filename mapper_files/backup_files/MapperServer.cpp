#include<iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/time.h>
#include <string.h>
#include <thread>
#include <list>
#include <fstream>
#include <unordered_map>
#include "WordCount.h"
#include "InvertedIndex.h"
#include "Utility.h"
#include "DummyReducerClient.h"
#include "MapperServer.h"

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
        DummyReducerClient r;
        r.connect_to_reducer(reducer_ip_split[0], stoi(reducer_ip_split[1]));
        this->reducer_instances.push_back(r);
    }
}

void JobRequest::set_job_id(string job_id)
{
    this->job_id = job_id;
}

void JobRequest::set_job_type(string job_type)
{
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

vector<pair<DummyReducerClient, string>> JobRequest::get_file_map()
{
    return this->file_map;
}

int JobRequest::get_job_type()
{
    return this->job_type;
}

void JobRequest::link_file_to_reducer(DummyReducerClient r, string file_path)
{
    pair<DummyReducerClient, string> link = make_pair(r, file_path);
    this->file_map.push_back(link);
}

bool MapperServer::is_pending_queue_empty()
{
    lock_guard<mutex> lock(queue_lock);
    if(this->pending_queue_size <= 0)
    {
        return true;
    }
    return false;
}

void MapperServer::add_job_to_pending_queue(JobRequest * jr)
{
    lock_guard<mutex> lock(queue_lock);
    this->pending_queue->push(jr);
    this->pending_queue_size++;
}

JobRequest * MapperServer::get_next_job()
{
    lock_guard<mutex> lock(queue_lock);
    if(this->pending_queue_size <= 0)
    {
        return NULL;
    }
    JobRequest * jr = this->pending_queue->front();
    this->pending_queue->pop();
    this->pending_queue_size--;
    return jr;
}

bool MapperServer::take_slot()
{
    lock_guard<mutex> lock(slot_lock);
    if(slots<=0)
    {
        return false;
    }
    slots--;
    return true;
}

void MapperServer::release_slot()
{
    lock_guard<mutex> lock(slot_lock);
    slots++;
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

void MapperServer::dispatch()
{
    unordered_map<int, bool> slots_available;
    while(true)
    {
        for(int i=0; i<this->reducer_instances.size(); i++)
        {
            int slots = stoi(this->reducer_instances[i].receive_heart_beat());
            if(slots>0)
            {
                slots_available[i] = true;
            }
            else
            {
                slots_available[i] = false;
            }
            cout<<"\nHeart beat received from reducer "<<(i+1)<<" "<<slots<<endl;
            this->reducer_instances[i].reply_to_heart_beat();
        }
        int reducers_available = 0;
        for(unordered_map<int, bool>::iterator it = slots_available.begin(); it!=slots_available.end(); ++it)
        {
            if(it->second)
            {
                reducers_available++;
            }
        }
        if(reducers_available >= this->reducer_instances.size())
        {
            cout<<"\n\nHURRAY OK TO SEND!\n\n";
            if(!this->is_pending_queue_empty())
            {
                JobRequest * jr = this->get_next_job();
                vector<pair<DummyReducerClient, string>> file_map = jr->get_file_map();
                for(int i=0; i<file_map.size(); i++)
                {
                    DummyReducerClient dr = file_map[i].first;
                    string file_path = file_map[i].second;
                    if(jr->get_job_type()==WORD_COUNT)
                    {
                        cout<<"\n\nAssigning "<<file_path<<" to reducer "<<(i+1);
                        dr.start_word_count_job(file_path);
                    }
                    else if(jr->get_job_type()==INVERTED_INDEX)
                    {
                        cout<<"\n\nAssigning "<<file_path<<" to reducer "<<(i+1);
                        dr.start_inverted_index_job(file_path);
                    }
                
                }    
            }
            else
            {
                cout<<"\n\nNo jobs at queue!\n\n";
            }
            
            
        }
        else
        {
            cout<<"\n\nNOT OK TO SEND\n\n";
        }
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

            JobRequest * jr = new JobRequest();
            jr->set_job_id(wc.get_job_id());
            jr->set_job_type("WORD_COUNT");
            jr->link_file_to_reducer(this->reducer_instances[0], reducer_file1);
            jr->link_file_to_reducer(this->reducer_instances[1], reducer_file2);
            jr->link_file_to_reducer(this->reducer_instances[2], reducer_file3);
            jr->link_file_to_reducer(this->reducer_instances[3], reducer_file4);
            this->add_job_to_pending_queue(jr);
            cout<<"\n\nAdded JOB "<<wc.get_job_id()<<" to pending queue!\n\n";

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
            string output_file_path = ii.start_job();
            
            release_slot();

            FILE * output_file = fopen(output_file_path.c_str(), "r");
            string file_dir = "output_files/";
            string job_id = ii.get_job_id();
            string reducer_file1 = file_dir+job_id+"_reducer1.txt";
            string reducer_file2 = file_dir+job_id+"_reducer2.txt";
            string reducer_file3 = file_dir+job_id+"_reducer3.txt";
            string reducer_file4 = file_dir+job_id+"_reducer4.txt";
            int r_wd1 = open(reducer_file1.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
            int r_wd2 = open(reducer_file2.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
            int r_wd3 = open(reducer_file3.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
            int r_wd4 = open(reducer_file4.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
            
            string line;
            ifstream infile(output_file_path.c_str());
            while(getline(infile, line))
            {
                if(line.empty())
                {
                    continue;
                }
                vector<string> words_split = split_string(line, ' ');
                if((words_split[0][0]>=65 && words_split[0][0]<=71) || (words_split[0][0]>=97 && words_split[0][0]<=103))
                {
                    write(r_wd1, words_split[0].c_str(), words_split[0].length());
                    for(int i=1; i<words_split.size(); i++)
                    {
                        write(r_wd1, " ", 1);
                        write(r_wd1, words_split[i].c_str(), words_split[i].length());
                    }
                    write(r_wd1, "\n", 1);
                }
                else if((words_split[0][0]>=72 && words_split[0][0]<=78) || (words_split[0][0]>=104 && words_split[0][0]<=110))
                {
                    write(r_wd2, words_split[0].c_str(), words_split[0].length());
                    for(int i=1; i<words_split.size(); i++)
                    {
                        write(r_wd2, " ", 1);
                        write(r_wd2, words_split[i].c_str(), words_split[i].length());
                    }
                    write(r_wd2, "\n", 1);
                }
                else if((words_split[0][0]>=79 && words_split[0][0]<=85) || (words_split[0][0]>=111 && words_split[0][0]<=117))
                {
                    write(r_wd3, words_split[0].c_str(), words_split[0].length());
                    for(int i=1; i<words_split.size(); i++)
                    {
                        write(r_wd3, " ", 1);
                        write(r_wd3, words_split[i].c_str(), words_split[i].length());
                    }
                    write(r_wd3, "\n", 1);
                }
                else
                {
                    write(r_wd4, words_split[0].c_str(), words_split[0].length());
                    for(int i=1; i<words_split.size(); i++)
                    {
                        write(r_wd4, " ", 1);
                        write(r_wd4, words_split[i].c_str(), words_split[i].length());
                    }
                    write(r_wd4, "\n", 1);
                }

            }

            close(r_wd1);
            close(r_wd2);
            close(r_wd3);
            close(r_wd4);

            JobRequest * jr = new JobRequest();
            jr->set_job_id(ii.get_job_id());
            jr->set_job_type("INVERTED_INDEX");
            jr->link_file_to_reducer(this->reducer_instances[0], reducer_file1);
            jr->link_file_to_reducer(this->reducer_instances[1], reducer_file2);
            jr->link_file_to_reducer(this->reducer_instances[2], reducer_file3);
            jr->link_file_to_reducer(this->reducer_instances[3], reducer_file4);
            this->add_job_to_pending_queue(jr);
            cout<<"\n\nAdded JOB "<<ii.get_job_id()<<" to pending queue!\n\n";
            
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
    this->pending_queue_size = 0;
    this->pending_queue = new queue<JobRequest *>();
    thread dispatch_thread = thread(&MapperServer::dispatch, this);
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