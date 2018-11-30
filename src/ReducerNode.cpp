#include<iostream>
#include<string>
#include<thread>
#include<utility>
#include "utilities.h"
#include "DummyMaster.h"
#include "ReducerNode.h"

using namespace std;

void ReducerNode::word_count(DummyMaster dm, string request_string)
{
    vector<string> req_split = split_string(request_string, '$');
    string job_id = req_split[0];
    int category = stoi(req_split[1]);
    string file_path = req_split[2];
    int no_of_mappers = stoi(req_split[3]);
    if(this->wc_reducer_map.find(job_id)==this->wc_reducer_map.end())
    {
        WordCountReducer * wcr = new WordCountReducer();
        string init_string = job_id+"$"+to_string(no_of_mappers);
        wcr->init(init_string);
        this->wc_reducer_map.emplace(make_pair(job_id, wcr));
    }
    WordCountReducer * wcr = this->wc_reducer_map[job_id];
    string status = wcr->reduce(category, file_path);
    if(!status.compare("FAILURE"))
    {
        dm.job_failure_reducer(stoi(job_id), category);
    }
    else if(status.compare("INCOMPLETE"))
    {
        dm.job_completed_reducer(job_id, category, status);
    }
}

void ReducerNode::inverted_index(DummyMaster dm, string request_string)
{
    vector<string> req_split = split_string(request_string, '$');
    string job_id = req_split[0];
    int category = stoi(req_split[1]);
    string file_path = req_split[2];
    int no_of_mappers = stoi(req_split[3]);
    if(this->ii_reducer_map.find(job_id)==this->ii_reducer_map.end())
    {
        InvertedIndexReducer * iir = new InvertedIndexReducer();
        string init_string = job_id+"$"+to_string(no_of_mappers);
        iir->init(init_string);
        this->ii_reducer_map.emplace(make_pair(job_id, iir));
    }
    InvertedIndexReducer * iir = this->ii_reducer_map[job_id];
    string status = iir->reduce(category, file_path);

    if(!status.compare("FAILURE"))
    {
        dm.job_failure_reducer(stoi(job_id), category);
    }
    else if(status.compare("INCOMPLETE"))
    {
        dm.job_completed_reducer(job_id, category, status);
    }
}

void ReducerNode::start_reducer_node(string master_ip_address, int master_port_number)
{
    DummyMaster dm;
    while(true)
    {
        string request_string;
        while(!dm.connection_exists())
        {
            if(FAILURE == dm.connect_as_mapper(master_ip_address, master_port_number))
            {
                cout << "master Connection Failure\n";
                sleep(2);
            }
        }
        if (FAILURE == dm.get_request(request_string))
        {
            continue;
        }
        vector<string> req_split = split_string(request_string, '#');
        string req_type = req_split[0];
        thread t;
        if(!req_type.compare("word_count"))
        {
            cout<<"\n\nReceived word count request\n\n";
            t = thread(&ReducerNode::word_count, this, dm, req_split[1]);
            t.detach();
        }
        else if(!req_type.compare("inverted_index"))
        {
            cout<<"\n\nReceived word count request\n\n";
            t = thread(&ReducerNode::inverted_index, this, dm, req_split[1]);
            t.detach();
        }   
    }
}

