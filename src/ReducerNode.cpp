#include<iostream>
#include<string>
#include<thread>
#include<utility>
#include <unistd.h>
#include "utilities.h"
#include "master_client.h"
#include "ReducerNode.h"
#include "fs_client.h"

using namespace std;

void ReducerNode::word_count(MasterClient dm, string request_string, FS_Client * fs)
{
    // cout << "Reducer sleeping for 10 seconds " << endl;
    // sleep(10);

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
    // wcr->set_client_port_number(client_port_number);
    string status = wcr->reduce(category, file_path, fs);
    if(!status.compare("FAILURE"))
    {
        dm.job_failure_reducer(stoi(job_id), category);
    }
    else if(status.compare("INCOMPLETE"))
    {
        // string client_ip_address = "127.0.0.1:"+to_string(client_port_number);
        // FS_Client fs = FS_Client("127.0.0.1:9004", FILE_SERVER_IP);
        // FS_Client * fs = FileClient::get_file_client_object();
        string output_file_name = "word_count_"+job_id+"_output.txt";
        fs->append_file(status, output_file_name);
        fs->remove_file(status);
        dm.job_completed_reducer(stoi(job_id), category, status);
    }
}

void ReducerNode::inverted_index(MasterClient dm, string request_string, FS_Client * fs)
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
    string status = iir->reduce(category, file_path, fs);

    if(!status.compare("FAILURE"))
    {
        dm.job_failure_reducer(stoi(job_id), category);
    }
    else if(status.compare("INCOMPLETE"))
    {
        // string client_ip_address = "127.0.0.1:"+to_string(client_port_number);
        // FS_Client fs = FS_Client("127.0.0.1:9005", FILE_SERVER_IP);
        // FS_Client * fs = FileClient::get_file_client_object();

        string output_file_name = "word_count_"+job_id+"_output.txt";
        fs->append_file(status, output_file_name);
        fs->remove_file(status);
        dm.job_completed_reducer(stoi(job_id), category, status);
    }
}

void ReducerNode::start_reducer_node(string master_ip_address, int master_port_number, FS_Client * fs)
{
    MasterClient dm;
    cout <<"starting reducer" <<endl;
    while(true)
    {
        string request_string;
        while(!dm.connection_exists())
        {
            if(FAILURE == dm.connect_as_reducer(master_ip_address, master_port_number))
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
            t = thread(&ReducerNode::word_count, this, dm, req_split[1], fs);
            t.detach();
        }
        else if(!req_type.compare("inverted_index"))
        {
            cout<<"\n\nReceived word count request\n\n";
            t = thread(&ReducerNode::inverted_index, this, dm, req_split[1], fs);
            t.detach();
        }   
    }
}

