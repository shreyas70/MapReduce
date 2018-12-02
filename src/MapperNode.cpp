#include<iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include<string>
#include<string.h>
#include<thread>
#include <fstream>
#include <cmath>

#include "master_client.h"
#include "WordCount.h"
#include "InvertedIndex.h"
#include "MapperNode.h"
#include "utilities.h"
#include "fs_client.h"

using namespace std;

void MapperNode::word_count(MasterClient dm, string request_string, FS_Client * fs)
{ 
    cout <<"Socket in thread : " << dm.sock_get() << endl << flush;
    cout << request_string << endl << flush;

    // cout << "Sleeping for 10 seconds" << endl;
    // sleep(10);
    // cout << "Wokeup after 10 seconds" << endl;

    WordCountMapper wc = WordCountMapper(request_string);
    // wc.set_client_port_number(client_port_number);
    string status = wc.start_job(fs);

    if(!status.compare("FAILURE"))
    {
        dm.job_failure_mapper(stoi(wc.get_job_id()), stoi(wc.get_chunk_id()));
    }

    string output_file_path = status;
    FILE * output_file = fopen(output_file_path.c_str(), "r");
    // string file_dir = "output_files/";
    string job_id = wc.get_job_id();
    vector<string> reducer_files;
    int no_of_reducers = wc.get_no_of_reducers();
    for(int i=0; i<no_of_reducers; i++)
    {
        string file_name = "M_job_" + job_id + "_chunk_" + wc.get_chunk_id() + "_category_" + to_string(i) + ".txt";
        reducer_files.push_back(file_name);
    }
    vector<int> r_wd;
    for(int i=0; i<no_of_reducers; i++)
    {
        int wd = open(reducer_files[i].c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
        r_wd.push_back(wd);
    }

    int inc = ceil((double)26/no_of_reducers);
    vector<pair<int,int>> small_ranges;
    vector<pair<int,int>> capital_ranges;
    int x=0,y=1;
    for(int i=0; i<no_of_reducers; i++)
    {
        int start = 'a' + (inc * x);
        int end = start + (inc - 1);
        end = (end > 'z') ? 'z' : end;
        if(i==(no_of_reducers-1))
        {
            end = 'z';
        }
        pair<int,int> range = make_pair(start,end);
        small_ranges.push_back(range);
        x++;
        y++;
        cout << " cat " << i << " start - " << (char)start << "| end -" << (char) end <<endl;
    }

    x=0,y=1;
    for(int i=0; i<no_of_reducers; i++)
    {
        int start = 'A' + (inc * x);
        int end = start + (inc - 1);
        end = (end > 'Z') ? 'Z' : end;
        if(i == (no_of_reducers-1))
        {
            end = 'Z';
        }
        pair<int,int> range = make_pair(start,end);
        capital_ranges.push_back(range);
        x++;
        y++;
        cout << " cat " << i << " start - " << (char)start << "| end -" << (char) end <<endl;
    }

    char buff[100];
    bzero(buff, 100);
    while( fscanf(output_file, "%s", buff)!=EOF )
    {
        string word = buff;
        bzero(buff, 100);
        fscanf(output_file, "%s", buff);
        string count = buff;
        for(int i=0; i<no_of_reducers; i++)
        {
            int start = small_ranges[i].first;
            int end = small_ranges[i].second;
            if(word[0]>=start && word[0]<=end)
            {
                write(r_wd[i], word.c_str(), word.length());
                write(r_wd[i], " ", 1);
                write(r_wd[i], count.c_str(), count.length());
                write(r_wd[i], "\n", 1);
                break;
            }
            start = capital_ranges[i].first;
            end = capital_ranges[i].second;
            if(word[0]>=start && word[0]<=end)
            {
                write(r_wd[i], word.c_str(), word.length());
                write(r_wd[i], " ", 1);
                write(r_wd[i], count.c_str(), count.length());
                write(r_wd[i], "\n", 1);
                break;
            }
        }
    }
    fclose(output_file);
    for(int i=0; i<no_of_reducers; i++)
    {
        close(r_wd[i]);
    }

    // string client_ip_address = "127.0.0.1:"+to_string(client_port_number);
    // FS_Client fs = FS_Client("127.0.0.1:9002", FILE_SERVER_IP);
    // FS_Client * fs = FileClient::get_file_client_object();
    // remove(output_file_path.c_str());
    for(int i=0; i<reducer_files.size(); i++)
    {
        string rf = reducer_files[i];
        if(fs->upload_file(rf.c_str()))
        {
            cout<<endl<<rf<<" uploaded successfully!\n";
        }
        // remove(rf.c_str());
    }
    
    dm.job_completed_mapper(stoi(wc.get_job_id()), stoi(wc.get_chunk_id()) ,reducer_files);
}

void MapperNode::inverted_index(MasterClient dm, string request_string, FS_Client * fs)
{
    cout << endl << "[II]: req string: " << request_string << endl;
    InvertedIndexMapper ii = InvertedIndexMapper(request_string);
    // ii.set_client_port_number(client_port_number);
    string status = ii.start_job(fs);

    cout << "Sleeping for 10 seconds" << endl;
    sleep(10);
    cout << "Wokeup after 10 seconds" << endl;

    if(!status.compare("FAILURE"))
    {
        dm.job_failure_mapper(stoi(ii.get_job_id()), stoi(ii.get_chunk_id()));
    }        
    
    string output_file_path = status;        
    FILE * output_file = fopen(output_file_path.c_str(), "r");
    // string file_dir = "output_files/";
    string job_id = ii.get_job_id();
    int no_of_reducers = ii.get_no_of_reducers();
    vector<string> reducer_files;
    for(int i=0; i<no_of_reducers; i++)
    {
        string file_name = "M_job_" + job_id + "_chunk_" + ii.get_chunk_id() + "_category_" + to_string(i) + ".txt";
        // string file_name = file_dir + "M_job_" + job_id + "_chunk_" + ii.get_chunk_id() + "_category_" + to_string(i) + ".txt";
        reducer_files.push_back(file_name);
    }

    vector<int> r_wd;
    for(int i=0; i<no_of_reducers; i++)
    {
        int wd = open(reducer_files[i].c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
        r_wd.push_back(wd);
    }

    int inc = ceil((double)26/no_of_reducers);
    // int inc = (26%no_of_reducers==0)?(26/no_of_reducers):((26/no_of_reducers)+1);
    vector<pair<int,int>> small_ranges;
    vector<pair<int,int>> capital_ranges;
    int x=0,y=1;
    for(int i=0; i<no_of_reducers; i++)
    {
        int start = 'a'+(inc*x);
        int end = start + (inc - 1);
        end = (end > 'z') ? 'z' : end;
        if(i==(no_of_reducers-1))
        {
            end = 'z';
        }
        pair<int,int> range = make_pair(start,end);
        small_ranges.push_back(range);
        x++;
        y++;
        cout << " cat " << i << " start - " << (char)start << "| end -" << (char) end <<endl;
    }

    x=0,y=1;
    for(int i=0; i<no_of_reducers; i++)
    {
        int start = 'A' + (inc*x);
        int end = start + (inc - 1);
        end = (end > 'Z') ? 'Z' : end;
        if(i==(no_of_reducers-1))
        {
            end = 'Z';
        }
        pair<int,int> range = make_pair(start,end);
        capital_ranges.push_back(range);
        x++;
        y++;
        cout << " cat " << i << " start - " << (char)start << "| end -" << (char) end <<endl;
    }

    string line;
    ifstream infile(output_file_path.c_str());
    while(getline(infile, line))
    {
        if(line.empty())
        {
            continue;
        }
        vector<string> words_split = split_string(line, ' ');
        for(int i=0; i<no_of_reducers; i++)
        {
            int start = small_ranges[i].first;
            int end = small_ranges[i].second;
            if(words_split[0][0]>=start && words_split[0][0]<=end)
            {
                write(r_wd[i], words_split[0].c_str(), words_split[0].length());
                write(r_wd[i], " ", 1);
                write(r_wd[i], words_split[1].c_str(), words_split[1].length());
                for(int j=2; j<words_split.size(); j++)
                {
                    write(r_wd[i], " ", 1);
                    write(r_wd[i], words_split[j].c_str(), words_split[j].length());
                }
                write(r_wd[i], "\n", 1);
                break;
            }
            start = capital_ranges[i].first;
            end = capital_ranges[i].second;
            if(words_split[0][0]>=start && words_split[0][0]<=end)
            {
                write(r_wd[i], words_split[0].c_str(), words_split[0].length());
                write(r_wd[i], " ", 1);
                write(r_wd[i], words_split[1].c_str(), words_split[1].length());
                for(int j=2; j<words_split.size(); j++)
                {
                    write(r_wd[i], " ", 1);
                    write(r_wd[i], words_split[j].c_str(), words_split[j].length());
                }
                write(r_wd[i], "\n", 1);
                break;
            }
        }

    }

    for(int i=0; i<no_of_reducers; i++)
    {
        close(r_wd[i]);
    }

    for(int i=0; i<reducer_files.size(); i++)
    {
        string rf = reducer_files[i];
        if(fs->upload_file(rf.c_str()))
        {
            cout<<endl<<rf<<" uploaded successfully!\n";
        }
        // remove(rf.c_str());
    }

    dm.job_completed_mapper(stoi(ii.get_job_id()),stoi(ii.get_chunk_id()), reducer_files);
}

void MapperNode::start_mapper_node(string master_ip_address, int master_port_number, FS_Client * fs)
{
    MasterClient dm;
    while(true)
    {
        string request_string;
        while(!dm.connection_exists())
        {
            if(FAILURE == dm.connect_as_mapper(master_ip_address, master_port_number))
            {
                cout << "Master Connection Failure\n";
                sleep(2);
            }
        }

        if (FAILURE == dm.get_request(request_string))
        {
            continue;
        }
        cout << "request string " << request_string << endl << flush;
        vector<string> req_split = split_string(request_string, '#');
        string req_type = req_split[0];
        thread t;
        if(!req_type.compare("initiate_word_count"))
        {
            cout<<"\n\nReceived initiate word count request\n\n";
            t = thread(&MapperNode::word_count, this, dm, req_split[1], fs);
            t.detach();
        }
        else if(!req_type.compare("initiate_inverted_index"))
        {
            cout<<"\n\nReceived inverted index request\n\n";
            t = thread(&MapperNode::inverted_index, this, dm, req_split[1], fs);
            t.detach();
        }   
    }
}
