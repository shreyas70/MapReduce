#include<iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include<string>
#include<string.h>
#include<thread>
#include <fstream>
#include "DummyMaster.h"
#include "WordCount.h"
#include "InvertedIndex.h"
#include "MapperNode.h"
#include "Utility.h"

using namespace std;

void MapperNode::word_count(DummyMaster dm, string request_string)
{ 
    WordCountMapper wc = WordCountMapper(request_string);
    string output_file_path = wc.start_job();
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
    vector<string> reducer_files;
    reducer_files.push_back(reducer_file1);
    reducer_files.push_back(reducer_file2);
    reducer_files.push_back(reducer_file3);
    reducer_files.push_back(reducer_file4);
    dm.job_completed(wc.get_job_id(), reducer_files);
}

void MapperNode::inverted_index(DummyMaster dm, string request_string)
{
    InvertedIndexMapper ii = InvertedIndexMapper(request_string);
    string output_file_path = ii.start_job();
            
            
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

    vector<string> reducer_files;
    reducer_files.push_back(reducer_file1);
    reducer_files.push_back(reducer_file2);
    reducer_files.push_back(reducer_file3);
    reducer_files.push_back(reducer_file4);
    dm.job_completed(ii.get_job_id(), reducer_files);
}

void MapperNode::start_mapper_node(string master_ip_address, int master_port_number)
{
    DummyMaster dm;
    dm.connect_as_mapper(master_ip_address, master_port_number);
    while(true)
    {
        string request_string = dm.get_request();
        vector<string> req_split = split_string(request_string, '#');
        string req_type = req_split[0];
        thread t;
        if(!req_type.compare("initiate_word_count"))
        {
            cout<<"\n\nReceived initiate word count request\n\n";
            t = thread(&MapperNode::word_count, this, dm, req_split[1]);
            t.detach();
        }
        else if(!req_type.compare("initiate_inverted_index"))
        {
            cout<<"\n\nReceived initiate word count request\n\n";
            t = thread(&MapperNode::inverted_index, this, dm, req_split[1]);
            t.detach();
        }   
    }
    
}
