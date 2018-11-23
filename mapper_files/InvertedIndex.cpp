#include<unistd.h>
#include<fcntl.h>
#include <iostream>
#include <vector>
#include <string.h>
#include <string>
#include <set>
#include <unordered_map>
#include <utility>
#include "InvertedIndex.h"

using namespace std;

InvertedIndex::InvertedIndex(string job_id, vector<string> file_paths, vector<off_t> offsets, vector<size_t> piece_sizes)
{
    this->job_id = job_id;
    for(int i=0; i<file_paths.size(); i++)
    {
        FileInfo file_info;
        file_info.file_path = file_paths[i];
        file_info.offset = offsets[i];
        file_info.piece_size = piece_sizes[i];
        this->input_files.push_back(file_info);
    }
}

InvertedIndex::InvertedIndex(string request_string)
{
    vector<string> req_vec;
    string curr_string = "";
    for(int i=0; i<request_string.length(); i++)
    {
        char curr_char = request_string[i];
        if(curr_char == '$')
        {
            req_vec.push_back(curr_string);
            curr_string = "";
        }
        else
        {
            curr_string+=curr_char;
        }
    }
    if(!curr_string.empty())
    {
        req_vec.push_back(curr_string);
    }
    this->job_id = req_vec[0];
    FileInfo curr_file_info;
    bool first = true;
    for(int i=1; i<req_vec.size(); i++)
    {
        string req_string = req_vec[i];
        int index = (i-1);
        if(index%3 == 0)
        {
            if(!first)
            {
                this->input_files.push_back(curr_file_info);
            }
            else
            {
                first = false;
            }
            FileInfo temp;
            curr_file_info = temp;
            curr_file_info.file_path = req_string;
        }
        else if(index%3 == 1)
        {
            curr_file_info.offset = stoi(req_string);
        }
        else if(index%3 == 2)
        {
            curr_file_info.piece_size = stoi(req_string);
        }
    }
    this->input_files.push_back(curr_file_info);
}

void InvertedIndex::start_job()
{
    cout<<"\n\nJOB "<<this->job_id<<" started!!"<<endl;
    unordered_map<string, set<string>> index;
    for(int i=0; i<this->input_files.size(); i++)
    {
        FileInfo file_info = this->input_files[i];
        string file_path = file_info.file_path;
        FILE * file_ptr = fopen(file_path.c_str(), "r");
        char buff[100];
        bzero(buff, 100);
        while( fscanf(file_ptr, "%s", buff)!=EOF )
        {
            string word = buff;
            if(index.find(word)==index.end())
            {
                set<string> file_names;
                index[word] = file_names;
            }
            set<string> file_names = index[word];
            file_names.insert(file_path);
            index[word] = file_names;
            bzero(buff,100);
        }
        fclose(file_ptr);
    }
    string out_file_name = "output_inverted.txt";
    int wd = open(out_file_name.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
    for(unordered_map<string, set<string>>::iterator it = index.begin(); it!=index.end(); ++it)
    {
        string word = it->first;
        write(wd, word.c_str(), word.length());
        write(wd, " ", 1);
        set<string> file_names = it->second;
        for(set<string>::iterator fit = file_names.begin(); fit!=file_names.end(); ++fit)
        {
            string file_name = *fit;
            write(wd, file_name.c_str(), file_name.length());
            write(wd, " ", 1);
        }
        write(wd, "\n", 1);
    }
    close(wd);
    cout<<"\nJOB "<<this->job_id<<" COMPLETED\n";
}