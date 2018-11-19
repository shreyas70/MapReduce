#include<iostream>
#include<string>
#include<vector>
#include "WordCount.h"

using namespace std;

WordCount::WordCount(std::string job_id, std::string file_path, off_t offset, size_t piece_size)
{
    this->job_id = job_id;
    this->file_path = file_path;
    this->offset = offset;
    this->piece_size = piece_size;
}

WordCount::WordCount(string request_string)
{
    vector<string> req_vec;
    string curr_string = "";
    for(int i=0; i<request_string.length(); i++)
    {
        char curr_char = request_string[i];
        if(curr_char=='$')
        {
            req_vec.push_back(curr_string);
            curr_string = "";
        }
        else
        {
            curr_string+=curr_char;
        }
    }
    req_vec.push_back(curr_string);
    this->job_id = req_vec[0];
    this->file_path = req_vec[1];
    this->offset = stoi(req_vec[2]);
    this->piece_size = stoi(req_vec[3]);
}

string WordCount::get_job_id()
{
    return this->job_id;
}

string WordCount::get_file_path()
{
    return this->file_path;
}

off_t WordCount::get_offset()
{
    return this->offset;
}

size_t WordCount::get_piece_size()
{
    return this->piece_size;
}