#include <iostream>
#include <string.h>
#include <vector>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include "Mapper.h"

using namespace std;

void Mapper::init(int mapper_socket)
{
    this->mapper_socket = mapper_socket;
}

int Mapper::get_socket()
{
    return this->mapper_socket;
}

void Mapper::initiate_word_count_request(int job_id, int chunk_id, string file_path, int start_line, int no_of_lines, int no_of_reducers)
{
    
    int sock = this->mapper_socket;
    string request_type = "initiate_word_count";
    string wc_details = to_string(job_id) +"$" + to_string(chunk_id)+"$"+file_path+"$"+to_string(start_line)+"$"+to_string(no_of_lines)+"$"+to_string(no_of_reducers);
    string request_string = request_type + "#" + wc_details;
    int write_bytes = request_string.length();
    send(sock, &write_bytes, sizeof(write_bytes), 0);
    send(sock, request_string.c_str(), request_string.length(), 0);

}

void Mapper::initiate_inverted_index_request(int job_id, int chunk_id, vector<string> file_paths, vector<int> start_lines, vector<int> no_of_lines, int no_of_reducers)
{
    int sock = this->mapper_socket;
    string request_type = "initiate_inverted_index";
    string ii_details = to_string(job_id) + "$"+to_string(chunk_id)+"$"+to_string(no_of_reducers);
    for(unsigned int i=0; i<file_paths.size(); i++)
    {
        ii_details = ii_details+"$"+file_paths[i]+"$"+to_string(start_lines[i])+"$"+to_string(no_of_lines[i]);
    }
    string request_string = request_type + "#" + ii_details;

    int write_bytes = request_string.length();
    send(sock, &write_bytes, sizeof(write_bytes), 0);
    send(sock, request_string.c_str(), request_string.length(), 0);
 
}

string Mapper::get_reply()
{
    int read_size;
    read(this->mapper_socket, &read_size, sizeof(read_size));
    char buff[read_size+1];
    bzero(buff,read_size+1);
    read(this->mapper_socket, buff, read_size);
    string reply = buff;
    return reply;
}