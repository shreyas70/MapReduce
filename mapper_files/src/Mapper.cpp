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

void Mapper::initiate_word_count_request(string job_id, int chunk_id, string file_path, off_t offset, size_t piece_size, int no_of_reducers)
{
    int sock = this->mapper_socket;
    string request_type = "initiate_word_count";
    string wc_details = job_id+"$"+to_string(chunk_id)+"$"+file_path+"$"+to_string(offset)+"$"+to_string(piece_size);
    string request_string = request_type + "#" + wc_details;
    int write_bytes = request_string.length();
    send(sock, &write_bytes, sizeof(write_bytes), 0);
    send(sock, request_string.c_str(), request_string.length(), 0);

}

void Mapper::initiate_inverted_index_request(string job_id, int chunk_id, vector<string> file_paths, vector<off_t> offsets, vector<size_t> piece_sizes, int no_of_reducers)
{
    int sock = this->mapper_socket;
    string request_type = "initiate_inverted_index";
    string ii_details = job_id;
    for(int i=0; i<file_paths.size(); i++)
    {
        ii_details = ii_details+"$"+file_paths[i]+"$"+to_string(offsets[i])+"$"+to_string(piece_sizes[i]);
    }
    string request_string = request_type + "#" + ii_details;

    int write_bytes = request_string.length();
    send(sock, &write_bytes, sizeof(write_bytes), 0);
    send(sock, request_string.c_str(), request_string.length(), 0);
 
    // int read_size;
    // read(sock, &read_size, sizeof(read_size));
    // char buff[read_size+1];
    // bzero(buff,read_size+1);
    // read(sock, buff, read_size);
    // string reply = buff;

    // if(!reply.compare("OK"))
    // {
    //     string reply_string = job_id;
    //     for(int i=0; i<file_paths.size(); i++)
    //     {
    //         reply_string = reply_string+"$"+file_paths[i]+"$"+to_string(offsets[i])+"$"+to_string(piece_sizes[i]);
    //     }
    //     int write_bytes = reply_string.length();
    //     send(sock, &write_bytes, sizeof(write_bytes), 0);
    //     send(sock, reply_string.c_str(), reply_string.length(), 0);
    // }
    // else
    // {
    //     cout<<"\nFailed to initiate connection with Mapper!";
    // }
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