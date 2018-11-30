#include "Reducer.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <string>
#include <string.h>

using namespace std;

void Reducer::init(int reducer_socket)
{
    this->reducer_socket = reducer_socket;
}

int Reducer::get_socket()
{
    return this->reducer_socket;
}

void Reducer::word_count_request(int job_id, int category, string file_path, int no_of_mappers)
{
    int sock = this->reducer_socket;
    string request_type = "word_count";
    string wc_details = to_string(job_id)+"$"+to_string(category)+"$"+file_path+"$"+to_string(no_of_mappers);
    string request_string = request_type + "#" + wc_details;
    int write_bytes = request_string.length();
    send(sock, &write_bytes, sizeof(write_bytes), 0);
    send(sock, request_string.c_str(), request_string.length(), 0);
}

void Reducer::inverted_index_request(int job_id, int category, string file_path, int no_of_mappers)
{
    int sock = this->reducer_socket;
    string request_type = "inverted_index";
    string wc_details = to_string(job_id)+"$"+to_string(category)+"$"+file_path+"$"+to_string(no_of_mappers);
    string request_string = request_type + "#" + wc_details;
    int write_bytes = request_string.length();
    send(sock, &write_bytes, sizeof(write_bytes), 0);
    send(sock, request_string.c_str(), request_string.length(), 0);
}

string Reducer::get_reply()
{
    int read_size;
    read(this->reducer_socket, &read_size, sizeof(read_size));
    char buff[read_size+1];
    bzero(buff,read_size+1);
    read(this->reducer_socket, buff, read_size);
    string reply = buff;
    return reply;
}