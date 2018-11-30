#include <iostream>
#include <vector>
#include <string>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <thread>
#include "Mapper.h"
#include "Utility.h"
#include "Reducer.h"

using namespace std;


void process_request(int socket)
{
    int read_size;
    read(socket, &read_size, sizeof(read_size));
    char buff[read_size+1];
    bzero(buff,read_size+1);
    read(socket, buff, read_size);
    string reply = buff;
    cout<<"\n\nReceived a "<<reply<<" request\n\n";
    if(!reply.compare("MAPPER"))
    {
        Mapper m;
        m.init(socket);
        // cout<<"\nInitiating word count request!\n";
        // m.initiate_word_count_request("job1", 1, "input_files/file1.txt", 0, 0, 3);
        cout<<"\n\nInitiating inverted index request";
        vector<string> file_paths;
        file_paths.push_back("input_files/i_file1.txt");
        file_paths.push_back("input_files/i_file2.txt");
        file_paths.push_back("input_files/i_file3.txt");
        vector<int> start_lines;
        start_lines.push_back(54);
        start_lines.push_back(48);
        start_lines.push_back(0);
        vector<int> no_of_lines;
        no_of_lines.push_back(334);
        no_of_lines.push_back(100);
        no_of_lines.push_back(300);
        m.initiate_inverted_index_request("job2", 1, file_paths, start_lines, no_of_lines, 3);

        cout<<endl<<m.get_reply()<<endl;
    }
    
    if(!reply.compare("REDUCER"))
    {
        Reducer r;
        r.init(socket);
        // cout<<"\nGiving word count request!\n";
        // r.word_count_request("job3", 1, "input_files/wc_reducer1.txt",4);
        // r.word_count_request("job3", 1, "input_files/wc_reducer2.txt",4);
        // r.word_count_request("job3", 1, "input_files/wc_reducer3.txt",4);
        // r.word_count_request("job3", 1, "input_files/wc_reducer4.txt",4);
        cout<<"\n\nGiving inverted index request\n";
        r.inverted_index_request("job4", 1, "output_files/job2_reducer1.txt",4);
        r.inverted_index_request("job4", 1, "output_files/job2_reducer2.txt",4);
        r.inverted_index_request("job4", 1, "output_files/job2_reducer3.txt",4);
        r.inverted_index_request("job4", 1, "output_files/job2_reducer4.txt",4);

        cout<<endl<<r.get_reply()<<endl;
    }
    
    
}

int main(int argc, char * argv[])
{
    if(argc < 2)
    {
        cout<<"\n\nProvide ip and port\n\n";
        exit(1);
    }
    vector<string> ip_split = split_string(argv[1], ':');
    string ip_address = ip_split[0];
    int port_number = stoi(ip_split[1]);
    int init_socket, client_socket;
    struct sockaddr_in server_address, client_address;
    socklen_t client_length;
    init_socket = socket(AF_INET, SOCK_STREAM, 0);
    if(init_socket < 0)
    {
        cout<<"Process: "<<getpid()<<" "<<"Error while creating file socket\n";
        exit(1);
    }
    bzero((char *)(&server_address), sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(port_number);
    if( bind(init_socket, (struct sockaddr *) &server_address, sizeof(server_address)) < 0 )
    {
        cout<<"Process: "<<getpid()<<" "<<"Unable to bind to file server address!\n";
        exit(1);
    }
    listen(init_socket,10);
    client_length = sizeof(client_address);
    int thread_count = 0;
    thread all_threads[50];
    
    while( (client_socket = accept(init_socket, (struct sockaddr *) &client_address, &client_length)) )
    {
        cout<<"\n\nNew connection received\n\n";
        all_threads[thread_count] =  thread(process_request,client_socket);
        thread_count++;
    
    }
    cout<<"\nEXITING : CLIENT SOCKET : "<<client_socket<<endl;
    for(int i=0; i<thread_count; i++)
    {
        all_threads[i].join();
    }
}