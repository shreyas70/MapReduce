#include <iostream>  
#include <stdio.h>  
#include <string.h>       //strlen  
#include <stdlib.h>  
#include <errno.h>  
#include <unistd.h>       //close  
#include <arpa/inet.h>    //close  
#include <sys/types.h>  
#include <sys/socket.h>  
#include <netinet/in.h>  
#include <sys/time.h>
#include <signal.h>
#include <pthread.h>
#include <string>
#include <sstream>
#include <algorithm>
#include <thread>
#include <mutex>
#include "fs_server.h"

using namespace std;

/* Skips n lines and position the file pointer at the start of (n+1)th line */ 
void FS_Server::skip(istream & is, int n , char delim)
{
  int i = 0;
   while ( i++ < n)
      is.ignore(80, delim); 
}

/* Get bytes count between two lines */ 
int FS_Server::get_byte_count(ifstream &inFile, int line_count)
{
    int bytes = 0;
    string line = "";
    int itr = 0;
    if (!inFile)
    {
        cout << __func__ << ": File not open\n";
        return 0;
    }
    while(itr != line_count)
    {
        getline(inFile, line);
        bytes += line.size() + 1;
        itr++;
    }

    return bytes;
}

/* Count number of lines the specified file*/
void FS_Server::get_lines_count(int sock, string path)
{
    int lines = 0;

    mutex* file_mutex = file_mutex_get(path);
    lock_guard<mutex> lg(*file_mutex);
    if(!util_file_exists(path))
    {
        send(sock, &lines, sizeof(lines), 0);
        return;
    }

    ifstream inFile(path); 
    lines = count(istreambuf_iterator<char>(inFile), istreambuf_iterator<char>(), '\n');
    send(sock, &lines, sizeof(lines), 0);
} 

void FS_Server::download_file(string client_ip, int client_port, string src_filename, string dest_filename)
{
    int client_socket = util_connection_make(client_ip, client_port);
    if(client_socket == FAILURE) 
        cout << "connection create failed, "<<  __func__ << ", Line: " << __LINE__ << "\n";

    string req_str = to_string(GET_FILE) + "$" + src_filename;
    util_write_to_sock(client_socket, req_str);

    mutex* download_mtx = file_mutex_get(dest_filename);

    lock_guard<mutex> lg(*download_mtx);
    util_read_data_into_file(client_socket, dest_filename);

}

mutex* FS_Server::create_mutex(std::string filename) 
{
    std::mutex* m = new std::mutex;
    download_mtx_map[filename] = m;
    return m;
}

mutex* FS_Server::file_mutex_get(std::string filename)
{
    std::lock_guard<std::mutex> lg(download_map_mutex);
    auto itr = download_mtx_map.find(filename);
    if(itr == download_mtx_map.end())
    {
        return create_mutex(filename);
    }
    else
    {
        return itr->second;
    }
}


void FS_Server::client_upload_handler(int sock, string req_str)
{
    string client_ip;
    int client_port;
    vector<string> tokens = split_string(req_str, '$');

    client_ip = tokens[0];
    client_port = stoi(tokens[1]);
    string filename = tokens[2];
    
    bool exists = util_file_exists(filename);
    int file_exists;

    if(exists)
    {
        file_exists = 1;
        send(sock, &file_exists, sizeof(file_exists), 0);
    }
    else
    {
        file_exists = 0;
        send(sock, &file_exists, sizeof(file_exists), 0);
        thread th(&FS_Server::download_file, this, client_ip, client_port, filename, filename);
        th.detach();
    }
}


void FS_Server::client_append_handler(int sock, string req_str)
{
    vector<string> tokens = split_string(req_str,'$');
    string client_ip;
    int client_port;



    // input_split(req_str, tokens);

    client_ip = tokens[0];
    client_port = stoi(tokens[1]);
    string src_filename = tokens[2];
    string dest_filename = tokens[3];

    thread th(&FS_Server::download_file, this, client_ip, client_port, src_filename, dest_filename);
    th.detach();
}

void FS_Server::file_data_send(int sock, string filename, int pos, int byte_count)
{
    mutex* download_mtx = file_mutex_get(filename);
    lock_guard<mutex> lg(*download_mtx);
    util_file_data_send(sock, filename, pos, byte_count);
}

/* Handles client requests */ 
void FS_Server::client_request_handle(int sock, string req_str)
{
    cout << "\n" << req_str << endl;
    int dollar_pos = req_str.find_first_of('$'); 
    string cmd = req_str.substr(0, dollar_pos); // get the opcode
    req_str = req_str.erase(0, dollar_pos+1);   // remove the opcode part from input
    int req = stoi(cmd);

    switch(req)
    {
        case TOTAL_COUNT:
        {
            thread th(&FS_Server::get_lines_count, this, sock, req_str);
            th.detach();
            break;
        }

        case GET_CHUNK:
        {
            vector<string> tokens=split_string(req_str,'$');
            

            int start_line = stoi(tokens[0]); 
            int line_count = stoi(tokens[1]);
            string filename = tokens[2];

            ifstream inFile;
            inFile.open(filename, ios::binary | ios::in);

            skip(inFile, start_line - 1,'\n');

            long pos = inFile.tellg();
            int byte_count = get_byte_count(inFile, line_count);
            inFile.close();

            thread th(&FS_Server::file_data_send, this, sock, filename, pos, byte_count);
            th.detach();
            break;
        }

        case UPLOAD_FILE:
        {
            thread th(&FS_Server::client_upload_handler, this, sock, req_str);
            th.detach();
            break;
        }

        case APPEND_FILE:
        {
            thread th(&FS_Server::client_append_handler, this, sock, req_str);
            th.detach();
            break;
        }

        case REMOVE_FILE:
        {
            if (util_file_exists(req_str))
            {
                remove(req_str.c_str());
            }
            break;
        }

        default:
            break;
    }
}

/* Listens to requests in select loop */ 
void FS_Server::start_server()
{
    int opt = true;
    int server_socket, addrlen, new_socket, client_socket[MAX_CONNS], max_clients = MAX_CONNS, activity, i, valread, sd;
    int max_sd;
    struct sockaddr_in address;

    fd_set readfds;

    for (i = 0; i < max_clients; i++)
    {
        client_socket[i] = 0;
    }

    if( (server_socket = socket(AF_INET , SOCK_STREAM , 0)) == 0)
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): socket failed!!";
        cout<<(ss.str());
        exit(EXIT_FAILURE);
    }

    if( setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0 )
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): setsockopt";
        cout<<(ss.str());
        exit(EXIT_FAILURE);
    }

    //type of socket created  
    address.sin_family = AF_INET;   
    address.sin_addr.s_addr = INADDR_ANY;   
    address.sin_port = htons(PORT);   

    //bind the socket to localhost port 4500
    if (bind(server_socket, (struct sockaddr *)&address, sizeof(address))<0)   
    {   
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): bind failed";
        cout<<(ss.str());
        exit(EXIT_FAILURE);   
    }   
    stringstream ss;
    ss << "File Server Listening on port\n";
    cout<<(ss.str());
         
    //try to specify maximum of 100 pending connections for the master socket  
    if (listen(server_socket, MAX_CONNS) < 0)   
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): listen failed";
        cout<<(ss.str());
        exit(EXIT_FAILURE);   
    }   
         
    //accept the incoming connection  
    addrlen = sizeof(address);

    while(true)
    {   
        //clear the socket set  
        FD_ZERO(&readfds);   

        //add master socket to set  
        FD_SET(server_socket, &readfds);   
        max_sd = server_socket;   

        //add child sockets to set  
        for ( i = 0 ; i < max_clients ; i++)   
        {   
            //socket descriptor  
            sd = client_socket[i];   

            //if valid socket descriptor then add to read list  
            if(sd > 0)   
                FD_SET( sd , &readfds);   
                 
            //highest file descriptor number, need it for the select function  
            if(sd > max_sd)   
                max_sd = sd;   
        }   
     
        // wait for an activity on one of the sockets , timeout is NULL ,  
        // so wait indefinitely  
        activity = select( max_sd + 1 , &readfds , NULL , NULL , NULL);   
       
        if (activity < 0)
        {
            if(errno != EINTR)
            {
                stringstream ss;
                ss << __func__ << " (" << __LINE__ << "): select error";
                cout<<(ss.str());
            }
            continue;
        }

        // If something happened on the master socket ,  
        // then its an incoming connection  
        if (FD_ISSET(server_socket, &readfds))   
        {
            if ((new_socket = accept(server_socket, (struct sockaddr *)&address, (socklen_t*)&addrlen))<0)   
            {   
                stringstream ss;
                ss << __func__ << " (" << __LINE__ << "): accept() failed!!";
                cout<<(ss.str());
                exit(EXIT_FAILURE);   
            }   
             
            //add new socket to array of sockets  
            for (i = 0; i < max_clients; i++)   
            {
                //if position is empty
                if( client_socket[i] == 0 )
                {
                    client_socket[i] = new_socket;
                    break;   
                }
            }
        }
        else
        {
            //else its some IO operation on some other socket 
            for (i = 0; i < max_clients; i++)   
            {   
                sd = client_socket[i];   
                     
                if (FD_ISSET( sd , &readfds))
                {
                    string buffer_str, error_msg;
                    if (FAILURE == util_socket_data_get(sd, buffer_str, error_msg))
                    {
			            cout << error_msg << endl;
                        close(sd);
                        client_socket[i] = 0;
                        continue;
                    }
                    client_request_handle(sd, buffer_str);
                }
            }
        }
    }
}


int main()
{
    FS_Server fs_server;
    fs_server.start_server();
}
