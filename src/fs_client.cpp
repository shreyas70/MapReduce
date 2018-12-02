#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <cmath>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <vector>
#include <sys/ioctl.h>
#include <stdio.h> 
#include <sys/socket.h> 
#include <netinet/in.h> 
#include <arpa/inet.h>
#include <string>
#include <thread>
#include <sstream>

#include "fs_client.h"
#include "utilities.h"

using namespace std;

FS_Client::FS_Client(string client_ip_addr, string server_ip_addr)
{
    util_ip_port_split(client_ip_addr, m_client_ip, m_client_port);
    util_ip_port_split(server_ip_addr, m_server_ip, m_server_port);

    thread th(&FS_Client::uploader, this);
    th.detach();
}


void FS_Client::server_request_handler(int sock, string req_str)
{
    cout << "\n" << req_str << endl;
    int dollar_pos = req_str.find_first_of('$'); 
    string cmd = req_str.substr(0, dollar_pos); // get the opcode
    req_str = req_str.erase(0, dollar_pos+1);   // remove the opcode part from input
    int req = stoi(cmd);

    switch(req)
    {
        case GET_FILE:
        {
            thread th(util_complete_file_data_send, sock, req_str);   
            th.detach();
            break;
        }

        default:
            break;
    }
}


void FS_Client::uploader()
{
    int opt = true;
    int fs_client_socket, addrlen, new_socket, fs_server_socket[MAX_CONNS], max_conns = MAX_CONNS, activity, i, valread, sd;
    int max_sd;
    struct sockaddr_in address;

    fd_set readfds;

    for (i = 0; i < max_conns; i++)
    {
        fs_server_socket[i] = 0;
    }

    if( (fs_client_socket = socket(AF_INET , SOCK_STREAM , 0)) == 0)
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): socket failed!!";
        cout<<(ss.str());
        exit(EXIT_FAILURE);
    }

    //set master socket to allow multiple connections ,
    //this is just a good habit, it will work without this
    if( setsockopt(fs_client_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0 )
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): setsockopt";
        cout<<(ss.str());
        exit(EXIT_FAILURE);
    }

    //type of socket created  
    address.sin_family = AF_INET;   
    address.sin_addr.s_addr = INADDR_ANY;   
    address.sin_port = htons(m_client_port);   

    //bind the socket to localhost port 4500
    if (bind(fs_client_socket, (struct sockaddr *)&address, sizeof(address))<0)   
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
    if (listen(fs_client_socket, MAX_CONNS) < 0)   
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
        FD_SET(fs_client_socket, &readfds);   
        max_sd = fs_client_socket;   

        //add child sockets to set  
        for ( i = 0 ; i < max_conns ; i++)   
        {   
            //socket descriptor  
            sd = fs_server_socket[i];   

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
        if (FD_ISSET(fs_client_socket, &readfds))   
        {
            if ((new_socket = accept(fs_client_socket, (struct sockaddr *)&address, (socklen_t*)&addrlen))<0)   
            {   
                stringstream ss;
                ss << __func__ << " (" << __LINE__ << "): accept() failed!!";
                cout<<(ss.str());
                exit(EXIT_FAILURE);   
            }   
             
            //add new socket to array of sockets  
            for (i = 0; i < max_conns; i++)   
            {
                //if position is empty
                if( fs_server_socket[i] == 0 )
                {
                    fs_server_socket[i] = new_socket;
                    break;   
                }
            }
        }
        else
        {
            //else its some IO operation on some other socket 
            for (i = 0; i < max_conns; i++)   
            {   
                sd = fs_server_socket[i];   
                     
                if (FD_ISSET( sd , &readfds))
                {
                    string buffer_str, error_msg;
                    if (FAILURE == util_socket_data_get(sd, buffer_str, error_msg))
                    {
			            // util_file_log_print(m_logfile_path, error_msg);
                        cout << error_msg << endl;
                        close(sd);
                        fs_server_socket[i] = 0;
                        continue;
                    }
                    server_request_handler(sd, buffer_str);
                }
            }
        }
    }
}

/* Get nummber of lines from FS, Returns -1 if failed */ 
int FS_Client::get_lines_count(string file_path)
{
    int num = 0;
    int size = 0;
    int status;
    string opcode = to_string(TOTAL_COUNT) + "$";

    int server_sock = util_connection_make(m_server_ip, m_server_port);
    if(FAILURE == server_sock)
    {
        return FAILURE;
    }

    opcode += file_path;
    util_write_to_sock(server_sock, opcode);

    status = read(server_sock, &num, sizeof(num));

    if(FAILURE == status || 0 == status)
    {
        /* Some error occurred */ 
        num = FAILURE;
        // stringstream ss;
        cout << __func__ << ":" << __LINE__ << " : read() Failed";
        // util_file_log_print(m_logfile_path, ss.str());

        close(server_sock);
        return 0;
    }

    close(server_sock);
    return num;
}

void FS_Client::get_chunk(string input_filename, string output_filename, int start_line, int line_count)
{
    int status;
    int bytes_to_read = 0;
    char buffer[MAX_SIZE];        
    memset(buffer, 0, sizeof(char)*MAX_SIZE);

    int server_sock = util_connection_make(m_server_ip, m_server_port);
    if(server_sock == FAILURE)
    {
        // stringstream ss;
        cout << __func__ << ":" << __LINE__ << " util_connection_make() :  Failed";
        // util_file_log_print(m_logfile_path, ss.str());
        return;
    }

    string opcode = to_string(GET_CHUNK) + "$";
    opcode += to_string(start_line) + "$";
    opcode += to_string(line_count) + "$";; 
    opcode += input_filename;
    util_write_to_sock(server_sock, opcode);
    util_read_data_into_file(server_sock, output_filename);
} 

int FS_Client::upload_file(string input_filename)
{
    int server_sock = util_connection_make(m_server_ip, m_server_port);
    if(FAILURE == server_sock)
    {
        // stringstream ss;
        cout << __func__ << ":" << __LINE__ << " util_connection_make() :  Failed";
        // util_file_log_print(m_logfile_path, ss.str());
        return FAILURE;
    }
    string req_str = to_string(UPLOAD_FILE) + "$" + m_client_ip + "$" + to_string(m_client_port) + "$" + input_filename;
    util_write_to_sock(server_sock, req_str);

    int file_exists;
    read(server_sock, &file_exists, sizeof(file_exists));
    
    if(file_exists)
    {
        cout << "File " << input_filename <<" Already Exists!!! Please specify a different name\n";
        close(server_sock);
        server_sock = INVALID_SOCK;
        return FILE_EXISTS;
    }

    close(server_sock);
    server_sock = INVALID_SOCK;
    return SUCCESS;
}

int FS_Client::append_file(string src_filename, string dest_filename)
{
    int server_sock = util_connection_make(m_server_ip, m_server_port);
    if(server_sock == FAILURE)
    {
        // stringstream ss;
        cout << __func__ << ":" << __LINE__ << " util_connection_make() :  Failed";
        // util_file_log_print(m_logfile_path, ss.str());
        return FAILURE;
    }

    string req_str = to_string(APPEND_FILE) + "$" + m_client_ip + "$" + to_string(m_client_port) + "$" + src_filename + "$" + dest_filename ;
    util_write_to_sock(server_sock, req_str);

    close(server_sock);
    server_sock = INVALID_SOCK;
    return SUCCESS;
}

void FS_Client::remove_file(string filename)
{
    int server_sock = util_connection_make(m_server_ip, m_server_port);
    if(server_sock == FAILURE)
    {
        // stringstream ss;
        cout << __func__ << ":" << __LINE__ << " util_connection_make() :  Failed";
        // util_file_log_print(m_logfile_path, ss.str());
        return;
    }

    string req_str = to_string(REMOVE_FILE) + "$" + filename;
    util_write_to_sock(server_sock, req_str);

    close(server_sock);
    server_sock = INVALID_SOCK;
}

