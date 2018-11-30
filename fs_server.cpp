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

#define MAX_CONNS           100
#define PORT                4000
#define IP                  "127.0.0.1"
#define MAX_SIZE            (512*1024)

using namespace std;


/* Skips n lines and position the file pointer at the start of (n+1)th line */ 
void FS_Server::skip(istream & is, int n , char delim)
{
  int i = 0;
   while ( i++ < n)
      is.ignore(80, delim); 
// ignores up to 80 chars but stops ignoring after delim
// istream stream position var is changed. (we want that)
}

/* split the input into tokens */
void FS_Server::input_split(string input, vector<string>& tokens)
{
    int len = input.length();
    char arr[len+1];
    strcpy(arr, input.c_str());

    char* tok = strtok(arr, "$");
    while(tok != NULL)
    {
        tokens.push_back(tok);
        tok = strtok(NULL,"$");
    }

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
        // cout<<line<<":"<<bytes<<flush;
        itr++;
    }

#if 0
    if(! getline(inFile, line))
    {
        bytes -= 1;
    }
#endif
    // cout<<bytes<<flush;
    return bytes;
}

int FS_Server::util_file_data_read(ifstream &inFile, int sock, int bytes_to_read)
{
    int bytes_read = 0, total_bytes_read = 0;
    char temp_buff[MAX_SIZE + 1];
    memset(temp_buff, 0, sizeof(temp_buff));

    do{
        cout << "To Read: " << bytes_to_read << ", Done Read: " << bytes_read << endl;
        inFile.read(temp_buff + total_bytes_read, bytes_to_read);
        bytes_read = inFile.gcount();
        total_bytes_read += bytes_read;
        bytes_to_read -= bytes_read;

        if(inFile.fail() && !inFile.eof())
        {
            cout << "Error: (" << __func__ << ") (" << __LINE__ << "): " << strerror(errno);
            return 0;
        }
    }while(bytes_to_read);

    util_write_to_sock(sock, temp_buff);
    return total_bytes_read;
}


/* Return chunk of a file  */ 
void FS_Server::file_chunks_upload(int sock, string input)
{
    vector<string> tokens;
    input_split(input, tokens);

    int start_line = stoi(tokens[0]); 
    int line_count = stoi(tokens[1]);
    string file_path = tokens[2];

    ifstream inFile;
    inFile.open(file_path, ios::binary | ios::in);

    skip(inFile, start_line - 1,'\n');

    long pos = inFile.tellg();
    cout << "Pos: " << pos << endl;
    int byte_count = get_byte_count(inFile, line_count);
    // cout<<byte_count; // debug
    inFile.clear();
    inFile.seekg(pos);

    if (!inFile)
    {
        cout << __func__ << ":" << __LINE__ << ": File open() failed!!\n ";
        return;
    }

    // send(sock, &byte_count, sizeof(byte_count), 0);
    int bytes_to_read = 0;
    while (byte_count > 0)
    {
        bytes_to_read = byte_count <= MAX_SIZE ? byte_count : MAX_SIZE;
        byte_count -= util_file_data_read(inFile, sock, bytes_to_read);
    }
}


/* Count number of lines the specified file*/
void FS_Server::get_lines_count(int sock, string path)
{
    int lines = 0;

    ifstream inFile(path); 
    lines = count(istreambuf_iterator<char>(inFile), istreambuf_iterator<char>(), '\n');
    cout<<lines<<endl;
    send(sock, &lines, sizeof(lines), 0);
} 

/* Handles client requests */ 
void FS_Server::client_request_handle(int sock, string req_str)
{
    int dollar_pos = req_str.find_first_of('$'); 
    string cmd = req_str.substr(0, dollar_pos); // get the opcode
    req_str = req_str.erase(0, dollar_pos+1); // remove the opcode part from input
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
            thread th(&FS_Server::file_chunks_upload, this, sock, req_str);
            th.detach();
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

    // signal(SIGUSR1, sigusr1_handler);

    // seeding_files_share();

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

    //set master socket to allow multiple connections ,
    //this is just a good habit, it will work without this
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
			            cout << "FAILED READ" << endl;
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
