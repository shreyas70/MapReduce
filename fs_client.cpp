#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <cmath>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
//#include <iomanip>
#include <vector>
#include <sys/ioctl.h>
#include <stdio.h> 
#include <sys/socket.h> 
#include <netinet/in.h> 
#include <arpa/inet.h>
#include <string>

#include "fs_client.h"
#include "utilities.h"

using namespace std;

/* Get nummber of lines from FS */ 
int FS_Client::get_lines_count(string file_path)
{
    int num = 0;
    int size = 0;
    int status;
    string opcode = "0$";

    m_file_socket = util_connection_make(m_file_ip, m_file_port);

    opcode += file_path;
    util_write_to_sock(m_file_socket, opcode);

    status = read(m_file_socket, &num, sizeof(num));

    if(FAILURE == status || 0 == status)
    {
        /* Some error occurred */ 
        num = -1;
    }

    close(m_file_socket);
    return num;
}

void FS_Client::get_chunk(string input_filename, string output_filename, int start_line, int line_count)
{
    int status;
    int bytes_to_read = 0;
    char buffer[MAX_SIZE];        
    memset(buffer, 0, sizeof(char)*MAX_SIZE);

    m_file_socket = util_connection_make(m_file_ip, m_file_port);

    ofstream outfile(output_filename, std::ios_base::app);

    string opcode = "1$";
    opcode += to_string(start_line) + "$";
    opcode += to_string(line_count) + "$";; 
    opcode += input_filename;
    util_write_to_sock(m_file_socket, opcode);

    while(true)
    {
        string buffer_str, error_msg;
        if (FAILURE == util_socket_data_get(m_file_socket, buffer_str, error_msg))
        {
            cout << error_msg << endl;
            close(m_file_socket);
            m_file_socket = INVALID_SOCK;
            break;
        }
        outfile << buffer_str;
        if (buffer_str.length() < MAX_SIZE)
        {
            close(m_file_socket);
            m_file_socket = INVALID_SOCK;
            break;
        }
        #if 0
        status = read(m_file_socket, &bytes_to_read, sizeof(bytes_to_read));
        cout<<bytes_to_read<<endl;
        if(FAILURE == status || 0 == status)
        {
            /* Some error occurred */ 
            cout<<"Error Occurred\n";
            break ;
        }

        else
        {
            int itr = bytes_to_read/MAX_SIZE;
            int last_iter_read = MAX_SIZE;
            int to_read = MAX_SIZE;

            if(bytes_to_read % MAX_SIZE != 0)
            {
                last_iter_read = bytes_to_read % MAX_SIZE;
                itr++;
            }

            for(int  i = 0;i < itr; i++)
            {
                if(i == itr - 1)
                    to_read = last_iter_read;

                util_data_read(m_file_socket, buffer, to_read);
                outfile << buffer;
                memset(buffer, 0, sizeof(char)*MAX_SIZE);
            }

        }
        #endif
    }  
} 

int main()
{
    int start_line = 1;
    int line_count = 3173441;
    FS_Client fs_client;
    // size_t cnt = 0;
    // cnt = f.get_lines_count("/home/kaushik/Coursework/OS/input.txt");
    fs_client.get_chunk("../input.txt", "output.txt", start_line, line_count);
    return 0 ;
}
