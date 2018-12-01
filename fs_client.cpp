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
    string opcode = to_string(TOTAL_COUNT) + "$";

    m_file_socket = util_connection_make(m_file_ip, m_file_port);
    // TODO : CHECK for connection status

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
    // TODO : CHECK for connection status

    string opcode = to_string(GET_CHUNK) + "$";
    opcode += to_string(start_line) + "$";
    opcode += to_string(line_count) + "$";; 
    opcode += input_filename;
    util_write_to_sock(m_file_socket, opcode);
    util_read_data_into_file(m_file_socket, output_filename);
} 

int FS_Client::upload_file(string input_filename)
{
    m_file_socket = util_connection_make(m_file_ip, m_file_port);
    // TODO : CHECK for connection status

    string req_str = to_string(UPLOAD_FILE) + "$" + input_filename;
    util_write_to_sock(m_file_socket, req_str);
    // TODO: check write status

    int file_exists;
    read(m_file_socket, &file_exists, sizeof(file_exists));
    
    if(file_exists)
    {
        cout << "File " << input_filename <<" Already Exists!!! Please specify a different name\n";
        close(m_file_socket);
        m_file_socket = INVALID_SOCK;
        return FILE_EXISTS;
    }

    int file_size = util_file_size_get(input_filename);
    util_file_data_send(m_file_socket, input_filename, 0, file_size);
    close(m_file_socket);
    m_file_socket = INVALID_SOCK;
}

int main()
{
    int start_line = 1;
    int line_count = 3173441;
    FS_Client fs_client;
    // size_t cnt = 0;
    // cnt = f.get_lines_count("/home/kaushik/Coursework/OS/input.txt");
    // fs_client.get_chunk("input.txt", "output.txt", start_line, line_count);
    int upload_state = fs_client.upload_file("file.cpp");
    while(1);
    return 0;
}
