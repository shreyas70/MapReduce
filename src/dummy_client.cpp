#include <iostream>
#include <string>
#include <cstring>
#include <cstdint>
#include <fstream>

#include <sys/socket.h>
#include <cstdlib>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>

#include "master_client.h"

using namespace std;

string working_dir;

int main(int argc, char* argv[])
{
    string m_ip_addr;
    string buffer;
    string error;
    int m_port;

    working_dir = getenv("PWD");
    if(working_dir != "/")
        working_dir = working_dir + "/";

    util_ip_port_split(argv[1], m_ip_addr, m_port);

    MasterClient m;

    // if(FAILURE == master.sock_get())
    //     return FAILURE;

    // string file_path = util_abs_path_get(argv[2]);
    
    vector<string> filesList;
    filesList.push_back("input_files/i_file1.txt");
    filesList.push_back("input_files/i_file2.txt");
    filesList.push_back("input_files/i_file3.txt");

    m.connect_as_client(m_ip_addr, m_port,Problem::INVERTED_INDEX, filesList);

    
    // m.get_request();

    if (FAILURE == util_socket_data_get(m.sock_get(), buffer, error))
    {
        // log_print(error_msg);
        close(m.sock_get());
        cout << " read failed" << endl;
       
    }
    else
    {
        cout << buffer << endl;

    }

    // response_handler(sock, buffer_str);

    

    

    // cout << read()
    // master.connect_as_reducer();
    // master.connect_as_mapper();

    // while(1);

    return 0;
}
