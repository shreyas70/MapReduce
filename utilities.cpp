#include "utilities.h"
#include "includes.h"

#include <sys/socket.h> 
//#include <cstdlib> 
#include <netinet/in.h> 
#include <arpa/inet.h>

using namespace std;

//extern string  working_dir;

void util_from_cursor_line_clear()
{
    cout << "\e[0K";
    cout.flush();
}

#if 0
string util_abs_path_get(string str)
{
    if(str[0] == '/')
        return str;

    char *str_buf = new char[str.length() + 1];
    strncpy(str_buf, str.c_str(), str.length());
    str_buf[str.length()] = '\0';

    string ret_path = working_dir, prev_tok = working_dir;
    if(str_buf[0] == '/')
        ret_path = "/";

    char *p_str = strtok(str_buf, "/");
    while(p_str)
    {
        string tok(p_str);
        if(tok == ".")
        {
            prev_tok = tok;
            p_str = strtok (NULL, "/");
        }
        else if(tok == "..")
        {
            ret_path.erase(ret_path.length() - 1);
            size_t fwd_slash_pos = ret_path.find_last_of("/");
            ret_path = ret_path.substr(0, fwd_slash_pos + 1);
            prev_tok = tok;
            p_str = strtok (NULL, "/");
        }
        else if (tok == "~")
        {
            ret_path = getenv("HOME");
            ret_path = ret_path + "/";
            prev_tok = tok;
            p_str = strtok (NULL, "/");
        }
        else if(tok == "")
        {
            if(!prev_tok.empty())
                ret_path = "/";
            p_str = strtok (NULL, "/");
        }
        else
        {
            p_str = strtok (NULL, "/");
            if(!p_str)
                ret_path += tok;
            else
                ret_path += tok + "/";
        }
    }

    return ret_path;
}
#endif

void util_ip_port_split(string addr, string &ip, int &port)
{
    unsigned int colon_pos = addr.find(':');
    if(colon_pos != string::npos)
    {
        ip = addr.substr(0, colon_pos);
        port = stoi(addr.substr(colon_pos + 1));
    }
}

int util_connection_make(string ip, uint16_t port)
{
    struct sockaddr_in serv_addr;
    int sock = 0;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        cout << "Socket connection error!!" << endl;
        return FAILURE;
    }

    memset(&serv_addr, '0', sizeof(serv_addr));

    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(port);

    // Convert IPv4 and IPv6 addresses from text to binary form 
    if(inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr)<=0)
    {
        cout << "Invalid address/ Address not supported" << endl;
        return FAILURE;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        cout << "Connection with tracker failed!!" << endl;
        return FAILURE;
    }

    return sock;
}

void util_data_read(int sock, char* read_buffer, int size_to_read)
{
    int bytes_read = 0;
    do
    {
        // read in a loop
        bytes_read += read(sock, read_buffer + bytes_read, size_to_read);
    }while(bytes_read < size_to_read);
}

int util_socket_data_get(int sock, string& buffer_str, string& error_msg)
{
    int read_size;
    int valread = read(sock, &read_size, sizeof(read_size));
    if(FAILURE == valread)
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): read() failed!!";
        error_msg = ss.str();
        return FAILURE;
    }
    if(0 == valread)
    {
        // Somebody disconnected, get his details and print
        //getpeername(sock , (struct sockaddr*)&m_sock_address, (socklen_t*)&addrlen);
        //stringstream ss;
        //ss << "Connection lost!! <ip_addr>:<port> -> " 
        //   << inet_ntoa(m_sock_address.sin_addr) << ":" << ntohs(m_sock_address.sin_port);
        error_msg = "Connection lost!!\n";
        return FAILURE;
    }
    char buffer[read_size + 1];
    memset(buffer, 0, sizeof(buffer));
    util_data_read(sock, buffer, read_size);
    buffer_str = buffer;
    return SUCCESS;
}


void util_write_to_sock(int sock, string data)
{
    int sz = data.length();
    send(sock, &sz, sizeof(sz), 0);
    send(sock, data.c_str(), data.length(), 0);
}
