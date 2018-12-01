#include "utilities.h"
#include "includes.h"

#include <sys/socket.h> 
//#include <cstdlib> 
#include <netinet/in.h> 
#include <arpa/inet.h>

using namespace std;




void util_from_cursor_line_clear()
{
    cout << "\e[0K";
    cout.flush();
}

string util_abs_path_get(string str)
{
    string  working_dir;

    working_dir = getenv("PWD");
    if(working_dir != "/")
        working_dir = working_dir + "/";

    
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

vector<string> split_string(string input_string, char delimiter)
{
    vector<string> output_vector;
    string curr_string = "";
    for(int i=0; i<input_string.length(); i++)
    {
        char curr_char = input_string[i];
        if(curr_char == delimiter)
        {
            output_vector.push_back(curr_string);
            curr_string = "";
        }
        else
        {
            curr_string+=curr_char;
        }
    }
    output_vector.push_back(curr_string);
    return output_vector;
}

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
        cout << "Socket connection error: " << errno << endl;
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
        cout << __func__ << ":" << __LINE__ << ": connect() failed!!" << endl;
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
    char buffer[read_size + 1] = {'\0'};
    util_data_read(sock, buffer, read_size);
    buffer_str = buffer;
    return SUCCESS;
}


void util_write_to_sock(int sock, string data)
{
    cout <<"trying to write" << endl;
    int sz = data.length();
    send(sock, &sz, sizeof(sz), 0);
    send(sock, data.c_str(), data.length(), 0);
}


/* Return chunk of a file  */
void util_file_data_send(int sock, string filename, long pos, int remaining_bytes)
{
    ifstream inFile;
    inFile.open(filename, ios::binary | ios::in);

    if (!inFile)
    {
        cout << __func__ << ":" << __LINE__ << ": File open() failed!!\n ";
        return;
    }
    inFile.seekg(pos);

    int chunk_size = 0;
    while (remaining_bytes > 0)
    {
        string buffer_str;
        chunk_size = remaining_bytes <= MAX_CHUNK_SIZE ? remaining_bytes : MAX_CHUNK_SIZE;
        remaining_bytes -= util_file_data_read(inFile, chunk_size, buffer_str);
        util_write_to_sock(sock, buffer_str);
    }
}

void util_complete_file_data_send(int sock, string filename)
{
    int file_size = util_file_size_get(filename);
    util_file_data_send(sock, filename, 0, file_size);
    cout <<"Uploaded complete file on FS\n";
}

int util_file_data_read(ifstream &inFile, int chunk_size, string& buffer_str)
{
    int bytes_read = 0, total_bytes_read = 0;
    char temp_buff[chunk_size + 1];
    memset(temp_buff, 0, sizeof(temp_buff));

    do{
        cout << "To Read: " << chunk_size << ", Done Read: " << bytes_read << endl;
        inFile.read(temp_buff + total_bytes_read, chunk_size);
        bytes_read = inFile.gcount();
        total_bytes_read += bytes_read;
        chunk_size -= bytes_read;

        if(inFile.fail() && !inFile.eof())
        {
            cout << "Error: (" << __func__ << ") (" << __LINE__ << "): " << strerror(errno);
            return 0;
        }
    }while(chunk_size);

    buffer_str = temp_buff;
    return total_bytes_read;
}

size_t  util_file_size_get(string filename)
{
    ifstream in(filename, ios_base::binary);
    in.seekg(0, ios_base::end);
    return in.tellg();
}

bool util_file_exists(string filename)
{
    const char *fname = filename.c_str();
    struct stat buffer;
    return (stat (fname, &buffer) == 0);
}

void util_read_data_into_file(int m_file_socket, string filename)
{
    ofstream out(filename, ios_base::binary | ios_base::app);
    // TODO: check file op
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
        out << buffer_str;
        if (buffer_str.length() < MAX_CHUNK_SIZE)
        {
            close(m_file_socket);
            m_file_socket = INVALID_SOCK;
            break;
        }
    }
}
