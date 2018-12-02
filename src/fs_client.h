#ifndef _FILE_SERVER_H_
#define _FILE_SERVER_H_

#include "utilities.h"

#define FAILURE           -1
#define SUCCESS           0
#define MAX_SIZE          (512*1024)
#define FILE_EXISTS       1
#define MAX_CONNS         100
#define FILE_SERVER_IP    "127.0.0.1:6000"

enum Client_Request
{
    TOTAL_COUNT,
    GET_CHUNK,
    UPLOAD_FILE,
    APPEND_FILE,
    REMOVE_FILE
};

enum Server_Request
{
    GET_FILE
};

class FS_Client
{
private:
    int      m_client_port;
    std::string m_client_ip;
    int      m_server_port;
    std::string m_server_ip;
    std::string m_logfile_path;

public:
    FS_Client(std::string client_ip_addr, std::string server_ip_addr);
    int get_lines_count(std::string file_path);
    int get_chunk(std::string input_filename, std::string output_filename, int start_line, int line_count);
    int upload_file(std::string input_filename);
    int append_file(std::string src_filename, std::string dest_filename);
    void uploader();
    void server_request_handler(int sock, std::string req_str);
    void remove_file(std::string filename);
};

#endif  // _FILE_SERVER_H_
