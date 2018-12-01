#ifndef _FS_SERVER_H_
#define _FS_SERVER_H_

#include <vector>
#include <string>
#include <fstream>
#include <mutex>
#include <map>

#include "utilities.h"

#define FAILURE             -1
#define SUCCESS             0
#define MAX_CONNS           100
#define PORT                4000
#define IP                  "127.0.0.1"
#define MAX_SIZE            (512*1024)

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

class FS_Server
{
    std::map<std::string, std::mutex*> download_mtx_map;
    std::mutex  download_map_mutex;

    std::mutex* create_mutex(std::string filename);
    std::mutex* file_mutex_get(std::string filename);

public:
    void skip(std::istream & is, int n , char delim);
    void input_split(std::string input, std::vector<std::string>& tokens);
    /* Get bytes count between two lines */ 
    int get_byte_count(std::ifstream &inFile, int line_count);
    
    int util_file_data_read(std::ifstream &inFile, int sock, int bytes_to_read);

    /* Count number of lines the specified file*/
    void get_lines_count(int sock, std::string path);

    /* Handles client requests */ 
    void client_request_handle(int sock, std::string req_str);

    /* Appends data from src file to destination file */ 
    void append_file(int sock, std::string dest_filename);

    /* handles client request to upload file */ 
    void client_upload_handler(int sock, std::string req_str);

    void client_append_handler(int sock, std::string req_str);
    
    /* Sends download request to client and download*/ 
    void download_file(std::string client_ip, int client_port, std::string src_filename, std::string dest_filename);

    void file_data_send(int sock, std::string filename, int pos, int byte_count);

    /* Listens to requests in select loop */ 
    void start_server();
};

#endif  // _FS_SERVER_H_
