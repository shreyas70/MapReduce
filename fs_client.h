#ifndef _FILE_SERVER_H_
#define _FILE_SERVER_H_

#include "utilities.h"

#define FAILURE   -1
#define SUCCESS   0
#define MAX_SIZE  (512*1024)
#define FILE_EXISTS 1

enum Request_Type
{
    TOTAL_COUNT,
    GET_CHUNK,
    UPLOAD_FILE
};

class FS_Client
{
private:
    size_t      m_file_port = 4000;
    std::string m_file_ip = "127.0.0.1";
    size_t      m_file_socket = 0;

public:

    int get_lines_count(std::string file_path);
    void get_chunk(std::string input_filename, std::string output_filename, int start_line, int line_count);
    int upload_file(std::string input_filename);
};

#endif  // _FILE_SERVER_H_
