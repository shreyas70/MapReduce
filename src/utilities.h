#ifndef _UTILITIES_H_
#define _UTILITIES_H_

#include <string>
#include <vector>

//#include <unistd.h>
//#include <termios.h>
//#include <dirent.h>
//#include <sys/ioctl.h>
//#include <sys/stat.h>

//#include <sstream>         // stringstream
//#include <fstream>
//#include <list>
//#include <stack>
//#include <string>
//#include <string.h>
//#include <iostream>
//#include <ftw.h>
//#include <limits>
//#include <cstdint>

#define pb        push_back
#define INT_MAX  (std::numeric_limits<std::int32_t>::max())

#define FAILURE   -1
#define SUCCESS   0

enum class Problem
{
    WORD_COUNT,
    INVERTED_INDEX
};

enum class Opcode
{
    CLIENT_REQUEST,
    MAPPER_CONNECTION,
    REDUCER_CONNECTION,
    MAPPER_SUCCESS,
    MAPPER_FAILURE,
    REDUCER_SUCCESS,
    REDUCER_FAILURE
};

void                      util_from_cursor_line_clear();
std::string               util_abs_path_get(std::string str);
void                      util_ip_port_split(std::string addr, std::string &ip, int &port);
int                       util_connection_make(std::string ip, uint16_t port);
void                      util_data_read(int sock, char* read_buffer, int size_to_read);
int                       util_socket_data_get(int sock, std::string& buffer_str, std::string& error_msg);
void                      util_write_to_sock(int sock, std::string data);
std::vector<std::string>  split_string(std::string input_string, char delimiter);

#endif
