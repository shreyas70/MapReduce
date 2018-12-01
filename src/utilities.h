#ifndef _UTILITIES_H_
#define _UTILITIES_H_

#include <string>
#include <fstream>
#include <sys/stat.h>
#include <vector>

#define pb        push_back
#define INT_MAX  (std::numeric_limits<std::int32_t>::max())

#define FAILURE         -1
#define SUCCESS         0
#define INVALID_SOCK    -1
#define MAX_CHUNK_SIZE  (512 * 1024)

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
void                      util_file_data_send(int sock, std::string filename, long pos, int remaining_bytes);
int                       util_file_data_read(std::ifstream &inFile, int chunk_size, std::string& buffer_str);
size_t                    util_file_size_get(std::string filename);
bool                      util_file_exists(std::string filename);
void                      util_read_data_into_file(int m_file_socket, std::string filename);
void                      util_complete_file_data_send(int sock, std::string filename);
std::vector<std::string>  split_string(std::string input_string, char delimiter);

#endif
