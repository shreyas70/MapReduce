#include <string>
#include <cstdio>
#include <sys/socket.h>

#include "includes.h"
#include "master_client.h"
#include "utilities.h"
#include <vector>

using namespace std;

int MasterClient::connect_as_client(string ip_address, int port_number, Problem problem, vector<string> files)
{
    
    m_sock = util_connection_make(ip_address, port_number);
    if (FAILURE == m_sock)
        return FAILURE;
    string client_opcode = to_string((int)Opcode::CLIENT_REQUEST);
    string problem_str = to_string((int)problem);
    string req = client_opcode + "$" + problem_str;

    if(problem==Problem::WORD_COUNT)
    {
        if(files.size()!=1)
        {
            cout << "Give only one file for word count problem" << endl;
            return FAILURE;
        }
        req+= "$"+files[0];
    }
    else
    {
        for(auto i:files)
        {
            req+="$"+i;
        }
    }


    util_write_to_sock(m_sock, req);
    return SUCCESS;
}


int MasterClient::connect_as_mapper(string ip_address, int port_number)
{
    m_sock = util_connection_make(ip_address, port_number);
    if (FAILURE == m_sock)
        return FAILURE;

    string req = to_string((int)Opcode::MAPPER_CONNECTION);
    util_write_to_sock(m_sock, req);
    return SUCCESS;
}


int MasterClient::connect_as_reducer(string ip_address, int port_number)
{
    m_sock = util_connection_make(ip_address, port_number);
    if (FAILURE == m_sock)
        return FAILURE;
    string req = to_string((int)Opcode::REDUCER_CONNECTION);
    cout << "sending request from reducer :" << req << endl;
    util_write_to_sock(m_sock, req);
    return SUCCESS;
}

int MasterClient::get_request(string &reply)
{
    int read_size;
    int ret = read(this->m_sock, &read_size, sizeof(read_size));
    if (FAILURE == ret)
    {
        cout << __func__ << " (" << __LINE__ << "): read() failed: errno: " << errno << endl;
        close(m_sock);
        m_sock = FAILURE;
        return FAILURE;
    }
    if (0 == ret)
    {
        cout << "Connection with master lost!!\n";
        close(m_sock);
        m_sock = FAILURE;
        return FAILURE;
    }

    char buff[read_size+1];
    bzero(buff,read_size+1);
    read(this->m_sock, buff, read_size);
    reply = buff;

    return SUCCESS;
}


void MasterClient::job_completed_mapper(int job_id, int chunk_id, vector<string> reducer_files)
{
    // int socket_fd = *(this->sock);
    string reply_string = to_string((int)(Opcode::MAPPER_SUCCESS));
   
   reply_string += "$" + to_string(job_id) + "$" + to_string(chunk_id);

    for(int i=0; i<reducer_files.size(); i++)
    {
        reply_string += "$" + reducer_files[i];
    }

    int write_bytes = reply_string.length();
    send(this->m_sock, &write_bytes, sizeof(write_bytes), 0);
    send(this->m_sock, reply_string.c_str(), reply_string.length(), 0);
}



void MasterClient::job_completed_reducer(int job_id, int category, string outputFile)
{
    // int socket_fd = *(this->sock);
    
    string reply_string = to_string((int)(Opcode::REDUCER_SUCCESS));
   
    reply_string += "$" + to_string(job_id) + "$" + to_string(category)+ "$" + outputFile;
    cout << "Reducer completed. Sending reply :" << reply_string << endl;

    int write_bytes = reply_string.length();
    send(this->m_sock, &write_bytes, sizeof(write_bytes), 0);
    send(this->m_sock, reply_string.c_str(), reply_string.length(), 0);
}

void MasterClient::job_failure_mapper(int job_id, int chunk_id)
{
    string reply_string = to_string((int)(Opcode::MAPPER_FAILURE));
    reply_string += "$" + to_string(job_id) + "$" + to_string(chunk_id);

    int write_bytes = reply_string.length();
    send(this->m_sock, &write_bytes, sizeof(write_bytes), 0);
    send(this->m_sock, reply_string.c_str(), reply_string.length(), 0);

}

void MasterClient::job_failure_reducer(int job_id, int category)
{
    string reply_string = to_string((int)(Opcode::REDUCER_FAILURE));
    reply_string += "$" + to_string(job_id) + "$" + to_string(category);

    int write_bytes = reply_string.length();
    send(this->m_sock, &write_bytes, sizeof(write_bytes), 0);
    send(this->m_sock, reply_string.c_str(), reply_string.length(), 0);
}
