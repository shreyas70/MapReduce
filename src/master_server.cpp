#include "includes.h"
#include "utilities.h"

#include <cstdio>
#include <cassert>
#include <vector>
#include <map>
#include <thread>
#include <mutex>
#include <algorithm>
#include <set>
#include <sstream>
#include <cmath>

#include <string.h> 
#include <signal.h>
#include <queue>
#include <set>
#include <list>

#include "utilities.h"
#include "master_server.h"

using namespace std;


string current_timestamp_get()
{
    time_t tt;
    struct tm *ti;

    time (&tt);
    ti = localtime(&tt);
    return asctime(ti);
}


int num_lines_get(string filePath)
{
    int count = 0;
    string line;
 
    /* Creating input filestream */ 
    ifstream file(filePath);
    while (getline(file, line))
        count++;
 
    cout << "Numbers of lines in the file : " << count << endl;
    return count;
}



void Master::log_print(string msg)
{
    ofstream out(m_log_path, ios_base::app);
    if(!out)
    {
        stringstream ss;
        ss << "[Master] Error: (" << __func__ << ") (" << __LINE__ << "): " << strerror(errno);
        //status_print(FAILURE, ss.str());
        return;
    }
    string curr_timestamp = current_timestamp_get();
    curr_timestamp.pop_back();
    out << "[Master] " << curr_timestamp << " : " << "\"" << msg << "\"" << "\n";
}


/* return the opcode of the incoming request */
int Master::request_handler(int sock, string req_str, Opcode& opcode)
{
    stringstream ss;
    ss << __func__ << ":" << __LINE__ << " : Handling request: " << req_str;
    log_print(ss.str());

    ss.str("");
    ss << req_str;
    
    string opcode_str;
    getline(ss, opcode_str, '$');

    cout << opcode_str << endl;
    opcode = (Opcode)(stoi(opcode_str));

    switch(opcode)
    {
        case Opcode::CLIENT_REQUEST:
            return client_request_handler(sock, req_str);

        case Opcode::MAPPER_CONNECTION:
        {
            Mapper* new_mapper = new Mapper();
            new_mapper->init(sock);
            mapper_list.push_back(new_mapper);
            break;
        }

        case Opcode::REDUCER_CONNECTION:
            //Reducer* new_reducer = new Reducer(sock);
            //reducer_list.push_back(new_reducer);
            break;

        default:
            break;
    }
    return SUCCESS;
}

int Master::client_request_handler(int client_sock, string req_str)
{
    // currently only taking "opcode$problem_id$file_path" in the request string
    // opcode has to be ignored
    vector <string> tokens_vec;
    stringstream ss(req_str);
    string intermediate;

    // Tokenizing w.r.t. space '$'
    while(getline(ss, intermediate, '$'))
    {
        tokens_vec.push_back(intermediate);
    }

    Problem problem = (Problem)(stoi(tokens_vec[1]));

    switch(problem)
    {
        case Problem::WORD_COUNT:
        {
            string file_path = tokens_vec[2];

            if(mapper_list.empty())
            {
                cout << "No mapper objects in vector" << endl;
                return FAILURE;
            }
            //***check if file is valid***
            if( access( file_path.c_str() , R_OK ) == -1) {
                cout << "File doesn't exists. Terminating request." << endl;
                break;
            } 
            //pendingJobs.push(file_path);

            int total_lines = num_lines_get(file_path);
            int chunk_num_lines = ceil((double) (total_lines / mapper_list.size()));

            Job* new_job = new Job(client_sock, mapper_list.size(), 4); // TODO: use reducer_list.size()
            jobs_map[new_job->job_id] = new_job;

            int curr_line_num = 1, end_line_num = 1, chunk_id = 0, num_lines;
            for(auto litr = mapper_list.begin(); litr != mapper_list.end(); ++litr, ++chunk_id)
            {
                Chunk* new_chunk;
                int mapper_socket = (*litr)->get_socket();
                if (curr_line_num > total_lines)
                {
                    end_line_num = curr_line_num - 1;
                }
                else
                {
                    end_line_num = min(curr_line_num + chunk_num_lines, total_lines);
                }
                num_lines = end_line_num - curr_line_num + 1;

                new_chunk = new Chunk(chunk_id, curr_line_num, num_lines, new_job->job_id, mapper_socket);
                new_job->chunks[chunk_id] = new_chunk;
                mapper_chunks_map[mapper_socket].insert({new_job->job_id, chunk_id});

                (*litr)->initiate_word_count_request(new_job->job_id,
                                                     new_chunk->chunk_id,
                                                     file_path,
                                                     curr_line_num,
                                                     num_lines,
                                                     new_job->num_reducers);

                
                curr_line_num = end_line_num + 1;

                stringstream ss;
                ss << __func__ << " (" << __LINE__ << "): Inititate WC - mapper socket: " << mapper_socket;
                log_print(ss.str());
            }
            break;
        }

        case Problem::INVERTED_INDEX:
            break;

        default:
            break;
    }
    return SUCCESS;
}


void Master::response_handler(int sock, string response_str)
{
    stringstream ss;
    ss << "Handling response: " << response_str << endl;
    log_print(ss.str());

    ss.str("");
    ss << response_str;
    
    vector <string> tokens_vec;
    string intermediate;

    // Tokenizing w.r.t '$'
    while(getline(ss, intermediate, '$'))
    {
        tokens_vec.push_back(intermediate);
    }

    Opcode opcode = (Opcode)(stoi(tokens_vec[0]));

    switch(opcode)
    {
        case Opcode::MAPPER_SUCCESS:
        {
            string job_id_str = tokens_vec[1];
            int job_id = stoi(job_id_str);
            int chunk_id = stoi(tokens_vec[2]);

            cout << "Mapper sock:" << sock << " response:\n";
            for (unsigned int i = 3; i < tokens_vec.size(); ++i)
            {
                 cout << "Job ID : " <<  job_id << "| chunk ID :  " << chunk_id << "| " << tokens_vec[i] << endl <<flush ;

            }
               
#if 0
            string file_path;
            int i = 0;
            while(1)
            {
                dollar_pos = response_str.find('$');
                file_path = response_str.substr(0, dollar_pos);
                if (dollor_pos == response_str.length())
                {
                    break;
                }
                response_str.erase(0, dollar_pos+1);

                category[i].push_back(file_path);
                send_to_reducer(reducer_sockets[reducer_of_category[i]], job_id
            }

            Problem problem = (Problem)(stoi(problem_str));
            response_str.erase(0, dollar_pos+1);

            case Problem::WORD_COUNT:
            {
                string input_file_path = response_str;
                break;
            }

            case Problem::INVERTED_INDEX:
            {
                break;
            }
#endif
            break;
        }

        case Opcode::MAPPER_FAILURE:
        {
            break;
        }

        default:
            break;
    }
}


void Master::run()
{
    int opt = true;
    int addrlen , new_sock , client_sockets[MAX_CONNECTIONS],
        activity, sd;                 // sd is used for general connections
    int max_sd;

    fd_set readfds;

    for (int i = 0; i < MAX_CONNECTIONS; i++)
    {
        client_sockets[i] = 0;
    }

    if((m_sock = socket(AF_INET , SOCK_STREAM , 0)) == 0)
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): socket failed!!";
        log_print(ss.str());
        exit(EXIT_FAILURE);
    }

    // set master's socket to allow multiple connections from clients, mappers and reducers
    // this is just a good habit, it will work without this
    if( setsockopt(m_sock, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0 )
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): setsockopt";
        log_print(ss.str());
        exit(EXIT_FAILURE);
    }

    // type of socket created  
    m_sock_address.sin_family = AF_INET;
    m_sock_address.sin_addr.s_addr = INADDR_ANY;
    m_sock_address.sin_port = htons(m_port);

    // bind the socket to r_tracker's port
    if (bind(m_sock, (struct sockaddr *)&m_sock_address, sizeof(m_sock_address))<0)   
    {   
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): bind failed";
        log_print(ss.str());
        exit(EXIT_FAILURE);
    }   
    stringstream ss;
    ss << "Master listening on " << m_ip_addr << ":" << m_port;
    log_print(ss.str());
         
    // try to specify maximum of pending connections for r_tracker's socket
    if (listen(m_sock, MAX_CONNECTIONS) < 0)
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): listen failed";
        log_print(ss.str());
        exit(EXIT_FAILURE);   
    }   

    // accept the incoming connection  
    addrlen = sizeof(m_sock_address);

    while(true)
    {
        // clear the socket set
        FD_ZERO(&readfds);   

        // add r_tracker's socket to the read fd set
        FD_SET(m_sock, &readfds);
        max_sd = m_sock;

        // add mapper sockets to set
        for (int i = 0 ; i < MAX_CONNECTIONS ; i++)
        {
            // socket descriptor
            sd = client_sockets[i];

            // if valid socket descriptor then add to read fd list
            if(sd > 0)
                FD_SET(sd , &readfds);

            // highest file descriptor number, need it for the select function
            if(sd > max_sd)
                max_sd = sd;
        }
        for(Mapper* m : mapper_list)
        {
            int sock = m->get_socket();
            if(sock > 0)
            {
                FD_SET(sock , &readfds);
                if(sock > max_sd)
                    max_sd = sock;
            }
        }
        #if 0
        for(Reducer* r : reducer_list)
        {
            int sock = r->get_socket();
            if(sock > 0)
            {
                FD_SET(sock , &readfds);
                if(sock > max_sd)
                    max_sd = sock;
            }
        }
        #endif

        // wait for an activity on one of the sockets , timeout is NULL,
        // so wait indefinitely
        activity = select(max_sd + 1 , &readfds , NULL , NULL , NULL);

        if (activity < 0)
        {
            if(errno != EINTR)
            {
                stringstream ss;
                ss << __func__ << " (" << __LINE__ << "): select error: " << errno;
                log_print(ss.str());
            }
            continue;
        }

        // If something happened on the master socket ,  
        // then its an incoming connection  
        if (FD_ISSET(m_sock, &readfds))   
        {
            new_sock = accept(m_sock, (struct sockaddr *) &m_sock_address, (socklen_t*) &addrlen);
            if(FAILURE == new_sock)
            {
                stringstream ss;
                ss << __func__ << " (" << __LINE__ << "): accept() failed!!";
                log_print(ss.str());
                exit(EXIT_FAILURE);
            }

            // add new socket to array of sockets
            for (int i = 0; i < MAX_CONNECTIONS; i++)
            {
                // if position is empty
                if(client_sockets[i] == 0)
                {
                    client_sockets[i] = new_sock;

                    stringstream ss;
                    ss << "New client with socket id " << new_sock << " connected\n";
                    log_print(ss.str());
                    break;
                }
            }
        }
        else
        {
            auto litr = mapper_list.begin();
            while(litr != mapper_list.end())
            {
                int sock = (*litr)->get_socket();
                if (FD_ISSET(sock , &readfds))
                {
                    string buffer_str, error_msg;
                    if (FAILURE == util_socket_data_get(sock, buffer_str, error_msg))
                    {
                        log_print(error_msg);

                        close(sock);
                        mapper_list.erase(litr++);
                        continue;
                    }

                    response_handler(sock, buffer_str);
                }
                ++litr;
            }
            #if 0
            for(Reducer* r : reducer_list)
            {
                int sock = r->get_socket();
                if (FD_ISSET(sock , &readfds))
                {
                    string buffer_str, error_msg;
                    if (FAILURE == util_socket_data_get(sock, buffer_str, error_msg))
                    {
                        log_print(error_msg);
                        continue;
                    }

                    response_handler(sock, buffer_str);
                }
            }
            #endif

            // else its some IO operation on some other socket
            for (int i = 0; i < MAX_CONNECTIONS; i++)
            {
                sd = client_sockets[i];
                if (FD_ISSET(sd , &readfds))
                {
                    string buffer_str, error_msg;
                    if (FAILURE == util_socket_data_get(sd, buffer_str, error_msg))
                    {
                        log_print(error_msg);
                        close(sd);
                        client_sockets[i] = 0;
                        continue;
                    }

                    Opcode opcode;
                    if (FAILURE == request_handler(sd, buffer_str, opcode))
                    {
                        // TODO: inform the client and close its connection
                    }
                    else
                    {
                        if((opcode == Opcode::MAPPER_CONNECTION) ||
                           (opcode == Opcode::REDUCER_CONNECTION))
                            client_sockets[i] = 0;
                    }
                }
            }
        }
    }
}

int main(int argc, char* argv[])
{
    if(argc != 3)
    {
        //status_print(FAILURE, "Reducer Launch command : \"/master <MASTER_IP>:<PORT> <worker_log_file>\"");
        cout << endl;
        return 0;
    }

    

    Master master;
    master.log_path_set(util_abs_path_get(argv[2]));         // USE NFS PATH HERE
    master.ip_addr_set(argv[1]);

    master.run();

    return 0;
}
