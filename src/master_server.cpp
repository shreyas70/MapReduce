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
#include "fs_client.h"

using namespace std;

FS_Client fs_client("127.0.0.1:3999", "127.0.0.1:6000");

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
    // cout << "Checking if new right is called" << endl;

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
        {
            Reducer* new_reducer = new Reducer();
            new_reducer->init(sock);
            reducer_list.push_back(new_reducer);
            new_reducer->get_socket();
            cout << "New reducer added. Socket : " << new_reducer->get_socket()<<endl;
            break;
        }

        default:
            break;
    }
    return SUCCESS;
}

int Master::client_request_handler(int client_sock, string req_str)
{

    if(mapper_list.empty())
    {
        cout << "[Error] No Mappers in the system!!" << endl;
        return FAILURE;
    }
    if (reducer_list.empty())
    {
        cout << "[Error] No Reducers in the system!!" << endl;
        return FAILURE;
    }    

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
            // check if file is valid
            // if( access( file_path.c_str() , R_OK ) == -1) 
            if(util_file_exists(file_path))
            {
                cout << "File doesn't exists. Terminating request." << endl;
                return FAILURE;
            }


            int total_lines = fs_client.get_lines_count(file_path);
            // int total_lines = num_lines_get(file_path);
            int chunk_num_lines = ceil(((double) total_lines / mapper_list.size()));

            Job* new_job = new Job(client_sock, mapper_list.size(), reducer_list.size());
            jobs_map[new_job->job_id] = new_job;

            new_job->problem_id = Problem::WORD_COUNT;
            new_job->input_filenames.push_back(file_path);

            //updating reducer of category array
            //initially, number of categories will be equal to number of reducers
            
            for(auto x:reducer_list)
            {
                new_job->reducer_of_category.push_back(x);
            }

            //testing. Printing reducer sockets
            #if 0
            cout << "Printing alloted reducers" << endl;
            for(int i=0;i<new_job->num_reducers;i++)
            {
                cout << " Category " << i << " | Reducer Socket " << new_job->reducer_of_category[i]->get_socket() << endl; 
                // new_job->reducer_of_category.push_back(*reducer_iterator);
                // reducer_iterator++;
            }
            #endif

            

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
                    end_line_num = min(curr_line_num + chunk_num_lines - 1, total_lines);
                }
                num_lines = end_line_num - curr_line_num + 1;

                //new_chunk = new Chunk(chunk_id, curr_line_num, num_lines, new_job->job_id, mapper_socket);
                new_chunk = new Chunk(chunk_id, new_job->job_id, mapper_socket);
                new_chunk->start_line_vec.push_back(curr_line_num);
                new_chunk->num_lines_vec.push_back(num_lines);

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
        {
            for(int i=2;i<tokens_vec.size();i++)
            {
                if(access(tokens_vec[i].c_str(), R_OK) == -1)
                {
                    cout << "Invalid files. Terminating request" << endl;
                    return FAILURE;
                }
            }

            Job* new_job = new Job(client_sock, mapper_list.size(), reducer_list.size());
            jobs_map[new_job->job_id] = new_job;

            new_job->problem_id = Problem::INVERTED_INDEX;


           

            for(auto x:reducer_list)
            {
                new_job->reducer_of_category.push_back(x);
            }

            vector<int> total_lines(tokens_vec.size()-2);
            vector<int> chunk_num_lines(tokens_vec.size()-2);
            vector<int> curr_line_num(tokens_vec.size()-2,1);
            vector<int> end_line_num(tokens_vec.size()-2,1);
            vector<int> num_lines(tokens_vec.size()-2);

            for(int i=2,j=0;i<tokens_vec.size();i++,j++)
            {
                new_job->input_filenames.push_back(tokens_vec[i]);
                total_lines[j] = num_lines_get(tokens_vec[i]);
                chunk_num_lines[j] = ceil( ((double)total_lines[j] / mapper_list.size())); 
            }

            int chunk_id = 0;
            for(auto litr = mapper_list.begin(); litr != mapper_list.end(); ++litr, ++chunk_id)
            {
                Chunk* new_chunk;
                int mapper_socket = (*litr)->get_socket();
                new_chunk = new Chunk(chunk_id, new_job->job_id, mapper_socket);
                for(int fileNumber=0;fileNumber<curr_line_num.size();fileNumber++)
                {
                    if (curr_line_num[fileNumber] > total_lines[fileNumber])
                    {
                        end_line_num[fileNumber] = curr_line_num[fileNumber] - 1;
                    }
                    else
                    {
                        end_line_num[fileNumber] = min(curr_line_num[fileNumber] + chunk_num_lines[fileNumber] - 1, total_lines[fileNumber]);
                    }

                    num_lines[fileNumber] = end_line_num[fileNumber] - curr_line_num[fileNumber] + 1;

                    new_chunk->start_line_vec.push_back(curr_line_num[fileNumber]);
                    new_chunk->num_lines_vec.push_back(num_lines[fileNumber]);

                    curr_line_num[fileNumber] = end_line_num[fileNumber] + 1;
                    //new_chunk = new Chunk(chunk_id, curr_line_num, num_lines, new_job->job_id, mapper_socket);
                    
                }
                // tokens_vec.erase(tokens_vec.begin());
                // tokens_vec.erase(tokens_vec.begin());

                // vector<int> v = {1,2,3,4,5};
                vector<string> filePathVector(tokens_vec.begin() + 2, tokens_vec.end());
        
                
                new_job->chunks[chunk_id] = new_chunk;
                mapper_chunks_map[mapper_socket].insert({new_job->job_id, chunk_id});

                cout << "Printing allocation of inverted index for a mapper" << endl;
                cout << "Job ID " << new_job->job_id << " | Chunk ID " << new_chunk->chunk_id<<endl;

                for(int i=0;i<tokens_vec.size()-2;i++)
                {
                    cout << "File " <<i << " start_line "  << new_chunk->start_line_vec[i]  << " | Num of lines " << new_chunk->num_lines_vec[i] << endl;    
                }

                (*litr)->initiate_inverted_index_request(new_job->job_id,
                                                     new_chunk->chunk_id,
                                                     filePathVector,
                                                     new_chunk->start_line_vec,
                                                     new_chunk->num_lines_vec,
                                                     new_job->num_reducers);

                for(int i=0;i<curr_line_num.size();i++)
                {
                    curr_line_num[i] = end_line_num[i] + 1;
                }
               

                stringstream ss;
                ss << __func__ << " (" << __LINE__ << "): Inititate WC - mapper socket: " << mapper_socket;
                log_print(ss.str());
            }
            break;
        }

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

            cout << "Mapper sock:" << sock << " replied\n";
            Job* job = jobs_map[job_id];

            cout << "Got reply from mapper. Sending files to reducers " << endl;    
            for (unsigned int i = 3, catID=0; i < tokens_vec.size(); ++i,catID++)
            {
                Reducer* r = job->reducer_of_category[catID];
                cout << "Job ID : " <<  job_id << "| chunk ID :  " << chunk_id << "| " << tokens_vec[i] << endl <<flush;
                job->category_files[catID].push_back(tokens_vec[i]);
                reducer_category_map[r->get_socket()].insert({job->job_id, catID});
                if(job->problem_id == Problem::INVERTED_INDEX)
                {
                    r->inverted_index_request(job->job_id,catID,tokens_vec[i],job->num_mappers);
                }
                else
                {
                    r->word_count_request(job->job_id,catID,tokens_vec[i],job->num_mappers);
                }
            }

            //removing from mapper map
            set<pair<int,int>> &chunks_set = mapper_chunks_map[sock];
            chunks_set.erase({job_id,chunk_id});

            break;
        }

        case Opcode::MAPPER_FAILURE:
        {
            break;
        }

        case Opcode::REDUCER_SUCCESS:
        {
            
            cout << "Reduced replied success" << endl;
            string job_id_str = tokens_vec[1];
            int job_id = stoi(job_id_str);
            int cat_id = stoi(tokens_vec[2]);

            cout << "Reducer sock:" << sock << " replied success" << endl;;
            Job* job = jobs_map[job_id];

            job->num_successful_reductions++;

            //removing from reducer map
            set<pair<int,int>> &cateogry_set = reducer_category_map[sock];
            cateogry_set.erase({job_id,cat_id});

            if(job->num_successful_reductions == job->num_reducers)
            {
                //job done
                switch(job->problem_id)
                {
                    case Problem::WORD_COUNT:
                        util_write_to_sock(job->client_socket, "Your job for the file " + job->input_filenames[0] +" is done! Output file : " + to_string(job->job_id) + "_output.txt");
                        break;

                    case Problem::INVERTED_INDEX:
                    {
                        string files="";
                        for(auto fileName:job->input_filenames)
                        {
                            files+= fileName +",";

                        }
                        util_write_to_sock(job->client_socket, "Your job for files :" +files  + " is done! Output file : " + to_string(job->job_id) + "_output.txt");
                        break;
                    }

                    default:
                        break;
                }
                jobs_map.erase(job->job_id);
                delete job;
            }
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

                        mapper_list.erase(litr++);

                        // /*HANDLIING MAPPER NODE FAILURE*/
                        int old_mapper_socket = sock;
                        
                        set<pair<int,int>> old_mapper_set = mapper_chunks_map[old_mapper_socket];
                        mapper_chunks_map.erase(old_mapper_socket);

                        int no_of_mappers = mapper_list.size();
                        
                        // cout << " Number of mappers left after mapper sock " <<  << " failure " << no_of_mappers <<endl;

                        if(no_of_mappers <=0)
                        {
                            
                            close(sock);
                            continue;
                        }
                        


                        string debug_string = "Mapper on socket: "+to_string(old_mapper_socket)+" failed! "+to_string(no_of_mappers)+" mappers left";
                        log_print(debug_string);

                        for(set<pair<int,int>>::iterator jit = old_mapper_set.begin(); jit!=old_mapper_set.end(); ++jit)
                        {
                            int new_mapper_index = (rand() % no_of_mappers);

                            auto mlitr = mapper_list.begin();
                            for(int sub_index = 0; sub_index<new_mapper_index; sub_index++)
                            {
                                ++mlitr;
                            }
                            Mapper * current_mapper = *mlitr;
                            int current_socket = current_mapper->get_socket();
                            set<pair<int,int>> current_mapper_set = mapper_chunks_map[current_socket];
                            pair<int,int> job_chunk_pair = *jit;
                            current_mapper_set.insert(job_chunk_pair);
                            mapper_chunks_map[current_socket] = current_mapper_set;
                            
                            int curr_job_id = jit->first;
                            int curr_chunk_id = jit->second;

                            Job * job_obj = jobs_map[curr_job_id];
                            Chunk ** chunks = job_obj->chunks;
                            Chunk * curr_chunk = chunks[curr_chunk_id];
                            curr_chunk->mapper_sock = current_socket;

                            string curr_file_path = job_obj->input_filenames[0];
                            int curr_start_line = curr_chunk->start_line_vec[0];
                            int curr_no_of_lines = curr_chunk->num_lines_vec[0];
                            int curr_num_reducers = job_obj->num_reducers;

                            debug_string = "Reassigning job "+to_string(curr_job_id)+" chunk "+to_string(curr_chunk_id)+" to mapper on socket : "+to_string(current_socket);
                            log_print(debug_string);
                            
                            if(job_obj->problem_id == Problem::WORD_COUNT)
                            {
                                current_mapper->initiate_word_count_request(curr_job_id,
                                                                            curr_chunk_id,
                                                                            curr_file_path,
                                                                            curr_start_line,
                                                                            curr_no_of_lines,
                                                                            curr_num_reducers);

                            }
                            
                            // else if(job_obj->problem_id == INVERTED_INDEX)
                            // {
                            //     current_mapper->initiate_inverted_index_request();
                            // }
                        }

                        close(sock);
                        continue;
                    }

                    response_handler(sock, buffer_str);
                }
                ++litr;
            }

            // for(Reducer* r : reducer_list)
            // {
            //     int sock = r->get_socket();
            //     if (FD_ISSET(sock , &readfds))
            //     {
            //         string buffer_str, error_msg;
            //         if (FAILURE == util_socket_data_get(sock, buffer_str, error_msg))
            //         {
            //             log_print(error_msg);

                    


            //             continue;
            //         }

            //         response_handler(sock, buffer_str);
            //     }
            // }

            auto r_litr = reducer_list.begin();
            while(r_litr != reducer_list.end())
            {
                int sock = (*r_litr)->get_socket();
                if (FD_ISSET(sock , &readfds))
                {
                    string buffer_str, error_msg;
                    if (FAILURE == util_socket_data_get(sock, buffer_str, error_msg))
                    {
                        log_print(error_msg);

                        reducer_list.erase(r_litr++);

                        int old_reducer_socket = sock;
                        
                        set<pair<int,int>> old_reducer_set = reducer_category_map[old_reducer_socket];
                        reducer_category_map.erase(old_reducer_socket);

                        int no_of_reducers = reducer_list.size();
                        
                        if(no_of_reducers <=0)
                        {
                            close(sock);
                            continue;
                        }

                        string debug_string = "Reducer on socket: "+to_string(old_reducer_socket)+" failed! "+to_string(no_of_reducers)+" mappers left";
                        log_print(debug_string);

                        for(set<pair<int,int>>::iterator jit = old_reducer_set.begin(); jit!=old_reducer_set.end(); ++jit)
                        {
                            int new_reducer_index = (rand() % no_of_reducers);

                            auto rlitr = reducer_list.begin();
                            for(int sub_index = 0; sub_index<new_reducer_index; sub_index++)
                            {
                                ++rlitr;
                            }
                            Reducer * current_reducer = *rlitr;
                            int current_socket = current_reducer->get_socket();
                            set<pair<int,int>> current_reducer_set = reducer_category_map[current_socket];
                            pair<int,int> job_category_pair = *jit;
                            current_reducer_set.insert(job_category_pair);
                            reducer_category_map[current_socket] = current_reducer_set;
                            
                            int curr_job_id = jit->first;
                            int curr_category_id = jit->second;

                            Job * job_obj = jobs_map[curr_job_id];
                            // Chunk ** chunks = job_obj->chunks;
                            // Chunk * curr_chunk = chunks[curr_chunk_id];
                            // curr_chunk->mapper_sock = current_socket;
                            job_obj->reducer_of_category[curr_category_id] = current_reducer;

                            // string curr_file_path = job_obj->input_file_path;
                            // int curr_start_line = curr_chunk->start_line;
                            // int curr_no_of_lines = curr_chunk->num_lines;
                            // int curr_num_reducers = job_obj->num_reducers;

                            vector<string> file_paths = job_obj->category_files[curr_category_id];
                            int curr_num_mappers = job_obj->num_mappers;

                            debug_string = "Reassigning job "+to_string(curr_job_id)+" category "+to_string(curr_category_id)+" to reducer on socket : "+to_string(current_socket);
                            log_print(debug_string);
                            
                            if(job_obj->problem_id == Problem::WORD_COUNT)
                            {
                                 for(int fi = 0; fi<file_paths.size(); fi++)
                                 {
                                     current_reducer->word_count_request(curr_job_id, curr_category_id, file_paths[fi], curr_num_mappers);
                                 }                              
                            }
                            
                            // else if(job_obj->problem_id == INVERTED_INDEX)
                            // {
                            //     current_mapper->initiate_inverted_index_request();
                            // }
                        }

                        close(sock);
                        continue;
                    }

                    response_handler(sock, buffer_str);
                }
                ++r_litr;
            }

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
