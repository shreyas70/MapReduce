#include "includes.h"
#include "utilities.h"

#include <cstdio>
#include <cassert>
// #include <openssl/sha.h>
#include <vector>
#include <map>
#include <thread>
#include <mutex>
#include <algorithm>
#include <set>
#include <sstream>

#include <string.h> 
#include <signal.h>


#include "utilities.h"
#include "master_worker.h"

#include "Mapper.h"




using namespace std;

#define PIECE_SIZE     (512 * 1024)
#define pb             push_back

#define REDUCER             0
#define MAX_DOWNLOADS       100
#define MAX_CLIENTS         10

// static int            num_mappers_alive = MAX_CLIENTS;

string working_dir;
vector<Mapper> mappers;


string current_timestamp_get()
{
    time_t tt;
    struct tm *ti;

    time (&tt);
    ti = localtime(&tt);
    return asctime(ti);
}


void MasterTracker::log_print(string msg)
{
    ofstream out(m_log_path, ios_base::app);
    if(!out)
    {
        stringstream ss;
        ss << "Error: (" << __func__ << ") (" << __LINE__ << "): " << strerror(errno);
        //status_print(FAILURE, ss.str());
        return;
    }
    string curr_timestamp = current_timestamp_get();
    curr_timestamp.pop_back();
    out << curr_timestamp << " : " << "\"" << msg << "\"" << "\n";
}


void MasterTracker::replyToHeartBeat()
{
    
    
    Mapper m = mappers[0];
    cout << "heart beat thread initiated working! " << endl;
    while(true)
    {
        cout<<"\nHeart beat received : "<<m.receive_heart_beat()<<endl;
        // m.receive_heart_beat();
        m.reply_to_heart_beat();
    }
    exit(0);
}


void MasterTracker::client_request_handler(int client_sock, string req_str)
{
    stringstream ss;
    ss << "Handling request: " << req_str;
    log_print(ss.str());

    // currently only taking "problem_id$job_id$file_path" in the request buffer
    int dollar_pos = req_str.find('$');
    string cmd = req_str.substr(0, dollar_pos);
    Problem problem = (Problem)(stoi(cmd));

    switch(problem)
    {
        case Problem::WORD_COUNT:
        {
            string file_path = req_str.erase(0, dollar_pos+1);
           
            if(mappers.empty())
            {
                cout << "No mapper objects in vector" << endl;
            }
            else
            {
                Mapper m = mappers[0];
                
                cout << "reached ! File path received -" << file_path<<"-" <<endl;
                // cout << "available slots " << z.get_available_slots() << endl;
                m.initiate_word_count_request("job1", file_path, 54, 43);

            }
        
            


            // job_file_add(job_id, req_str);



            // if(num_job_files(job_id) == num_mappers_alive)
            // {
            //     thread counting_thread(&MasterTracker::count_all_words, this, job_id);
            //     counting_thread.detach();
            // }
            break;
        }

        case Problem::INVERTED_INDEX:
            break;

        default:
            break;
    }
}


void MasterTracker::run()
{
    int opt = true;
    int addrlen , new_sock , client_socks[MAX_CLIENTS],
        activity, valread , sd;
    int max_sd;

    fd_set readfds;

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        client_socks[i] = 0;
    }

    if((m_sock = socket(AF_INET , SOCK_STREAM , 0)) == 0)
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): socket failed!!";
        log_print(ss.str());
        exit(EXIT_FAILURE);
    }

    // set r_tracker's socket to allow multiple connections from mappers,
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
    ss << "MasterTracker listening on " << m_ip_addr << ":" << m_port;
    log_print(ss.str());


    connectToMappers();
         
    // try to specify maximum of pending connections for r_tracker's socket
    if (listen(m_sock, MAX_CLIENTS) < 0)   
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
        for (int i = 0 ; i < MAX_CLIENTS ; i++)
        {
            // socket descriptor
            sd = client_socks[i];

            // if valid socket descriptor then add to read fd list
            if(sd > 0)
                FD_SET(sd , &readfds);

            // highest file descriptor number, need it for the select function
            if(sd > max_sd)
                max_sd = sd;
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
            for (int i = 0; i < MAX_CLIENTS; i++)
            {
                // if position is empty
                if(client_socks[i] == 0)
                {
                    client_socks[i] = new_sock;
            
                    stringstream ss;
                    ss << "New client with socket id " << new_sock << " connected\n";
                    log_print(ss.str());
                    break;
                }
            }
        }
        else
        {
            // else its some IO operation on some other socket
            for (int i = 0; i < MAX_CLIENTS; i++)
            {
                sd = client_socks[i];

                if (FD_ISSET( sd , &readfds))
                {
                    int read_size;
                    valread = read(sd, &read_size, sizeof(read_size));
                    if(FAILURE == valread)
                    {
                        close(sd);
                        client_socks[i] = 0;

                        stringstream ss;
                        ss << __func__ << " (" << __LINE__ << "): read() failed!!";
                        log_print(ss.str());
                    }
                    else if(0 == valread)
                    {
                        // Somebody disconnected, get his details and print
                        getpeername(sd , (struct sockaddr*)&m_sock_address, (socklen_t*)&addrlen);
                        stringstream ss;
                        ss << "Client disconnected!! <ip_addr>:<port> -> " 
                           << inet_ntoa(m_sock_address.sin_addr) << ":" << ntohs(m_sock_address.sin_port);
                        log_print(ss.str());

                        close(sd);
                        client_socks[i] = 0;
                        // --num_mappers_alive;
                    }
                    else
                    {
                        char buffer[read_size + 1] = {'\0'};
                        util_data_read(sd, buffer, read_size);

                        stringstream ss;
                        ss << "Request read: " << buffer;
                        log_print(ss.str());

                        // getpeername(sd , (struct sockaddr*)&m_sock_address, (socklen_t*)&addrlen);
                        
                        client_request_handler(sd, buffer);
                    }
                }
            }
        }
    }
}

void MasterTracker::connectToMappers()
{
    Mapper m1;
    m1.connect_to_mapper("127.0.0.1", 7000);
    mappers.push_back(m1);


    Mapper m2;
    m2.connect_to_mapper("127.0.0.1", 70001);
    mappers.push_back(m2);

    thread hb = thread(&MasterTracker::replyToHeartBeat, this);
    hb.detach();
}

int main(int argc, char* argv[])
{
    if(argc != 3)
    {
        //status_print(FAILURE, "Reducer Launch command : \"/master <MASTER_IP>:<PORT> <worker_log_file>\"");
        cout << endl;
        return 0;
    }

    MasterTracker master;
    master.log_path_set(util_abs_path_get(argv[2]));         // USE NFS PATH HERE
    master.ip_addr_set(argv[1]);

    master.run();

    return 0;
}
