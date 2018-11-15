#include "includes.h"
#include "common.h"

#include <cstdio>
#include <cassert>
#include <openssl/sha.h>
#include <vector>
#include <map>
#include <thread>
#include <mutex>
#include <algorithm>
#include <set>
#include <sstream>

#include <sys/socket.h> 
#include <cstdlib> 
#include <netinet/in.h> 
#include <arpa/inet.h>
#include <string.h> 
#include <signal.h>
//#include <pthread.h>

#include "reducer_tracker.h"

using namespace std;

#define PIECE_SIZE     (512 * 1024)
#define pb             push_back

#define FAILURE             -1
#define SUCCESS             0
#define ENTER               10
#define ESC                 27
#define UP                  11
#define DOWN                12
#define RIGHT               13
#define LEFT                14
#define BACKSPACE           127
#define SEEDING_FILES_LIST  "seeding_files.txt"
#define REDUCER             0
#define MAX_DOWNLOADS       100
#define MAX_MAPPERS         4

static ReducerTracker r_tracker;
static int            num_alive_mappers = MAX_MAPPERS;



static int cursor_r_pos;
static int cursor_c_pos;
static int cursor_left_limit = 1;
static int cursor_right_limit = 1;
string working_dir;
static struct termios prev_attr, new_attr;

map<string, string>   seeding_files_map;
map<string, set<int>> seeding_file_chunks;
map<string, string>   files_downloading;
map<string, string>   files_downloaded;

static bool is_status_on;


static string reducer_log_path, seeding_files_path, client_exec_path;
static string addr[3];
static string ip[3];
static int port[3];
static int curr_tracker_id;
static mutex download_mtx[MAX_DOWNLOADS];
static mutex g_mtx;
static bool mtx_inuse[MAX_DOWNLOADS];

enum operation
{
    ADD,
    REMOVE
};

enum client_to_tracker_req
{
    SHARE,
    GET,
    REMOVE_TORRENT,
    REMOVE_ALL
};

enum client_to_client_req
{
    GET_CHUNK_IDS,
    GET_CHUNKS
};

//void ip_and_port_split(string addr, string &ip, int &port);
void data_read(int sock, char* read_buffer, int size_to_read);

void cursor_init()
{
    cout << "\033[" << cursor_r_pos << ";" << cursor_c_pos << "H";
    cout.flush();
}

void status_print(int result, string msg)
{
    if(is_status_on)
        return;

    is_status_on = true;
    cursor_c_pos = cursor_left_limit;
    cursor_init();

    from_cursor_line_clear();
    if(FAILURE == result)
        cout << "\033[1;31m" << msg << "\033[0m";	// RED color
    else
        cout << "\033[1;32m" << msg << "\033[0m";	// GREEN color

    cout.flush();
}

string current_timestamp_get()
{
    time_t tt;
    struct tm *ti;

    time (&tt);
    ti = localtime(&tt);
    return asctime(ti);
}


void ReducerTracker::log_print(string msg)
{
    ofstream out(log_path, ios_base::app);
    if(!out)
    {
        stringstream ss;
        ss << "Error: (" << __func__ << ") (" << __LINE__ << "): " << strerror(errno);
        status_print(FAILURE, ss.str());
        return;
    }
    string curr_timestamp = current_timestamp_get();
    curr_timestamp.pop_back();
    out << curr_timestamp << " : " << "\"" << msg << "\"" << "\n";
}


int command_size_check(vector<string> &v, unsigned int min_size, unsigned int max_size, string error_msg)
{
    if(v.size() < min_size || v.size() > max_size)
    {
        status_print(FAILURE, error_msg);
        return FAILURE;
    }
    return SUCCESS;
}

int make_connection(string ip, uint16_t port)
{
    struct sockaddr_in serv_addr; 
    int sock = 0;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) 
    { 
        status_print(FAILURE, "Socket connection error!!");
        return FAILURE; 
    } 

    memset(&serv_addr, '0', sizeof(serv_addr)); 

    serv_addr.sin_family = AF_INET; 
    serv_addr.sin_port = htons(port);

    // Convert IPv4 and IPv6 addresses from text to binary form 
    if(inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr)<=0)  
    { 
        status_print(FAILURE, "Invalid address/ Address not supported");
        return FAILURE;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) 
    { 
        status_print(FAILURE, "Connection with tracker failed!!");
        return FAILURE; 
    } 
 
    return sock;
}

int send_request(int sock, int req, string str)
{
    string req_op = to_string(req);
    str = req_op + "$" + str;
    int sz = str.length();
    send(sock, &sz, sizeof(sz), 0);
    send(sock, str.c_str(), str.length(), 0); 
    return SUCCESS;
}

void seeding_files_recreate()
{
    ofstream out(seeding_files_path, ios_base::trunc);
    if(!out)
    {
        stringstream ss;
        ss << "Error: (" << __func__ << ") (" << __LINE__ << "): " << strerror(errno);
        status_print(FAILURE, ss.str());
        return;
    }
    for(auto itr = seeding_files_map.begin(); itr != seeding_files_map.end(); ++itr)
    {
        out << itr->first << "$" << itr->second << "\n";
    }
}

void update_seeding_map_n_file(operation opn, string double_sha1_str, string file_path = "", string mtorrent_file_path = "")
{
    switch(opn)
    {
        case ADD:
        {
            ofstream out(seeding_files_path, ios_base::app);
            if(mtorrent_file_path.empty())
            {
                seeding_files_map[double_sha1_str] = file_path;
                out << double_sha1_str << "$" << file_path << "\n";
            }
            else
            {
                seeding_files_map[double_sha1_str] = file_path + "$" + mtorrent_file_path;
                out << double_sha1_str << "$" << file_path << "$" << mtorrent_file_path << "\n";
            }
            break;
        }
        case REMOVE:
        {
            seeding_files_map.erase(double_sha1_str);
            seeding_files_recreate();
            break;
        }
        default:
            break;
    }
}

void data_read(int sock, char* read_buffer, int size_to_read)
{
    int bytes_read = 0;
    stringstream ss;
    ss << this_thread::get_id();
    do
    {
        bytes_read += read(sock, read_buffer + bytes_read, size_to_read);   // read in a loop
        //fprint_log("Bytes read: " + to_string(bytes_read) + " thread id: " + ss.str());
    }while(bytes_read < size_to_read);
}


#if 0
void ip_and_port_split(string addr, string &ip, int &port)
{
    int colon_pos = addr.find(':');
    if(colon_pos != string::npos)
    {
        ip = addr.substr(0, colon_pos);
        port = stoi(addr.substr(colon_pos + 1));
    }
}
#endif

void ReducerTracker::job_file_add(int job_id, string file_path)
{
    // check number of files received for this job
    set<string>& files_set = job_files_map[job_id];
    int num_files = files_set.size();
    assert(num_files < num_alive_mappers);
    files_set.insert(file_path);
}

int ReducerTracker::num_job_files(int job_id)
{
    // check number of files received for this job
    set<string>& files_set = job_files_map[job_id];
    int num_files = files_set.size();
    assert(num_files <= num_alive_mappers);
    return num_files;
}


set<string>& ReducerTracker::job_files_get(int job_id)
{
    return r_tracker.job_files_map[job_id];
}


void count_all_words(int job_id)
{
    map<string, int> word_count_map;
    set<string>&     files_set = r_tracker.job_files_get(job_id);
    string           word;
    for(auto itr = files_set.begin(); itr != files_set.end(); ++itr)
    {
        ifstream in(*itr);
        if(!in)
        {
            stringstream ss;
            ss << "Error: (" << __func__ << ") (" << __LINE__ << "): " << strerror(errno);
            r_tracker.log_print(ss.str());
            return;
        }
        while(in >> word)
        {
            ++word_count_map[word];
        }
    }
    string temp_file = "temp_" + to_string(job_id) + ".txt";
    ofstream out(temp_file, ios_base::app);
    if(!out)
    {
        stringstream ss;
        ss << "Error: (" << __func__ << ") (" << __LINE__ << "): " << strerror(errno);
        r_tracker.log_print(ss.str());
        return;
    }
    for(auto itr = word_count_map.begin(); itr != word_count_map.end(); ++itr)
    {
        out << itr->first << " " << itr->second << "\n";
    }
}


void mapper_request_handle(int sock, string req_str)
{
    
    stringstream ss;
    ss << "Handling request: " << req_str << endl;
    r_tracker.log_print(ss.str());

    // currently only taking job_id$file_path in the request buffer
    int dollar_pos = req_str.find('$');
    string cmd = req_str.substr(0, dollar_pos);
    req_str = req_str.erase(0, dollar_pos+1);
    int job_id = stoi(cmd);

    r_tracker.job_file_add(job_id, req_str);

    if(r_tracker.num_job_files(job_id) == num_alive_mappers)
    {
        thread counting_thread(count_all_words, job_id);
        counting_thread.detach();
    }

#if 0
    switch(req)
    {
        case GET_CHUNK_IDS:
        {
            string double_sha1_str = req_str;
            auto itr = seeding_file_chunks.find(double_sha1_str);
            set<int>& id_set = itr->second;
            auto set_itr = id_set.begin();
            string ids_str;
            ids_str += to_string(*set_itr);
            for(++set_itr; set_itr != id_set.end(); ++set_itr)
            {
                ids_str += "$" + to_string(*set_itr);
            }
            int sz = ids_str.length();
            send(sock, &sz, sizeof(sz), 0);
            send(sock, ids_str.c_str(), ids_str.length(), 0);

            fprint_log("GET_CHUNK_IDS: Sending " + ids_str);
            break;
        }

        case GET_CHUNKS:
        {
            thread th(file_chunks_upload, sock, req_str);
            th.detach();
            break;
        }

        default:
            break;
    }
#endif
}


void run()
{
    int opt = true;
    int addrlen , new_sock , mapper_socks[MAX_MAPPERS],
        activity, valread , sd;
    int max_sd;
    struct sockaddr_in address;

    fd_set readfds;

    for (int i = 0; i < MAX_MAPPERS; i++)
    {
        mapper_socks[i] = 0;
    }

    if((r_tracker.sock = socket(AF_INET , SOCK_STREAM , 0)) == 0)
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): socket failed!!";
        r_tracker.log_print(ss.str());
        exit(EXIT_FAILURE);
    }

    // set r_tracker's socket to allow multiple connections from mappers,
    // this is just a good habit, it will work without this
    if( setsockopt(r_tracker.sock, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0 )
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): setsockopt";
        r_tracker.log_print(ss.str());
        exit(EXIT_FAILURE);
    }

    // type of socket created  
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(r_tracker.port);

    // bind the socket to r_tracker's port
    if (bind(r_tracker.sock, (struct sockaddr *)&address, sizeof(address))<0)   
    {   
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): bind failed";
        r_tracker.log_print(ss.str());
        exit(EXIT_FAILURE);   
    }   
    stringstream ss;
    ss << "ReducerTraceker listen on " << r_tracker.ip_addr << ":" << r_tracker.port;
    r_tracker.log_print(ss.str());
         
    // try to specify maximum of pending connections for r_tracker's socket
    if (listen(r_tracker.sock, MAX_MAPPERS) < 0)   
    {
        stringstream ss;
        ss << __func__ << " (" << __LINE__ << "): listen failed";
        r_tracker.log_print(ss.str());
        exit(EXIT_FAILURE);   
    }   
         
    // accept the incoming connection  
    addrlen = sizeof(address);

    while(true)
    {   
        // clear the socket set
        FD_ZERO(&readfds);   

        // add r_tracker's socket to the read fd set
        FD_SET(r_tracker.sock, &readfds);
        max_sd = r_tracker.sock;

        // add mapper sockets to set
        for (int i = 0 ; i < MAX_MAPPERS ; i++)
        {
            // socket descriptor
            sd = mapper_socks[i];

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
                ss << __func__ << " (" << __LINE__ << "): select error";
                r_tracker.log_print(ss.str());
            }
            continue;
        }

        // If something happened on the master socket ,  
        // then its an incoming connection  
        if (FD_ISSET(r_tracker.sock, &readfds))   
        {
            new_sock = accept(r_tracker.sock, (struct sockaddr *) &address, (socklen_t*) &addrlen);
            if(FAILURE == new_sock)
            {
                stringstream ss;
                ss << __func__ << " (" << __LINE__ << "): accept() failed!!";
                r_tracker.log_print(ss.str());
                exit(EXIT_FAILURE);
            }

            // add new socket to array of sockets
            for (int i = 0; i < MAX_MAPPERS; i++)
            {
                // if position is empty
                if(mapper_socks[i] == 0)
                {
                    mapper_socks[i] = new_sock;
                    if(num_alive_mappers < MAX_MAPPERS)
                        ++num_alive_mappers;

                    stringstream ss;
                    ss << "New mapper with socket id " << new_sock << " connected\n";
                    r_tracker.log_print(ss.str());
                    break;
                }
            }
        }
        else
        {
            // else its some IO operation on some other socket
            for (int i = 0; i < MAX_MAPPERS; i++)
            {
                sd = mapper_socks[i];

                if (FD_ISSET( sd , &readfds))
                {
                    int read_size;
                    valread = read(sd, &read_size, sizeof(read_size));
                    if(FAILURE == valread)
                    {
                        close(sd);
                        mapper_socks[i] = 0;

                        stringstream ss;
                        ss << __func__ << " (" << __LINE__ << "): read() failed!!";
                        r_tracker.log_print(ss.str());
                    }
                    else if(0 == valread)
                    {
                        // Somebody disconnected, get his details and print
                        getpeername(sd , (struct sockaddr*)&address, (socklen_t*)&addrlen);
                        stringstream ss;
                        ss << "Mapper disconnected!! <ip_addr>:<port> -> " 
                           << inet_ntoa(address.sin_addr) << ":" << ntohs(address.sin_port);
                        r_tracker.log_print(ss.str());

                        close(sd);
                        mapper_socks[i] = 0;
                        --num_alive_mappers;
                    }
                    else
                    {
                        char buffer[read_size + 1] = {'\0'};
                        data_read(sd, buffer, read_size);

                        stringstream ss;
                        ss << "Request read: " << buffer;
                        r_tracker.log_print(ss.str());

                        getpeername(sd , (struct sockaddr*)&address, (socklen_t*)&addrlen);

                        mapper_request_handle(sd, buffer);
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
        status_print(FAILURE, "Reducer Launch command : \"/reducer <REDUCER_IP>:<PORT> <tracker_log_file> <worker_log_file>\"");
        cout << endl;
        return 0;
    }

    //addr[REDUCER] = argv[1];
    r_tracker.log_path = abs_path_get(argv[2]);

    ip_and_port_split(argv[1], r_tracker.ip_addr, r_tracker.port);

    run();

    return 0;
}
