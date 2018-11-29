#ifndef DUMMYMASTER_H
#define DUMMYMASTER_H

#include<string>
#include<vector>


class DummyMaster
{
    private:

    // std::string * ip_address;
    // int * port_number;
    //int * sock;
    int sock_id;

    public:
    //DummyMaster();
    void connect_as_mapper(std::string ip_address, int port_number);
    void connect_as_reducer(std::string ip_address, int port_number);
    std::string get_request();
    void confirm();
    void job_completed(std::string job_id, std::vector<std::string> reducer_files);
    //int get_socket();

};

#endif