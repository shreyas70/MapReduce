#ifndef DREDUCERSERVER_H
#define DREDUCERSERVER_H

#include<string>

class DummyReducerServer
{
    private:
    std::string ip_address;
    int port_number;
    void give_heart_beats(int sock_desc);
    void process_reduce_request(int client_socket);

    public:
    DummyReducerServer(std::string ip_address, int port_number);
    void start_server();
};

#endif