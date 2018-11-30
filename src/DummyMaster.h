#ifndef DUMMYMASTER_H
#define DUMMYMASTER_H

#include<string>
#include<vector>


class DummyMaster
{
    private:

    int sock_id;

    public:
    void connect_as_mapper(std::string ip_address, int port_number);
    void connect_as_reducer(std::string ip_address, int port_number);
    std::string get_request();
    void confirm();
    void job_completed(std::string job_id, std::vector<std::string> reducer_files);
    void job_completed_reducer(std::string job_id, int category, std::string output_file);

};

#endif