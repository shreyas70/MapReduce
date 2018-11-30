#ifndef DREDUCERCLIENT_H
#define DREDUCERCLIENT_H

#include<string>

class DummyReducerClient
{
    private:
    std::string reducer_ip;
    int reducer_port;
    int reducer_socket;
    int heart_beat_socket;
    int initiate_heart_beats();
    
    public:
    // DummyReducerClient(std::string reducer_ip, int reducer_port);
    // ~DummyReducerClient();
    void connect_to_reducer(std::string reducer_ip, int reducer_port);

    std::string receive_heart_beat();
    void reply_to_heart_beat();
    int get_heart_beat_socket();
    void start_word_count_job(std::string file_path);
    void start_inverted_index_job(std::string file_path);
};

#endif