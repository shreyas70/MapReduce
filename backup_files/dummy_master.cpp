#include<iostream>
#include<string>
#include<string.h>
#include<unistd.h>
#include "Mapper.h"

using namespace std;

int main()
{
    Mapper m;
    m.connect_to_mapper("127.0.0.1", 7000);
    if(fork()==0)
    {
        while(true)
        {
            cout<<"\nHeart beat received : "<<m.receive_heart_beat()<<endl;
            m.reply_to_heart_beat();
        }
        exit(0);
    }
    
    m.initiate_word_count_request("job1", "input_files/file1.txt", 54, 43);
    // vector<string> file_paths;
    // file_paths.push_back("input_files/i_file1.txt");
    // file_paths.push_back("input_files/i_file2.txt");
    // file_paths.push_back("input_files/i_file3.txt");
    // vector<off_t> offsets;
    // offsets.push_back(54);
    // offsets.push_back(48);
    // offsets.push_back(0);
    // vector<size_t> piece_sizes;
    // piece_sizes.push_back(334);
    // piece_sizes.push_back(100);
    // piece_sizes.push_back(300);
    // m.initiate_inverted_index_request("job2", file_paths, offsets, piece_sizes);

    return 0;
}