#include<iostream>
#include<string>
#include<string.h>
#include<unistd.h>
#include "Mapper.h"

using namespace std;

int main()
{
    Mapper m = Mapper("127.0.0.1", 7000);
    if(fork()==0)
    {
        while(true)
        {
            cout<<"\nHeart beat received : "<<m.receive_heart_beat()<<endl;
            m.reply_to_heart_beat();
        }
        exit(0);
    }
    vector<string> file_names;
    file_names.push_back("i_file1.txt");
    file_names.push_back("i_file2.txt");
    file_names.push_back("i_file3.txt");
    vector<off_t> offsets;
    offsets.push_back(45);
    offsets.push_back(32);
    offsets.push_back(98);
    vector<size_t> piece_sizes;
    piece_sizes.push_back(654);
    piece_sizes.push_back(65);
    piece_sizes.push_back(100);
    m.initiate_inverted_index_request("job2", file_names, offsets, piece_sizes);
    //m.initiate_word_count_request("job1", "file1.txt", 54, 43);
    return 0;
}