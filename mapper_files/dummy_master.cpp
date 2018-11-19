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
    m.initiate_word_count_request("job1", "file1.txt", 54, 43);
    return 0;
}