#include<iostream>
#include<string>
#include<string.h>
#include<unistd.h>
#include "Mapper.h"

using namespace std;

int main()
{
    Mapper m = Mapper("127.0.0.1", 7000);
    while(true)
    {
        cout<<"\nHeart beat received : "<<m.receive_heart_beat()<<endl;
        m.reply_to_heart_beat();
    }
    return 0;
}