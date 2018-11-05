#include<iostream>
#include<string>
#include<string.h>
#include<unistd.h>
#include "Mapper.h"

using namespace std;

int main()
{
    Mapper m;
    MapperConnection mc = m.connect_to_mapper("127.0.0.1", 7000);
    int hb_sock = mc.heart_beat_socket;
    while(true)
    {
        char heart_beat[10];
        bzero(heart_beat, 10);
        read(hb_sock, heart_beat, 10);
        cout<<"\nHeart beat received : "<<heart_beat<<endl;
        write(hb_sock, "NULL", 4);
    }
    return 0;
}