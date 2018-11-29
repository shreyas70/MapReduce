#include <iostream>
#include <string>
#include "InvertedIndex.h"

using namespace std;

int main()
{
    InvertedIndexReducer irc;
    irc.init("job2$4");
    irc.reduce(1, "../output_files/job2_reducer1.txt");
    irc.reduce(1, "../output_files/job2_reducer2.txt");
    irc.reduce(1, "../output_files/job2_reducer3.txt");
    irc.reduce(1, "../output_files/job2_reducer4.txt");
    return 0;
}
