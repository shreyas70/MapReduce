#include<iostream>
#include<unistd.h>
#include<fcntl.h>
#include<string>
#include<queue>
#include<string.h>
using namespace std;

typedef struct
{
    string data;
    int file_id;
}HeapData;

class Compare
{
    public:
    
    bool operator()(HeapData d1, HeapData d2)
    {
        return (d1.data.compare(d2.data));
    }
};

int main(int argc, char ** argv)
{
    if(argc < 2)
    {
        cout<<"\nenter the file name\n";
    }
    string file_name = argv[1];
    FILE * file_ptr = fopen(file_name.c_str(), "r");
    int iteration = 0;
    string out_file_name = "out_file"+to_string(iteration)+".txt";
    int wd = open(out_file_name.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
    int count = 0;
    char word[12];
    bzero(word,12);
    while( fscanf(file_ptr, "%s", word) != EOF )
    {
        string word_string = word;
        word_string+=' ';
        count++;
        if(count > 100000)
        {
            count = 0;
            close(wd);
            iteration++;
            out_file_name = "out_file"+to_string(iteration)+".txt";
            wd = open(out_file_name.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
        }
        write(wd, word_string.c_str(), word_string.length());
        bzero(word, 12);
    }
    close(wd);
    fclose(file_ptr);
    FILE * out_files[iteration];
    for(int i=0; i<iteration; i++)
    {
        string out_file_name = "out_file"+to_string(i)+".txt";
        out_files[i] = fopen(out_file_name.c_str(), "r");
    }

    priority_queue<HeapData, vector<HeapData>, Compare> min_heap;

    for(int i=0; i<iteration; i++)
    {
        char buff[15];
        bzero(buff, 15);
        if( fscanf(out_files[i], "%s", buff)!=EOF )
        {
            string curr_string = buff;
            HeapData d;
            d.data = curr_string;
            d.file_id = i;
            min_heap.push(d);
        }
    }

    string output_file_name = "word_file_sorted.txt";
    int sorted_wd = open(output_file_name.c_str(),(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
    bool start = true;
    while(!min_heap.empty())
    {
        HeapData d = min_heap.top();
        min_heap.pop();
        write(sorted_wd, d.data.c_str(), d.data.length());
        write(sorted_wd, " ", 1);
        int curr_id = d.file_id;
        char buff[15];
        bzero(buff, 15);
        if(fscanf(out_files[curr_id], "%s", buff)!=EOF)
        {
            string curr_string = buff;
            HeapData d;
            d.data = curr_string;
            d.file_id = curr_id;
            min_heap.push(d);
        }
    }
    close(sorted_wd);
    for(int i=0; i<iteration; i++)
    {
        fclose(out_files[i]);
    }


    return 0;
}