#include<iostream>
#include<unistd.h>
#include<fcntl.h>
#include<vector>
#include<string>
#include<algorithm>
#include<stdlib.h>

using namespace std;

int main()
{
    cout<<endl;
    vector<string> words;
    int fd = open("small_file.txt",(O_WRONLY | O_CREAT | O_TRUNC),(S_IRUSR | S_IWUSR));
    for(int i=0; i<1000000; i++)
    {
        int word_length = (rand() % 10) + 1;
        string alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        string word = "";
        for(int j=0; j<word_length; j++)
        {
            int index = rand() % 62;
            word += alphabet[index];
        }
        words.push_back(word);
        //cout<<word<<" ";
    }
    //sort(words.begin(), words.end());
    for(int i=0; i<words.size(); i++)
    {
        string new_word = words[i];
        if(i!=(words.size()-1))
        {
            new_word = new_word+" ";
        }
        //cout<<words[i]<<" ";
        write(fd, new_word.c_str(), new_word.length());
    }
    close(fd);
    cout<<endl;
}