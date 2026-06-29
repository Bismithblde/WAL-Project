#include <iostream>
#include <unordered_map>
#include <functional>
#include <fstream>
#include <sstream>
using namespace std;

unordered_map<string, string> memTable;
unordered_map<string, function<void()>> command_map;

void handle_get() {
    string key;
    cin >> key; 
    
    if (memTable.find(key) != memTable.end()) {
        cout << memTable[key] << "\n";
    } else {
        cout << "(nil)\n";
    }
}

void handle_exit() {
    exit(0); 
}

void handle_set() {
    string key, value;
    cin >> key >> value;
    memTable[key] = value;
    ofstream File("wal.txt", ios::app);
    File << "SET " + key + " " + value << "\n";
    File.close();
}


void initWal() {
    string line;
    ifstream File("wal.txt");

    command_map["GET"] = handle_get;
    command_map["EXIT"] = handle_exit;
    command_map["SET"] = handle_set;


    while (getline(File, line)) {
        // get first word
        string command = line.substr(0, line.find(" "));
        if (command == "SET") {
            string keyValue = line.substr(line.find(" ")+1);
            istringstream iss(keyValue);
            string key, value;
            iss >> key >> value;

            memTable[key] = value;
        }
    }
}
int main() {

    string input = "";
    bool inLoop = true;
    
    initWal();
    
    while (true) {
        cin >> input;
        if (command_map.find(input) != command_map.end()) {
            command_map[input]();
            
        }
        else {
            cout << "Invalid Input \n";
        }
    }
    return 0;
}