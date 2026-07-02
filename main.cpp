#include <iostream>
#include <unordered_map>
#include <functional>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <WinSock2.h>
using namespace std;

unordered_map<string, string> memTable;
unordered_map<string, function<void(istringstream&)>> command_map;

// Test-NetConnection 127.0.0.1 -Port 6379
void handle_get(istringstream &ISS) {
    string key;
    ISS >> key; 
    
    if (memTable.find(key) != memTable.end()) {
        cout << memTable[key] << "\n";
    } else {
        cout << "(nil)\n";
    }
}

void handle_exit(istringstream &ISS) {
    exit(0); 
}

void handle_set(istringstream &ISS) {
    string key, value;
    ISS >> key >> value;
    memTable[key] = value;
    ofstream File("wal.txt", ios::app);
    File << "SET " + key + " " + value << "\n";
    File.close();
}

void handle_compact(istringstream &ISS) {
    ofstream TempFile("wal.tmp");
    if (!TempFile.is_open()) {
        cout << "Error Opening Temp File";
        return;
    }

    for (auto const& [key, value] : memTable) {
        TempFile << "SET " + key + " " + value + "\n";
    }

    TempFile.close();

    try {
        filesystem::rename("wal.tmp", "wal.txt");
    } catch (const filesystem::filesystem_error& e){
        cout << "ERROR: Atomic swap failed: " << e.what() << "\n";
    }
}

void handle_delete(istringstream &ISS) {
    string key;
    ISS >> key;
    memTable.erase(key);
    handle_compact(ISS);
}

void initWal() {


    string line;
    ifstream File("wal.txt");

    command_map["GET"] = handle_get;
    command_map["EXIT"] = handle_exit;
    command_map["SET"] = handle_set;
    command_map["COMPACT"] = handle_compact;
    command_map["DELETE"] = handle_delete;

    while (getline(File, line)) {
        if (line.empty()) {
            continue;
        }
        istringstream iss(line);

        string command;
        iss >> command;

        if (command == "SET") {
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

    // init socket:
    WSADATA wsaData = {0};
    int iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (iResult != 0) {
        cout << "Socket Startup Failed.\n";
        return 1;
    }
    SOCKET server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket == INVALID_SOCKET) {
        cout << "Socket creation failed.\n";
        WSACleanup();
        return 1;
    }
    sockaddr_in serveraddr;
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_port = htons(6379);
    serveraddr.sin_addr.s_addr = INADDR_ANY;

    if (::bind(server_socket, (struct sockaddr*)&serveraddr, sizeof(serveraddr)) == SOCKET_ERROR) {
        cout << "BIND FAILED: " << WSAGetLastError() << "\n";
        closesocket(server_socket);
        WSACleanup();
        return 1;
    }

    if (listen(server_socket, SOMAXCONN) == SOCKET_ERROR) {
        cout << "FAILED TO LISTEN SOCKET: " << WSAGetLastError() << "\n";
        WSACleanup();
        return 1;
    }

    sockaddr_in clientaddr;
    int clientaddr_size = sizeof(clientaddr);
    SOCKET client_socket = accept(server_socket, (sockaddr*)&clientaddr, &clientaddr_size);

    if (client_socket == INVALID_SOCKET) {
        cout << "ACCEPT FAILED: " << WSAGetLastError() << "\n";

        closesocket(server_socket);
        WSACleanup();
        return 1;
    }

    cout << "Connection Successful \n";
    string accumulator = "";
    while (true) {
        char buffer[512];
        memset(buffer, 0, sizeof(buffer));
        int bytes_received = recv(client_socket,  buffer, sizeof(buffer) - 1, 0);
        if (bytes_received <= 0) { 
                cout << "No bytes received!" << endl;
                break;
        }
        accumulator.append(buffer, bytes_received);
        if (accumulator.find("\n") != -1) {
            istringstream ISS(accumulator);
            string command;
            ISS >> command;

            if (command_map.find(command) != command_map.end()) {
                command_map[command](ISS);
            }
            else {
                cout << "Invalid Command Entered.\n";
            }
            accumulator = "";
        }
    }

    closesocket(server_socket);
    WSACleanup();
    return 0;
}