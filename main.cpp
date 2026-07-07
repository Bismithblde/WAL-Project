#include <iostream>
#include <unordered_map>
#include <functional>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <WinSock2.h>
#include <thread>
#include <mutex>
using namespace std;

unordered_map<string, string> memTable;
unordered_map<string, function<void(istringstream&, SOCKET)>> command_map;
std::mutex db_mutex;

// Test-NetConnection 127.0.0.1 -Port 6379
void send_response(SOCKET client_socket, string message) {
    send(client_socket, message.c_str(), static_cast<int>(message.length()), 0);
}

void handle_get(istringstream &ISS, SOCKET client_socket) {
    lock_guard<std::mutex> lock(db_mutex);
    string key;
    ISS >> key; 
    
    if (memTable.find(key) != memTable.end()) {
        send_response(client_socket, memTable[key] + "\n"); 
    } else {
        send_response(client_socket, "(nil)\n");
    }
}

void handle_set(istringstream &ISS, SOCKET client_socket) {
    lock_guard<std::mutex> lock(db_mutex);
    string key, value;
    ISS >> key >> value;
    if (key.empty()) {
        send_response(client_socket, "ERR: Key Empty!\n");
        return;
    }
    if (value.empty()) {
        send_response(client_socket, "ERR: Value Empty!\n");
        return;
    }
    memTable[key] = value;
    ofstream File("wal.txt", ios::app);
    File << "SET " + key + " " + value << "\n";
    File.close();
    
    send_response(client_socket, "OK\n");
}

void handle_compact(istringstream &ISS, SOCKET client_socket) {
    lock_guard<std::mutex> lock(db_mutex);
    ofstream TempFile("wal.tmp");
    if (!TempFile.is_open()) {
        send_response(client_socket, "ERR: Error Opening Temp File\n");
        return;
    }

    for (auto const& [key, value] : memTable) {
        TempFile << "SET " + key + " " + value + "\n";
    }

    TempFile.close();

    try {
        filesystem::rename("wal.tmp", "wal.txt");
        send_response(client_socket, "OK\n");
    } catch (const filesystem::filesystem_error& e){
        send_response(client_socket, "ERR: Atomic swap failed\n");
    }
}

void handle_delete(istringstream &ISS, SOCKET client_socket) {
    {
        lock_guard<std::mutex> lock(db_mutex);
        string key;
        ISS >> key;
        memTable.erase(key);
    }
    handle_compact(ISS, client_socket);
}

void initWal() {
    string line;
    ifstream File("wal.txt");

    command_map["GET"] = handle_get;
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

void workerFunction(SOCKET client_socket) {
    string accumulator = "";
    while (true) {
        char buffer[512];
        memset(buffer, 0, sizeof(buffer));
        int bytes_received = recv(client_socket, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received <= 0) { 
                cout << "Client disconnected." << endl;
                closesocket(client_socket);
                break;
        }
        accumulator.append(buffer, bytes_received);

        size_t newline_pos;
        while ((newline_pos = accumulator.find("\n")) != string::npos) {
            string currentLine = accumulator.substr(0, newline_pos);
            accumulator.erase(0, newline_pos+1);
            if (!currentLine.empty() && currentLine.back() == '\r') {
                currentLine.pop_back();
            }
            if (!currentLine.empty()) {
                istringstream ISS(currentLine);
                string command;
                ISS >> command;

                if (command == "EXIT") {
                    send_response(client_socket, "Bye!\n");
                    closesocket(client_socket);
                    return;
                }

                if (command_map.find(command) != command_map.end()) {
                    command_map[command](ISS, client_socket);
                }
                else {
                    send_response(client_socket, "ERR: Invalid Command Entered: " + command + "\n");
                }
            }
        }
    }
}

int main() {
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

    cout << "Server boot complete. Awaiting client lines...\n";

    while (true) {
        sockaddr_in clientaddr;
        int clientaddr_size = sizeof(clientaddr);
        SOCKET client_socket = accept(server_socket, (sockaddr*)&clientaddr, &clientaddr_size);
        if (client_socket == INVALID_SOCKET) {
            cout << "ACCEPT FAILED: " << WSAGetLastError() << "\n";
            continue;
        }
        cout << "Connection Successful \n";
        thread wt(workerFunction, client_socket);
        wt.detach();
    }

    closesocket(server_socket);
    WSACleanup();
    return 0;
}