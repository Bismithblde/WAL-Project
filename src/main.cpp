#include <iostream>
#include <unordered_map>
#include <functional>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <WinSock2.h>
#include <thread>
#include <mutex>
#include <vector>
#include <algorithm>
#include <memory>
#include <atomic>
#include <chrono>
#include <shared_mutex>
#include <ThreadSafeQueue.h>
using namespace std;

enum class ClientState {
    COMMAND_MODE,
    SUBSCRIBE_MODE
};

struct ClientContext {
    SOCKET socket;
    ClientState state = ClientState::COMMAND_MODE;
    string accumulator;
    bool queued = false;
    bool connected = true;
    mutex mtx;
};

unordered_map<string, string> memTable;
unordered_map<string, function<void(istringstream&, shared_ptr<ClientContext>)>> command_map;
unordered_map<string, vector<SOCKET>> channel_map;
std::shared_mutex db_mutex;
std::mutex pubsub_mutex;
vector<shared_ptr<ClientContext>> clients;
std::mutex clients_mutex;
ThreadSafeQueue workers_queue;
atomic<int> active_worker_count(0);

void setSocketNonBlocking(SOCKET socket);
void disconnect_client(shared_ptr<ClientContext> client_context);

// Test-NetConnection 127.0.0.1 -Port 6379
void send_response(SOCKET client_socket, string message) {
    send(client_socket, message.c_str(), static_cast<int>(message.length()), 0);
}

void handle_get(istringstream &ISS, shared_ptr<ClientContext> client_context) {
    if (client_context->state == ClientState::SUBSCRIBE_MODE) {
        send_response(client_context->socket, "Failed Command: In Subcribe Mode;\nSend \'EXITSUB\' to exit.");
        return;
    }
    SOCKET client_socket = client_context-> socket;
    shared_lock<std::shared_mutex> lock(db_mutex);
    string key;
    ISS >> key; 
    
    if (memTable.find(key) != memTable.end()) {
        send_response(client_socket, memTable[key] + "\n"); 
    } else {
        send_response(client_socket, "(nil)\n");
    }
}

void handle_set(istringstream &ISS, shared_ptr<ClientContext> client_context) {
    if (client_context->state == ClientState::SUBSCRIBE_MODE) {
        send_response(client_context->socket, "Failed Command: In Subcribe Mode;\nSend \'EXITSUB\' to exit.");
        return;
    }
    SOCKET client_socket = client_context-> socket;
    unique_lock<std::shared_mutex> lock(db_mutex);
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

void handle_compact(istringstream &ISS, shared_ptr<ClientContext> client_context) {
    if (client_context->state == ClientState::SUBSCRIBE_MODE) {
        send_response(client_context->socket, "Failed Command: In Subcribe Mode;\nSend \'EXITSUB\' to exit.");
        return;
    }
    SOCKET client_socket = client_context-> socket;
    unique_lock<std::shared_mutex> lock(db_mutex);
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

void handle_delete(istringstream &ISS, shared_ptr<ClientContext> client_context) {
    if (client_context->state == ClientState::SUBSCRIBE_MODE) {
        send_response(client_context->socket, "Failed Command: In Subcribe Mode;\nSend \'EXITSUB\' to exit.");
        return;
    }
    SOCKET client_socket = client_context -> socket;
    {
        unique_lock<std::shared_mutex> lock(db_mutex);
        string key;
        ISS >> key;
        memTable.erase(key);
    }
    handle_compact(ISS, client_context);
}

void handle_subscribe(istringstream &ISS, shared_ptr<ClientContext> client_context) {
    SOCKET client_socket = client_context->socket;
    string channel;
    ISS >> channel;
    if (channel.empty()) {
        return;
    }
    {
        lock_guard<std::mutex> lock(pubsub_mutex);
        channel_map[channel].emplace_back(client_socket);
        client_context->state = ClientState::SUBSCRIBE_MODE;
    }
    

    string reply = "*3\r\n$9\r\nsubscribe\r\n$" + to_string(channel.length()) 
                   + "\r\n" + channel + "\r\n:1\r\n";
    send_response(client_context->socket, reply);
}
void handle_publish(istringstream &ISS, shared_ptr<ClientContext> client_context) {
    string channel, message;
    ISS >> channel;
    getline(ISS, message);
    if (!message.empty() && message[0] == ' ') {
        message.erase(0, 1);
    }

    if (channel.empty() || message.empty()) {
        send_response(client_context->socket, "-ERR Channel or Message Empty\r\n");
        return;
    }

    vector<SOCKET> targets;
    {
        lock_guard<::mutex> lock(pubsub_mutex);
        if (channel_map.find(channel) != channel_map.end()) {
            targets = channel_map[channel];
        }
    }

    string resp_msg = "*3\r\n$7\r\nmessage\r\n$" + to_string(channel.length()) 
                      + "\r\n" + channel + "\r\n$" + to_string(message.length()) 
                      + "\r\n" + message + "\r\n";
    
    int active_rec = 0;
    for (SOCKET sub_socket : targets) {
        int res = send(sub_socket, resp_msg.c_str(), static_cast<int>(resp_msg.length()), 0);
        if (res != SOCKET_ERROR) {
            active_rec++;
        }
    }

    string pub_reply = ":" + to_string(active_rec) + "\r\n";
    send_response(client_context->socket, pub_reply);

}

void initWal() {
    string line;
    ifstream File("wal.txt");

    command_map["GET"] = handle_get;
    command_map["SET"] = handle_set;
    command_map["COMPACT"] = handle_compact;
    command_map["DELETE"] = handle_delete;
    command_map["SUBSCRIBE"] = handle_subscribe;
    command_map["PUBLISH"] = handle_publish;

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

void disconnect_client(shared_ptr<ClientContext> client_context) {
    SOCKET client_socket = client_context->socket;

    {
        lock_guard<mutex> lock(client_context->mtx);
        if (!client_context->connected) {
            return;
        }
        client_context->connected = false;
        client_context->queued = false;
    }

    {
        lock_guard<mutex> lock(pubsub_mutex);
        for (auto& [channel, subscribers] : channel_map) {
            subscribers.erase(
                remove(subscribers.begin(), subscribers.end(), client_socket),
                subscribers.end()
            );
        }
    }

    {
        lock_guard<mutex> lock(clients_mutex);
        clients.erase(
            remove(clients.begin(), clients.end(), client_context),
            clients.end()
        );
    }

    closesocket(client_socket);
}

void workerFunction(shared_ptr<ClientContext> client_context) {
    SOCKET client_socket = client_context->socket;
    auto finish_client_event = [&]() {
        lock_guard<mutex> lock(client_context->mtx);
        if (client_context->connected) {
            client_context->queued = false;
        }
    };

    char buffer[512];
    int bytes_received = recv(client_socket, buffer, sizeof(buffer), 0);

    if (bytes_received == 0) {
        disconnect_client(client_context);
        return;
    }

    if (bytes_received == SOCKET_ERROR) {
        int error = WSAGetLastError();
        if (error != WSAEWOULDBLOCK && error != WSAEINTR) {
            cerr << "Socket receive failed: " << error << '\n';
            disconnect_client(client_context);
        } else {
            finish_client_event();
        }
        return;
    }

    client_context->accumulator.append(buffer, bytes_received);

    size_t newline_pos;
    while ((newline_pos = client_context->accumulator.find("\n")) != string::npos) {
        string currentLine = client_context->accumulator.substr(0, newline_pos);
        client_context->accumulator.erase(0, newline_pos + 1);
            if (!currentLine.empty() && currentLine.back() == '\r') {
                currentLine.pop_back();
            }
            if (!currentLine.empty()) {
                istringstream ISS(currentLine);
                string command;
                ISS >> command;

                if (command == "EXIT") {
                    send_response(client_socket, "Bye!\n");
                    disconnect_client(client_context);
                    return;
                }

                if (client_context->state == ClientState::SUBSCRIBE_MODE) {
                    if (command == "SUBSCRIBE") {
                        handle_subscribe(ISS, client_context);
                    } else if (command == "EXITSUB") {
                        // implement later
                    } else {
                        send_response(client_socket, "Failed Command: In Subscribe Mode;\nSend 'EXITSUB' to exit.\n");
                    }
                } 
                else {
                    if (command_map.find(command) != command_map.end()) {
                        command_map[command](ISS, client_context);
                    } else {
                        send_response(client_socket, "ERR: Invalid Command Entered: " + command + "\n");
                    }
                }
            }
    }

    finish_client_event();
}
void workerLoop() {
    while (true) {
        // dequeue handles empty, waits until queue has something
        auto client = workers_queue.dequeue();
        active_worker_count++;
        workerFunction(client);
        active_worker_count--;
    }
}
void pollLoop() {
    while (true) {
        vector<shared_ptr<ClientContext>> client_snapshot;
        {
            lock_guard<mutex> lock(clients_mutex);
            client_snapshot = clients;
        }

        vector<WSAPOLLFD> poll_fds;
        vector<shared_ptr<ClientContext>> poll_clients;
        for (const auto& client : client_snapshot) {
            lock_guard<mutex> lock(client->mtx);
            if (!client->connected || client->queued) {
                continue;
            }

            WSAPOLLFD poll_fd{};
            poll_fd.fd = client->socket;
            poll_fd.events = POLLRDNORM;
            poll_fds.push_back(poll_fd);
            poll_clients.push_back(client);
        }

        if (poll_fds.empty()) {
            this_thread::sleep_for(chrono::milliseconds(1));
            continue;
        }

        int poll_result = WSAPoll(
            poll_fds.data(), static_cast<ULONG>(poll_fds.size()), 1000
        );
        if (poll_result == SOCKET_ERROR) {
            int error = WSAGetLastError();
            if (error != WSAEINTR) {
                cerr << "Socket poll failed: " << error << '\n';
            }
            continue;
        }

        for (size_t i = 0; i < poll_fds.size(); i++) {
            const auto& client = poll_clients[i];

            if (poll_fds[i].revents & (POLLERR | POLLHUP | POLLNVAL)) {
                disconnect_client(client);
                continue;
            }

            if (poll_fds[i].revents & POLLRDNORM) {
                bool should_enqueue = false;
                {
                    lock_guard<mutex> lock(client->mtx);
                    if (client->connected && !client->queued) {
                        client->queued = true;
                        should_enqueue = true;
                    }
                }

                if (should_enqueue) {
                    workers_queue.enqueue(client);
                }
            }
        }
    }
}

int acceptFunction(SOCKET server_socket) {
    while (true) {
        sockaddr_in clientaddr;
        int clientaddr_size = sizeof(clientaddr);
        SOCKET client_socket = accept(server_socket, (sockaddr*)&clientaddr, &clientaddr_size);
        if (client_socket == INVALID_SOCKET) {
            cout << "ACCEPT FAILED: " << WSAGetLastError() << "\n";
            continue;
        }

        setSocketNonBlocking(client_socket);
        auto client = make_shared<ClientContext>();
        client->socket = client_socket;

        lock_guard<mutex> lock(clients_mutex);
        clients.push_back(client);
    }
}

void setSocketNonBlocking(SOCKET socket) {
    u_long mode = 1;
    ioctlsocket(socket, FIONBIO, &mode);
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

    vector<thread> thread_pool;

    for (int i = 0; i < thread::hardware_concurrency(); i++) {
        thread_pool.emplace_back(workerLoop);
    }

    thread pt(pollLoop);
    thread at(acceptFunction, server_socket);
    at.join();
    pt.join();
    

    closesocket(server_socket);
    WSACleanup();


    return 0;
}
