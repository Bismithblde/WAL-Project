#include <WinSock2.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <deque>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <io.h>

#include <ThreadSafeQueue.h>
#include <ankerl/unordered_dense.h>

using namespace std;

constexpr size_t MAX_INBOUND_BYTES = 1024 * 1024;
constexpr size_t MAX_OUTBOUND_BYTES = 1024 * 1024;
constexpr size_t MAX_BATCH_BYTES = 64 * 1024;
constexpr size_t MAX_BATCH_COMMANDS = 64;
constexpr size_t SHARD_COUNT = 64;

enum class ClientState {
    COMMAND_MODE,
    SUBSCRIBE_MODE
};

enum class ProtocolMode {
    UNDECIDED,
    LINE,
    RESP2
};

struct ClientContext {
    SOCKET socket = INVALID_SOCKET;
    ClientState state = ClientState::COMMAND_MODE;
    ProtocolMode protocol = ProtocolMode::UNDECIDED;
    string input_buffer;
    string output_buffer;
    size_t output_offset = 0;
    struct ResponseSlot {
        string response;
        bool ready = false;
        bool close_after_flush = false;
    };
    deque<shared_ptr<ResponseSlot>> response_slots;
    bool read_queued = false;
    bool worker_active = false;
    bool close_when_worker_finishes = false;
    bool write_pending = false;
    bool closing_after_flush = false;
    bool connected = true;
    mutex mtx;
};

struct ParsedCommand {
    vector<string_view> parts;
    size_t consumed = 0;
};

enum class ParseResult {
    INCOMPLETE,
    COMPLETE,
    INVALID
};

struct Shard {
    ankerl::unordered_dense::map<string, string> table;
    shared_mutex mutex;
};

array<Shard, SHARD_COUNT> shards;
unordered_map<string, vector<weak_ptr<ClientContext>>> channel_map;
mutex pubsub_mutex;
vector<shared_ptr<ClientContext>> clients;
mutex clients_mutex;
ThreadSafeQueue workers_queue;
atomic<int> active_worker_count(0);
SOCKET poll_wakeup_read = INVALID_SOCKET;
SOCKET poll_wakeup_write = INVALID_SOCKET;
shared_mutex wal_file_mutex;

void complete_response(const shared_ptr<ClientContext>& client_context,
                       const shared_ptr<ClientContext::ResponseSlot>& slot,
                       string response,
                       bool close_after_flush = false);

enum class WalOperation { SET, ERASE };
struct WalRequest {
    WalOperation operation;
    string key;
    string value;
    shared_ptr<ClientContext> client;
    shared_ptr<ClientContext::ResponseSlot> slot;
    bool resp = false;
};

class WalCoordinator {
public:
    void start() { writer_ = thread(&WalCoordinator::run, this); }
    bool submit(WalRequest request) {
        lock_guard<mutex> lock(mutex_);
        if (queue_.size() >= 65536) return false;
        queue_.push_back(move(request));
        cv_.notify_one();
        return true;
    }
    void stop() {
        { lock_guard<mutex> lock(mutex_); stopping_ = true; cv_.notify_all(); }
        if (writer_.joinable()) writer_.join();
    }
private:
    void run() {
        while (true) {
            vector<WalRequest> batch;
            { unique_lock<mutex> lock(mutex_); cv_.wait_for(lock, chrono::milliseconds(10), [&] { return stopping_ || !queue_.empty(); });
              if (stopping_ && queue_.empty()) break;
              batch.assign(make_move_iterator(queue_.begin()), make_move_iterator(queue_.end())); queue_.clear(); }
            bool durable = false;
            {
                shared_lock<shared_mutex> file_lock(wal_file_mutex);
                FILE* file = fopen("wal.txt", "ab");
                durable = file != nullptr;
                for (const auto& request : batch) {
                    string record = request.operation == WalOperation::SET ? "SET " + request.key + " " + request.value + "\n" : "DELETE " + request.key + "\n";
                    durable = durable && fwrite(record.data(), 1, record.size(), file) == record.size();
                }
                durable = durable && fflush(file) == 0 && _commit(_fileno(file)) == 0;
                if (file) fclose(file);
            }
            for (auto& request : batch) {
                string response = durable ? (request.resp ? "+OK\r\n" : "OK\n") : (request.resp ? "-ERR WAL flush failed\r\n" : "ERR: WAL flush failed\n");
                complete_response(request.client, request.slot, move(response));
            }
        }
    }
    mutex mutex_;
    condition_variable cv_;
    deque<WalRequest> queue_;
    bool stopping_ = false;
    thread writer_;
};
WalCoordinator wal_coordinator;

void setSocketNonBlocking(SOCKET socket);
void disconnect_client(const shared_ptr<ClientContext>& client_context);
void signal_poll_loop();

size_t shard_for(string_view key) {
    return ankerl::unordered_dense::hash<string_view>{}(key) % SHARD_COUNT;
}

bool get_value(string_view key, string& value) {
    auto& shard = shards[shard_for(key)];
    shared_lock<shared_mutex> lock(shard.mutex);
    auto entry = shard.table.find(string(key));
    if (entry == shard.table.end()) {
        return false;
    }
    value = entry->second;
    return true;
}

void put_value(string key, string value) {
    auto& shard = shards[shard_for(key)];
    unique_lock<shared_mutex> lock(shard.mutex);
    shard.table[move(key)] = move(value);
}

void erase_value(string_view key) {
    auto& shard = shards[shard_for(key)];
    unique_lock<shared_mutex> lock(shard.mutex);
    shard.table.erase(string(key));
}

string line_error(string_view message) {
    return "ERR: " + string(message) + "\n";
}

string resp_error(string_view message) {
    return "-ERR " + string(message) + "\r\n";
}

string resp_bulk(const string& value) {
    return "$" + to_string(value.size()) + "\r\n" + value + "\r\n";
}

bool uses_resp(const shared_ptr<ClientContext>& client_context) {
    return client_context->protocol == ProtocolMode::RESP2;
}

void promote_ready_responses_locked(ClientContext& client) {
    while (!client.response_slots.empty() && client.response_slots.front()->ready) {
        auto slot = move(client.response_slots.front());
        client.response_slots.pop_front();
        if (client.output_offset != 0) {
            client.output_buffer.erase(0, client.output_offset);
            client.output_offset = 0;
        }
        client.output_buffer += slot->response;
        client.write_pending = true;
        client.closing_after_flush = client.closing_after_flush || slot->close_after_flush;
    }
}

shared_ptr<ClientContext::ResponseSlot> reserve_response(const shared_ptr<ClientContext>& client_context) {
    auto slot = make_shared<ClientContext::ResponseSlot>();
    lock_guard<mutex> lock(client_context->mtx);
    if (!client_context->connected || client_context->closing_after_flush) {
        return {};
    }
    client_context->response_slots.push_back(slot);
    return slot;
}

void complete_response(const shared_ptr<ClientContext>& client_context,
                       const shared_ptr<ClientContext::ResponseSlot>& slot,
                       string response,
                       bool close_after_flush) {
    if (!slot) {
        return;
    }
    bool should_disconnect = false;
    {
        lock_guard<mutex> lock(client_context->mtx);
        if (!client_context->connected) {
            return;
        }
        slot->response = move(response);
        slot->close_after_flush = close_after_flush;
        slot->ready = true;
        size_t pending = client_context->output_buffer.size() - client_context->output_offset;
        for (const auto& queued : client_context->response_slots) {
            if (queued->ready) {
                pending += queued->response.size();
            }
        }
        if (pending > MAX_OUTBOUND_BYTES) {
            should_disconnect = true;
        } else {
            promote_ready_responses_locked(*client_context);
        }
    }
    if (should_disconnect) {
        disconnect_client(client_context);
    } else {
        signal_poll_loop();
    }
}

void queue_response(const shared_ptr<ClientContext>& client_context, string response, bool close_after_flush = false) {
    complete_response(client_context, reserve_response(client_context), move(response), close_after_flush);
}

void send_command_error(const shared_ptr<ClientContext>& client_context, string_view message) {
    queue_response(client_context, uses_resp(client_context) ? resp_error(message) : line_error(message));
}

void handle_get(const ParsedCommand& command, const shared_ptr<ClientContext>& client_context) {
    if (client_context->state == ClientState::SUBSCRIBE_MODE) {
        send_command_error(client_context, "In Subscribe Mode; Send EXITSUB to exit.");
        return;
    }

    string response;
    if (command.parts.size() < 2) {
        response = uses_resp(client_context) ? "$-1\r\n" : "(nil)\n";
    } else {
        string value;
        if (!get_value(command.parts[1], value)) {
            response = uses_resp(client_context) ? "$-1\r\n" : "(nil)\n";
        } else {
            response = uses_resp(client_context) ? resp_bulk(value) : value + "\n";
        }
    }

    queue_response(client_context, response);
}

void handle_set(const ParsedCommand& command, const shared_ptr<ClientContext>& client_context) {
    if (client_context->state == ClientState::SUBSCRIBE_MODE) {
        send_command_error(client_context, "In Subscribe Mode; Send EXITSUB to exit.");
        return;
    }
    if (command.parts.size() < 2 || command.parts[1].empty()) {
        send_command_error(client_context, "Key Empty!");
        return;
    }
    if (command.parts.size() < 3 || command.parts[2].empty()) {
        send_command_error(client_context, "Value Empty!");
        return;
    }

    string key(command.parts[1]);
    string value(command.parts[2]);
    auto slot = reserve_response(client_context);
    if (!slot || !wal_coordinator.submit({WalOperation::SET, key, value, client_context, slot, uses_resp(client_context)})) {
        complete_response(client_context, slot, uses_resp(client_context) ? "-ERR WAL queue full\r\n" : "ERR: WAL queue full\n");
        return;
    }
    put_value(move(key), move(value));
}

void handle_compact(const shared_ptr<ClientContext>& client_context) {
    if (client_context->state == ClientState::SUBSCRIBE_MODE) {
        send_command_error(client_context, "In Subscribe Mode; Send EXITSUB to exit.");
        return;
    }

    bool compacted = false;
    {
        unique_lock<shared_mutex> file_lock(wal_file_mutex);
        vector<unique_lock<shared_mutex>> locks;
        locks.reserve(SHARD_COUNT);
        for (auto& shard : shards) {
            locks.emplace_back(shard.mutex);
        }
        ofstream temp_file("wal.tmp");
        if (temp_file.is_open()) {
            for (const auto& shard : shards) {
                for (const auto& [key, value] : shard.table) {
                    temp_file << "SET " << key << " " << value << "\n";
                }
            }
            temp_file.close();

            for (int attempt = 0; attempt < 20 && !compacted; ++attempt) {
                error_code error;
                filesystem::remove("wal.txt", error);
                error.clear();
                filesystem::rename("wal.tmp", "wal.txt", error);
                compacted = !error;
                if (!compacted) this_thread::sleep_for(chrono::milliseconds(10));
            }
        }
    }

    if (compacted) {
        queue_response(client_context, uses_resp(client_context) ? "+OK\r\n" : "OK\n");
    } else {
        send_command_error(client_context, "Atomic swap failed");
    }
}

void handle_delete(const ParsedCommand& command, const shared_ptr<ClientContext>& client_context) {
    if (client_context->state == ClientState::SUBSCRIBE_MODE) {
        send_command_error(client_context, "In Subscribe Mode; Send EXITSUB to exit.");
        return;
    }
    if (command.parts.size() < 2) {
        send_command_error(client_context, "Key Empty!");
        return;
    }

    string key(command.parts[1]);
    auto slot = reserve_response(client_context);
    if (!slot || !wal_coordinator.submit({WalOperation::ERASE, key, "", client_context, slot, uses_resp(client_context)})) {
        complete_response(client_context, slot, uses_resp(client_context) ? "-ERR WAL queue full\r\n" : "ERR: WAL queue full\n");
        return;
    }
    erase_value(key);
}

void handle_subscribe(const ParsedCommand& command, const shared_ptr<ClientContext>& client_context) {
    if (command.parts.size() < 2 || command.parts[1].empty()) {
        send_command_error(client_context, "Channel Empty");
        return;
    }

    string channel(command.parts[1]);
    {
        lock_guard<mutex> lock(pubsub_mutex);
        channel_map[channel].emplace_back(client_context);
        client_context->state = ClientState::SUBSCRIBE_MODE;
    }

    string response = "*3\r\n$9\r\nsubscribe\r\n$" + to_string(channel.length())
        + "\r\n" + channel + "\r\n:1\r\n";
    queue_response(client_context, response);
}

void handle_publish(const ParsedCommand& command, const shared_ptr<ClientContext>& client_context) {
    if (command.parts.size() < 3 || command.parts[1].empty() || command.parts[2].empty()) {
        send_command_error(client_context, "Channel or Message Empty");
        return;
    }

    string channel(command.parts[1]);
    string message(command.parts[2]);
    vector<shared_ptr<ClientContext>> targets;
    {
        lock_guard<mutex> lock(pubsub_mutex);
        auto channel_entry = channel_map.find(channel);
        if (channel_entry != channel_map.end()) {
            auto& subscribers = channel_entry->second;
            subscribers.erase(remove_if(subscribers.begin(), subscribers.end(), [&](const weak_ptr<ClientContext>& subscriber) {
                auto client = subscriber.lock();
                if (!client) {
                    return true;
                }
                targets.push_back(move(client));
                return false;
            }), subscribers.end());
        }
    }

    string message_response = "*3\r\n$7\r\nmessage\r\n$" + to_string(channel.length())
        + "\r\n" + channel + "\r\n$" + to_string(message.length())
        + "\r\n" + message + "\r\n";
    for (const auto& target : targets) {
        queue_response(target, message_response);
    }

    queue_response(client_context, ":" + to_string(targets.size()) + "\r\n");
}

void dispatch_command(const ParsedCommand& command, const shared_ptr<ClientContext>& client_context) {
    if (command.parts.empty()) {
        return;
    }

    string_view name = command.parts[0];
    if (name == "EXIT") {
        queue_response(client_context, uses_resp(client_context) ? "+BYE\r\n" : "Bye!\n", true);
        return;
    }

    if (client_context->state == ClientState::SUBSCRIBE_MODE && name != "SUBSCRIBE") {
        send_command_error(client_context, "In Subscribe Mode; Send EXITSUB to exit.");
        return;
    }

    if (name == "GET") {
        handle_get(command, client_context);
    } else if (name == "SET") {
        handle_set(command, client_context);
    } else if (name == "COMPACT") {
        handle_compact(client_context);
    } else if (name == "DELETE") {
        handle_delete(command, client_context);
    } else if (name == "SUBSCRIBE") {
        handle_subscribe(command, client_context);
    } else if (name == "PUBLISH") {
        handle_publish(command, client_context);
    } else if (name == "EXITSUB") {
        client_context->state = ClientState::COMMAND_MODE;
        queue_response(client_context, uses_resp(client_context) ? "+OK\r\n" : "OK\n");
    } else {
        send_command_error(client_context, "Invalid Command Entered: " + string(name));
    }
}

bool parse_number(const string& input, size_t start, size_t& value, size_t& next) {
    size_t end = input.find("\r\n", start);
    if (end == string::npos) {
        return false;
    }
    auto result = from_chars(input.data() + start, input.data() + end, value);
    if (result.ec != errc() || result.ptr != input.data() + end) {
        value = numeric_limits<size_t>::max();
    }
    next = end + 2;
    return true;
}

ParseResult parse_resp_command(const string& input, ParsedCommand& command) {
    if (input.empty() || input.front() != '*') {
        return ParseResult::INVALID;
    }

    size_t count = 0;
    size_t position = 0;
    if (!parse_number(input, 1, count, position)) {
        return ParseResult::INCOMPLETE;
    }
    if (count == numeric_limits<size_t>::max() || count == 0 || count > MAX_BATCH_COMMANDS) {
        return ParseResult::INVALID;
    }

    command.parts.clear();
    command.parts.reserve(count);
    for (size_t index = 0; index < count; index++) {
        if (position >= input.size()) {
            return ParseResult::INCOMPLETE;
        }
        if (input[position] != '$') {
            return ParseResult::INVALID;
        }

        size_t length = 0;
        if (!parse_number(input, position + 1, length, position)) {
            return ParseResult::INCOMPLETE;
        }
        if (length == numeric_limits<size_t>::max() || length > MAX_INBOUND_BYTES) {
            return ParseResult::INVALID;
        }
        if (input.size() - position < length + 2) {
            return ParseResult::INCOMPLETE;
        }
        if (input[position + length] != '\r' || input[position + length + 1] != '\n') {
            return ParseResult::INVALID;
        }

        command.parts.emplace_back(input.data() + position, length);
        position += length + 2;
    }

    command.consumed = position;
    return ParseResult::COMPLETE;
}

ParseResult parse_line_command(const string& input, ParsedCommand& command) {
    size_t newline = input.find('\n');
    if (newline == string::npos) {
        return ParseResult::INCOMPLETE;
    }

    size_t end = newline;
    if (end != 0 && input[end - 1] == '\r') {
        end--;
    }
    command.parts.clear();
    command.consumed = newline + 1;

    size_t position = 0;
    while (position < end && input[position] == ' ') {
        position++;
    }
    if (position == end) {
        return ParseResult::COMPLETE;
    }

    size_t command_end = input.find(' ', position);
    if (command_end == string::npos || command_end > end) {
        command.parts.emplace_back(input.data() + position, end - position);
        return ParseResult::COMPLETE;
    }
    command.parts.emplace_back(input.data() + position, command_end - position);
    position = command_end;

    while (position < end && input[position] == ' ') {
        position++;
    }
    if (position == end) {
        return ParseResult::COMPLETE;
    }

    size_t first_argument_end = input.find(' ', position);
    if (first_argument_end == string::npos || first_argument_end > end) {
        command.parts.emplace_back(input.data() + position, end - position);
        return ParseResult::COMPLETE;
    }
    command.parts.emplace_back(input.data() + position, first_argument_end - position);
    position = first_argument_end;

    while (position < end && input[position] == ' ') {
        position++;
    }
    if (position < end) {
        command.parts.emplace_back(input.data() + position, end - position);
    }
    return ParseResult::COMPLETE;
}

ParseResult parse_next_command(const shared_ptr<ClientContext>& client_context, ParsedCommand& command) {
    if (client_context->input_buffer.empty()) {
        return ParseResult::INCOMPLETE;
    }
    if (client_context->protocol == ProtocolMode::UNDECIDED) {
        client_context->protocol = client_context->input_buffer.front() == '*'
            ? ProtocolMode::RESP2
            : ProtocolMode::LINE;
    }

    if (client_context->protocol == ProtocolMode::RESP2) {
        return parse_resp_command(client_context->input_buffer, command);
    }
    return parse_line_command(client_context->input_buffer, command);
}

void initWal() {
    ifstream file("wal.txt");
    string line;
    while (getline(file, line)) {
        if (line.rfind("DELETE ", 0) == 0) {
            erase_value(line.substr(7));
            continue;
        }
        if (line.rfind("SET ", 0) != 0) {
            continue;
        }
        size_t key_start = 4;
        size_t separator = line.find(' ', key_start);
        if (separator == string::npos || separator == key_start || separator + 1 >= line.size()) {
            continue;
        }
        put_value(line.substr(key_start, separator - key_start), line.substr(separator + 1));
    }
}

void disconnect_client(const shared_ptr<ClientContext>& client_context) {
    SOCKET client_socket = client_context->socket;
    bool should_close_socket = false;
    {
        lock_guard<mutex> lock(client_context->mtx);
        if (!client_context->connected) {
            return;
        }
        client_context->connected = false;
        client_context->read_queued = false;
        client_context->write_pending = false;
        if (client_context->worker_active) {
            client_context->close_when_worker_finishes = true;
        } else {
            should_close_socket = true;
        }
    }

    {
        lock_guard<mutex> lock(pubsub_mutex);
        for (auto& [channel, subscribers] : channel_map) {
            subscribers.erase(remove_if(subscribers.begin(), subscribers.end(), [&](const weak_ptr<ClientContext>& subscriber) {
                auto client = subscriber.lock();
                return !client || client == client_context;
            }), subscribers.end());
        }
    }

    {
        lock_guard<mutex> lock(clients_mutex);
        clients.erase(remove(clients.begin(), clients.end(), client_context), clients.end());
    }

    if (should_close_socket) {
        closesocket(client_socket);
    }
}

bool begin_worker(const shared_ptr<ClientContext>& client_context) {
    lock_guard<mutex> lock(client_context->mtx);
    if (!client_context->connected) {
        client_context->read_queued = false;
        return false;
    }
    client_context->worker_active = true;
    return true;
}

void finish_worker(const shared_ptr<ClientContext>& client_context, bool reschedule) {
    bool should_enqueue = false;
    bool should_close_socket = false;
    {
        lock_guard<mutex> lock(client_context->mtx);
        client_context->worker_active = false;
        if (client_context->connected) {
            client_context->read_queued = reschedule;
            should_enqueue = reschedule;
        } else {
            client_context->read_queued = false;
            should_close_socket = client_context->close_when_worker_finishes;
            client_context->close_when_worker_finishes = false;
        }
    }
    if (should_close_socket) {
        closesocket(client_context->socket);
    }
    if (should_enqueue) {
        workers_queue.enqueue(client_context);
    }
    signal_poll_loop();
}

void workerFunction(shared_ptr<ClientContext> client_context) {
    if (!begin_worker(client_context)) {
        return;
    }

    size_t received_bytes = 0;
    bool hit_byte_limit = false;

    while (received_bytes < MAX_BATCH_BYTES) {
        char buffer[4096];
        int capacity = static_cast<int>(min(sizeof(buffer), MAX_BATCH_BYTES - received_bytes));
        int bytes_received = recv(client_context->socket, buffer, capacity, 0);

        if (bytes_received > 0) {
            client_context->input_buffer.append(buffer, bytes_received);
            received_bytes += static_cast<size_t>(bytes_received);
            if (client_context->input_buffer.size() > MAX_INBOUND_BYTES) {
                disconnect_client(client_context);
                finish_worker(client_context, false);
                return;
            }
            continue;
        }
        if (bytes_received == 0) {
            disconnect_client(client_context);
            finish_worker(client_context, false);
            return;
        }

        int error = WSAGetLastError();
        if (error == WSAEINTR) {
            continue;
        }
        if (error != WSAEWOULDBLOCK) {
            cerr << "Socket receive failed: " << error << '\n';
            disconnect_client(client_context);
            finish_worker(client_context, false);
            return;
        }
        break;
    }
    hit_byte_limit = received_bytes == MAX_BATCH_BYTES;

    size_t processed_commands = 0;
    bool hit_command_limit = false;
    while (processed_commands < MAX_BATCH_COMMANDS) {
        ParsedCommand command;
        ParseResult result = parse_next_command(client_context, command);
        if (result == ParseResult::INCOMPLETE) {
            break;
        }
        if (result == ParseResult::INVALID) {
            send_command_error(client_context, "Malformed request");
            disconnect_client(client_context);
            finish_worker(client_context, false);
            return;
        }

        if (!command.parts.empty()) {
            dispatch_command(command, client_context);
            processed_commands++;
        }
        client_context->input_buffer.erase(0, command.consumed);

        bool closing_after_flush = false;
        {
            lock_guard<mutex> lock(client_context->mtx);
            closing_after_flush = client_context->closing_after_flush;
        }
        if (closing_after_flush) {
            break;
        }
    }
    hit_command_limit = processed_commands == MAX_BATCH_COMMANDS;

    finish_worker(client_context, hit_byte_limit || hit_command_limit);
}

void workerLoop() {
    while (true) {
        auto client = workers_queue.dequeue();
        active_worker_count++;
        workerFunction(client);
        active_worker_count--;
    }
}

void flush_output(const shared_ptr<ClientContext>& client_context) {
    bool should_disconnect = false;
    while (true) {
        {
            lock_guard<mutex> lock(client_context->mtx);
            if (!client_context->connected) {
                return;
            }
            if (client_context->output_offset == client_context->output_buffer.size()) {
                client_context->output_buffer.clear();
                client_context->output_offset = 0;
                client_context->write_pending = false;
                should_disconnect = client_context->closing_after_flush;
                break;
            }

            const char* data = client_context->output_buffer.data() + client_context->output_offset;
            int length = static_cast<int>(client_context->output_buffer.size() - client_context->output_offset);
            int bytes_sent = send(client_context->socket, data, length, 0);
            if (bytes_sent > 0) {
                client_context->output_offset += static_cast<size_t>(bytes_sent);
                continue;
            }
            if (bytes_sent == SOCKET_ERROR && WSAGetLastError() == WSAEWOULDBLOCK) {
                return;
            }
            should_disconnect = true;
            break;
        }
    }

    if (should_disconnect) {
        disconnect_client(client_context);
    }
}

void drain_poll_signal() {
    char buffer[256];
    while (recv(poll_wakeup_read, buffer, sizeof(buffer), 0) > 0) {
    }
}

void signal_poll_loop() {
    if (poll_wakeup_write == INVALID_SOCKET) {
        return;
    }
    char signal = 1;
    send(poll_wakeup_write, &signal, 1, 0);
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
        poll_fds.push_back(WSAPOLLFD{poll_wakeup_read, POLLRDNORM, 0});

        for (const auto& client : client_snapshot) {
            lock_guard<mutex> lock(client->mtx);
            if (!client->connected) {
                continue;
            }

            short events = 0;
            if (!client->read_queued && !client->closing_after_flush) {
                events |= POLLRDNORM;
            }
            if (client->write_pending) {
                events |= POLLWRNORM;
            }
            poll_fds.push_back(WSAPOLLFD{client->socket, events, 0});
            poll_clients.push_back(client);
        }

        int poll_result = WSAPoll(poll_fds.data(), static_cast<ULONG>(poll_fds.size()), -1);
        if (poll_result == SOCKET_ERROR) {
            int error = WSAGetLastError();
            if (error != WSAEINTR) {
                cerr << "Socket poll failed: " << error << '\n';
            }
            continue;
        }

        if (poll_fds[0].revents & POLLRDNORM) {
            drain_poll_signal();
        }

        for (size_t index = 0; index < poll_clients.size(); index++) {
            const auto& client = poll_clients[index];
            short events = poll_fds[index + 1].revents;

            if (events & (POLLERR | POLLHUP | POLLNVAL)) {
                disconnect_client(client);
                continue;
            }
            if (events & POLLWRNORM) {
                flush_output(client);
            }
            if (events & POLLRDNORM) {
                bool should_enqueue = false;
                {
                    lock_guard<mutex> lock(client->mtx);
                    if (client->connected && !client->read_queued && !client->closing_after_flush) {
                        client->read_queued = true;
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
        SOCKET client_socket = accept(server_socket, reinterpret_cast<sockaddr*>(&clientaddr), &clientaddr_size);
        if (client_socket == INVALID_SOCKET) {
            cerr << "Accept failed: " << WSAGetLastError() << '\n';
            continue;
        }

        setSocketNonBlocking(client_socket);
        auto client = make_shared<ClientContext>();
        client->socket = client_socket;
        {
            lock_guard<mutex> lock(clients_mutex);
            clients.push_back(client);
        }
        signal_poll_loop();
    }
}

void setSocketNonBlocking(SOCKET socket) {
    u_long mode = 1;
    ioctlsocket(socket, FIONBIO, &mode);
}

bool createPollWakeupSockets() {
    SOCKET listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listener == INVALID_SOCKET) {
        return false;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == SOCKET_ERROR ||
        listen(listener, 1) == SOCKET_ERROR) {
        closesocket(listener);
        return false;
    }

    int address_size = sizeof(address);
    if (getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_size) == SOCKET_ERROR) {
        closesocket(listener);
        return false;
    }

    poll_wakeup_write = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (poll_wakeup_write == INVALID_SOCKET ||
        connect(poll_wakeup_write, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == SOCKET_ERROR) {
        if (poll_wakeup_write != INVALID_SOCKET) {
            closesocket(poll_wakeup_write);
        }
        closesocket(listener);
        poll_wakeup_write = INVALID_SOCKET;
        return false;
    }

    poll_wakeup_read = accept(listener, nullptr, nullptr);
    closesocket(listener);
    if (poll_wakeup_read == INVALID_SOCKET) {
        closesocket(poll_wakeup_write);
        poll_wakeup_write = INVALID_SOCKET;
        return false;
    }

    setSocketNonBlocking(poll_wakeup_read);
    setSocketNonBlocking(poll_wakeup_write);
    return true;
}

int main() {
    WSADATA wsa_data{};
    if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
        cerr << "Socket startup failed.\n";
        return 1;
    }

    initWal();
    wal_coordinator.start();
    if (!createPollWakeupSockets()) {
        cerr << "Could not create poll wakeup sockets.\n";
        WSACleanup();
        return 1;
    }

    SOCKET server_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (server_socket == INVALID_SOCKET) {
        cerr << "Socket creation failed.\n";
        WSACleanup();
        return 1;
    }

    sockaddr_in server_address{};
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(6379);
    server_address.sin_addr.s_addr = INADDR_ANY;
    if (bind(server_socket, reinterpret_cast<sockaddr*>(&server_address), sizeof(server_address)) == SOCKET_ERROR ||
        listen(server_socket, SOMAXCONN) == SOCKET_ERROR) {
        cerr << "Server bind/listen failed: " << WSAGetLastError() << '\n';
        closesocket(server_socket);
        WSACleanup();
        return 1;
    }

    cout << "Server boot complete. Awaiting client lines...\n";
    vector<thread> thread_pool;
    unsigned int worker_count = max(1u, thread::hardware_concurrency());
    for (unsigned int index = 0; index < worker_count; index++) {
        thread_pool.emplace_back(workerLoop);
    }

    thread poll_thread(pollLoop);
    thread accept_thread(acceptFunction, server_socket);
    accept_thread.join();
    poll_thread.join();

    closesocket(server_socket);
    closesocket(poll_wakeup_read);
    closesocket(poll_wakeup_write);
    WSACleanup();
    return 0;
}
