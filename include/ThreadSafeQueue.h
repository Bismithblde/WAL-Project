#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

struct ClientContext;

class ThreadSafeQueue {
public:
    void enqueue(std::shared_ptr<ClientContext> client);
    std::shared_ptr<ClientContext> dequeue();

private:
    std::queue<std::shared_ptr<ClientContext>> raw_queue;
    std::mutex mtx;
    std::condition_variable cv;
};
