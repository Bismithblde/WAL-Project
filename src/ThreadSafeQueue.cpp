#include <ThreadSafeQueue.h>

void ThreadSafeQueue::enqueue(std::shared_ptr<ClientContext> client) {
    std::unique_lock<std::mutex> lock(mtx);
    raw_queue.push(client);
    cv.notify_one();
}

std::shared_ptr<ClientContext> ThreadSafeQueue::dequeue() {
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [this]() { return !raw_queue.empty(); });

    auto client = raw_queue.front();
    raw_queue.pop();
    return client;
}
