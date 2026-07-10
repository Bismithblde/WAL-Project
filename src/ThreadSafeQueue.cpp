#include <ThreadSafeQueue.h>
#include <main>

void ThreadSafeQueue::enqueue(std::shared_ptr<ClientContext> client) {
    std::unique_lock<std::mutex> lock(mtx);
    raw_queue.push(client);
    cv.notify_one();
}

std::shared_ptr<ClientContext> ThreadSafeQueue::dequeue() {
    std::unique_lock<std::mutex> lock(mtx);
    raw_queue.pop();
    while (raw_queue.empty()) {
        cv.
    }
}