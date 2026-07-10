#include <ankerl/unordered_dense.h>
#include <chrono>
#include <iostream>
#include <string>
#include <vector>

using namespace std;

class LearningFlatMap {
    struct Entry { string key; string value; bool occupied = false; bool tombstone = false; };
    vector<Entry> entries_ = vector<Entry>(16);
    size_t size_ = 0;
    size_t index(const string& key) const { return hash<string>{}(key) % entries_.size(); }
    void grow() {
        auto old = move(entries_); entries_ = vector<Entry>(old.size() * 2); size_ = 0;
        for (auto& entry : old) if (entry.occupied) put(move(entry.key), move(entry.value));
    }
public:
    void put(string key, string value) {
        if ((size_ + 1) * 10 >= entries_.size() * 7) grow();
        size_t pos = index(key), first_tombstone = entries_.size();
        while (entries_[pos].occupied || entries_[pos].tombstone) {
            if (entries_[pos].occupied && entries_[pos].key == key) { entries_[pos].value = move(value); return; }
            if (entries_[pos].tombstone && first_tombstone == entries_.size()) first_tombstone = pos;
            pos = (pos + 1) % entries_.size();
        }
        pos = first_tombstone == entries_.size() ? pos : first_tombstone;
        entries_[pos] = {move(key), move(value), true, false}; ++size_;
    }
    bool get(const string& key, string& value) const {
        size_t pos = index(key); while (entries_[pos].occupied || entries_[pos].tombstone) {
            if (entries_[pos].occupied && entries_[pos].key == key) { value = entries_[pos].value; return true; }
            pos = (pos + 1) % entries_.size(); }
        return false;
    }
    bool erase(const string& key) {
        size_t pos = index(key); while (entries_[pos].occupied || entries_[pos].tombstone) {
            if (entries_[pos].occupied && entries_[pos].key == key) { entries_[pos].occupied = false; entries_[pos].tombstone = true; --size_; return true; }
            pos = (pos + 1) % entries_.size(); } return false;
    }
};

int main() {
    LearningFlatMap learning; string value;
    learning.put("a", "one"); learning.put("a", "two");
    if (!learning.get("a", value) || value != "two") return 1;
    if (!learning.erase("a") || learning.get("a", value)) return 1;
    for (int i = 0; i < 10000; ++i) learning.put("key" + to_string(i), "value");
    size_t checksum = 0;
    auto start = chrono::steady_clock::now(); for (int i = 0; i < 1000000; ++i) { if (!learning.get("key" + to_string(i % 10000), value)) return 1; checksum += value.size(); }
    auto learning_ms = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();
    ankerl::unordered_dense::map<string, string> production; for (int i = 0; i < 10000; ++i) production["key" + to_string(i)] = "value";
    start = chrono::steady_clock::now(); for (int i = 0; i < 1000000; ++i) { auto it = production.find("key" + to_string(i % 10000)); if (it == production.end()) return 1; checksum += it->second.size(); }
    auto production_ms = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();
    cout << "LearningFlatMap lookup: " << learning_ms << " ms\nTrusted unordered_dense lookup: " << production_ms << " ms\nChecksum: " << checksum << "\n";
}
