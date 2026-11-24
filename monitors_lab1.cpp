#include <iostream>
#include <string>
#include <pthread.h>
#include <unistd.h>

using namespace std;

struct EventData {
    int id;
    string payload;

    EventData(int i, string p) : id(i), payload(p) {}
};

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t condition = PTHREAD_COND_INITIALIZER;
bool stopping = false;
bool ready = false;
EventData* shared_data = nullptr;

void* producer(void* arg) {
    int counter = 0;
    while (true) {
        sleep(1);
        pthread_mutex_lock(&mutex);
        if (stopping) {
            pthread_mutex_unlock(&mutex);
            break;
        }
        if (ready == true) {
            pthread_mutex_unlock(&mutex);
            continue;
        }
        counter++;
        shared_data = new EventData(counter, "Данные события");
        ready = true;
        cout << "[Поставщик] Событие " << counter << " отправлено" << endl;
        pthread_cond_signal(&condition);
        pthread_mutex_unlock(&mutex);
    }
    return nullptr;
}

void* consumer(void* arg) {
    pthread_mutex_lock(&mutex);

    while (true) {
        while (ready == false) {
            if (stopping) {
                pthread_mutex_unlock(&mutex);
                return nullptr;
            }

            pthread_cond_wait(&condition, &mutex);
        }

        EventData* ev = shared_data;
        shared_data = nullptr;
        ready = false;

        cout << "[Потребитель] Событие" << ev->id << " получено. Payload: " << ev->payload << endl;

        delete ev;
    }

    return nullptr;
}

int main() {
    pthread_t prod, cons;

    pthread_create(&prod, nullptr, producer, nullptr);
    pthread_create(&cons, nullptr, consumer, nullptr);

    sleep(5);

    pthread_mutex_lock(&mutex);
    stopping = true;
    pthread_cond_broadcast(&condition);
    pthread_mutex_unlock(&mutex);

    pthread_join(prod, nullptr);
    pthread_join(cons, nullptr);

    pthread_mutex_lock(&mutex);
    if (shared_data != nullptr) {
        delete shared_data;
        shared_data = nullptr;
        ready = false;
    }
    pthread_mutex_unlock(&mutex);

    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&condition);
    return 0;
}
