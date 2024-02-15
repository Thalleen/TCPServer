#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <string>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/socket.h>
#include <unordered_map>
#include <sstream>
#include <iostream>
#include <vector>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <queue>
#include <chrono>
#include <thread>

using namespace std;
 
struct WorkerThread {
    pthread_t thread_id;
    int connection_fd;
};

struct WorkerThreadArgs {
    struct WorkerThread* thread_array;
    size_t array_size;
};

queue<int> connection_queue;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t active_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t dictionary_mutex = PTHREAD_MUTEX_INITIALIZER;
int active_workers = 0;
unordered_map<string, string> data_dictionary;

void write_data(int* counter, string& key, string& value) {
    value.erase(value.begin(), value.begin() + 1);
    data_dictionary[key] = value;
    (*counter)++;
    (*counter)++;
}


void remove_entry(string& key, char* output) {
    int ret = data_dictionary.erase(key);
    if (ret == 0) {
        string res = "NULL\n";
        strcat(output, res.c_str());
    } else {
        string res = "FIN\n";
        strcat(output, res.c_str());
    }
}

void get_count(char* output) {
    int count = 0;
    for (auto& entry : data_dictionary) {
        count++;
    }
    string count_str = to_string(count);
    strcat(output, count_str.c_str());
    strcat(output, "\n");
}

void read_data(string& key, char* output) {
    if (data_dictionary.find(key) == data_dictionary.end()) {
        strcat(output, "NULL\n");
    } else {
        strcat(output, data_dictionary.find(key)->second.c_str());
        strcat(output, "\n");
    }
}




int process_request(char* input, char* output) {
    output[0] = '\0';
    char delimiter = '\n';
    istringstream ss(input);
    string token;
    vector<string> tokens;
    while (getline(ss, token, delimiter)) {
        tokens.push_back(token);
    }
    for (int i = 0; i < tokens.size(); i++) {
        auto& str = tokens[i];
        if (str == "WRITE") {
            auto& key = tokens[i + 1];
            auto& val = tokens[i + 2];
            pthread_mutex_lock(&dictionary_mutex);
            write_data(&i, key, val);
            pthread_mutex_unlock(&dictionary_mutex);
            strcat(output, "FIN\n");
        } else if (str == "READ") {
            auto& key = tokens[i + 1];
            pthread_mutex_lock(&dictionary_mutex);
            read_data(key, output);
            pthread_mutex_unlock(&dictionary_mutex);
            i++;
        } else if (str == "COUNT") {
            pthread_mutex_lock(&dictionary_mutex);
            get_count(output);
            pthread_mutex_unlock(&dictionary_mutex);
        } else if (str == "DELETE") {
            auto& key = tokens[i + 1];
            pthread_mutex_lock(&dictionary_mutex);
            remove_entry(key, output);
            pthread_mutex_unlock(&dictionary_mutex);
            i++;
        } else if (str == "END") {
            strcat(output, "\n");
            return -1;
        }
    }
    return 0;
}

void* serve_client(void* arg) {
    int* fd_ptr = static_cast<int*>(arg);
    int connection_fd = *fd_ptr;
    while (1) {
        char buffer[1024];
        int read_val = read(connection_fd, buffer, 1024);
        if (read_val > 0) {
            char response[1024];
            int return_val = process_request(buffer, response);
            if (return_val == -1) {
                char buff[100] = {"received message\n"};
                strcpy(buff, response);
                write(connection_fd, buff, strlen(buff));
                close(connection_fd);
                shutdown(connection_fd, SHUT_RDWR);
                pthread_mutex_lock(&active_mutex);
                active_workers--;
                pthread_mutex_unlock(&active_mutex);
                break;
            }
        }
    }
    pthread_exit(NULL);
}

void* worker_function(void* args) {
    struct WorkerThreadArgs* worker_args = static_cast<WorkerThreadArgs*>(args);
    struct WorkerThread* thread_pool = worker_args->thread_array;
    while (1) {
        pthread_mutex_lock(&queue_mutex);
        int queue_empty = !connection_queue.empty();
        pthread_mutex_unlock(&queue_mutex);
        if (queue_empty) {
            pthread_mutex_lock(&queue_mutex);
            for (int i = 0; i < worker_args->array_size; i++) {
                pthread_mutex_lock(&queue_mutex);
                int thread_termination_status = pthread_tryjoin_np(thread_pool[i].thread_id, NULL);
                pthread_mutex_unlock(&queue_mutex);
                if (thread_termination_status == 0) {
                    pthread_join(thread_pool[i].thread_id, NULL);
                    pthread_mutex_lock(&queue_mutex);
                    int front_element = connection_queue.front();
                    connection_queue.pop();
                    pthread_mutex_unlock(&queue_mutex);
                    pthread_mutex_lock(&queue_mutex);
                    thread_pool[i].connection_fd = front_element;
                    pthread_create(&thread_pool[i].thread_id, NULL, &serve_client, &thread_pool[i].connection_fd);
                    pthread_mutex_unlock(&queue_mutex);
                }
            }
        }
        usleep(1000);
    }
}

int main(int argc, char** argv) {
    int port_number;
    if (argc != 2) {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }
    port_number = atoi(argv[1]);
    printf("%d\t", port_number);
    struct sockaddr_in server_address, client_address;
    int client_length, new_socket;
    client_length = sizeof(server_address);
    char buffer[1500];
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);
    memset((char *)&server_address, 0, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = htonl(INADDR_ANY);
    server_address.sin_port = htons(port_number);
    bind(server_socket, (struct sockaddr *)&server_address, sizeof(server_address));
    printf("starting TCP server\n");
    listen(server_socket, 100);
    pthread_t worker_thread;
    int num_worker_threads = 8;
    struct WorkerThread worker_pool[num_worker_threads];
    struct WorkerThreadArgs worker_args;
    worker_args.thread_array = worker_pool;
    worker_args.array_size = num_worker_threads;
    pthread_create(&worker_thread, NULL, &worker_function, &worker_args);
    int return_value = 0;
    while (1) {
        new_socket = accept(server_socket, (struct sockaddr *)&client_address, (socklen_t *)&client_length);
        pthread_mutex_lock(&active_mutex);
        int current_active = active_workers++;
        pthread_mutex_unlock(&active_mutex);
        if (new_socket >= 0 && current_active < num_worker_threads) {
            pthread_mutex_lock(&queue_mutex);
            worker_pool[current_active].connection_fd = new_socket;
            pthread_create(&worker_pool[current_active].thread_id, NULL, &serve_client, &worker_pool[current_active].connection_fd);
            pthread_mutex_unlock(&queue_mutex);
        } else {
            pthread_mutex_lock(&queue_mutex);
            printf("pushing and printing the contents of queue\n");
            connection_queue.push(new_socket);
            pthread_mutex_unlock(&queue_mutex);
        }
    }
    close(server_socket);
    shutdown(server_socket, SHUT_RDWR);
    return 0;
}
