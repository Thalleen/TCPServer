#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <queue>
#include <pthread.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>

using namespace std;

#define MAX_WORKERS 10

map<string, string> data_store;
queue<int> task_queue;

namespace custom_ns{
    pthread_mutex_t mutex_lock = PTHREAD_MUTEX_INITIALIZER;
}

int current_workers = 0;

void *manage_client(void *arg)
{
    int client_sock = *((int *)arg);
    char buffer[1024] = {0};
    int val_read;
    while ((val_read = read(client_sock, buffer, sizeof(buffer))) > 0)
    {
        istringstream request(buffer);
        string token;
        vector<string> tokens;
        while (getline(request, token, '\n'))
        {
            tokens.push_back(token);
        }
        for (int i = 0; i < tokens.size(); i++)
        {
            string req_line = tokens[i];

            if (req_line.find("WRITE") != -1)
            {
                string req_key, req_value;
                string method = tokens[i];    
                req_key = tokens[i+1];
                req_value = tokens[i+2];
                i += 2;

                istringstream iss(req_key);
                string key, value;
                
                iss >> key;

                req_value.erase(req_value.begin(), req_value.begin() + 1);
                value = req_value;

                pthread_mutex_lock(&custom_ns::mutex_lock);
                data_store[key] = value;
                pthread_mutex_unlock(&custom_ns::mutex_lock);

                string response = "FIN\n";
                send(client_sock, response.c_str(), response.length(), 0);
            }
            else if (req_line.find("READ") != -1)
            {
                string method = tokens[i];
                req_line = tokens[i+1];
                i += 1;
                istringstream iss(req_line);
                string key;

                iss >> key;

                string value;
                if (data_store.find(key) != data_store.end())
                {
                    value = data_store[key];
                    string response = value + "\n";
                    send(client_sock, response.c_str(), response.length(), 0);
                }
                else
                {
                    string response = "NULL\n";
                    send(client_sock, response.c_str(), response.length(), 0);
                }
            }
            else if (req_line.find("COUNT") != -1)
            {
                pthread_mutex_lock(&custom_ns::mutex_lock);
                int count = data_store.size();
                pthread_mutex_unlock(&custom_ns::mutex_lock);
                string count_str = to_string(count);
                string response = count_str + "\n";
                send(client_sock, response.c_str(), response.length(), 0);
            }
            else if (req_line.find("DELETE") != -1)
            {
                string method = tokens[i];
                req_line = tokens[i+1];
                i += 1;

                istringstream iss(req_line);
                string key;

                iss >> key;

                string response;
                pthread_mutex_lock(&custom_ns::mutex_lock);
                int erased = data_store.erase(key);
                if (erased) {
                    response = "FIN\n";
                    send(client_sock, response.c_str(), response.length(), 0);
                } else {
                    response = "NULL\n";
                    send(client_sock, response.c_str(), response.length(), 0);
                }
                pthread_mutex_unlock(&custom_ns::mutex_lock);
            }
            else if (req_line.find("END") != -1)
            {
                pthread_mutex_lock(&custom_ns::mutex_lock);
                current_workers--;
                string response = "\n";
                send(client_sock, response.c_str(), response.length(), 0);
                close(client_sock);
                pthread_mutex_unlock(&custom_ns::mutex_lock);
                pthread_exit(NULL);
            }
            else
            {
                string response = "\nInvalid command\n";
                send(client_sock, response.c_str(), response.length(), 0);
            }
        }
        memset(buffer, 0, sizeof(buffer));
    }
    close(client_sock);
    pthread_mutex_lock(&custom_ns::mutex_lock);
    current_workers--;
    pthread_mutex_unlock(&custom_ns::mutex_lock);
    pthread_exit(NULL);
}

void *thread_pool_manager(void *)
{
    while (true)
    {
        if (!task_queue.empty())
        {
            pthread_mutex_lock(&custom_ns::mutex_lock);
            if (current_workers < MAX_WORKERS)
            {
                int client = task_queue.front();
                task_queue.pop();
                pthread_t thread;
                pthread_create(&thread, NULL, &manage_client, &client);
                current_workers++;
            }
            pthread_mutex_unlock(&custom_ns::mutex_lock);
        }
    }
}

int main(int argc, char*argv[])
{
    int port_number;
    if (argc != 2) {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }

    port_number = atoi(argv[1]);

    int server_socket, client_socket;
    struct sockaddr_in address;

    if ((server_socket = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        perror("Socket creation failed");
        return -1;
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port_number);

    bind(server_socket, (struct sockaddr *)&address, sizeof(address));
    
    if (listen(server_socket, 5) < 0)
    {
        perror("Listen failed");
        return -1;
    }

    cout << "Server listening on port " << port_number << "..." << endl;

    pthread_t thread_pool_thread;
    pthread_create(&thread_pool_thread, NULL, &thread_pool_manager, NULL);

    while (true)
    {
        client_socket = accept(server_socket, NULL, NULL);
        if (client_socket < 0)
        {
            perror("Accept failed");
            return -1;
        }

        pthread_mutex_lock(&custom_ns::mutex_lock);
        if (current_workers < MAX_WORKERS)
        {
            pthread_t thread;
            pthread_create(&thread, NULL, &manage_client, &client_socket);
            current_workers++;
        }
        else
        {
            task_queue.push(client_socket);
        }
        pthread_mutex_unlock(&custom_ns::mutex_lock);
    }

    close(server_socket);
    return 0;
}
