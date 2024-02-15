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
#include <unordered_map>

using namespace std;

#define MAX_WORKERS 5

map<string, string> myDatastore;
queue<int> myJobQueue;
pthread_mutex_t myDatastoreMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t myJobQueueMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t myPoolMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t myCountMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t myActiveMutex = PTHREAD_MUTEX_INITIALIZER;
int myActualCount = 0;
int myActiveWorkers = 0;

struct MyThread {
    pthread_t id;
    int fd;
};

struct MyHelper {
    struct MyThread *threadArr;
    size_t size;
};

void writeData(int *counter, string &key, string &value) {
    value.erase(value.begin(), value.begin() + 1);
    pthread_mutex_lock(&myDatastoreMutex);
    myDatastore[key] = value;
    pthread_mutex_unlock(&myDatastoreMutex);
    (*counter)++;
}

void removeEntry(string &key, char *output) {
    pthread_mutex_lock(&myDatastoreMutex);
    int erased = myDatastore.erase(key);
    pthread_mutex_unlock(&myDatastoreMutex);
    if (erased==0) {
        strcat(output, "NULL\n");
    } else {
        strcat(output, "FIN\n");
    }
}

void getCount(char *output) {
    pthread_mutex_lock(&myDatastoreMutex);
    int count = myDatastore.size();
    pthread_mutex_unlock(&myDatastoreMutex);
    string countStr = to_string(count);
    strcat(output, countStr.c_str());
    strcat(output, "\n");
}

void readData(string &key, char *output) {
    pthread_mutex_lock(&myDatastoreMutex);
    if (myDatastore.find(key) != myDatastore.end()) {
        strcat(output, myDatastore[key].c_str());
        strcat(output, "\n");
    } else {
        strcat(output, "NULL\n");
    }
    pthread_mutex_unlock(&myDatastoreMutex);
}

int processRequest(char *input, char *output) {
    output[0] = '\0';
    char delimiter = '\n';
    istringstream ss(input);
    string token;
    vector<string> tokens;
    while (getline(ss, token, delimiter)) {
        tokens.push_back(token);
    }
    for (int i = 0; i < tokens.size(); i++) {
        auto &str = tokens[i];
        if (str == "WRITE") {
            auto &key = tokens[i + 1];
            auto &val = tokens[i + 2];
            writeData(&i, key, val);
            strcat(output, "FIN\n");
        } else if (str == "READ") {
            auto &key = tokens[i + 1];
            readData(key, output);
            i++;
        } else if (str == "COUNT") {
            getCount(output);
        } else if (str == "DELETE") {
            auto &key = tokens[i + 1];
            removeEntry(key, output);
            i++;
        } else if (str == "END") {
            strcat(output, "\n");
            return -1;
        }
    }
    return 0;
}

void *handleClient(void *arg) {
    int clientSocket = *((int *)arg);
    char buffer[1024];
    int valRead;
    while ((valRead = read(clientSocket, buffer, sizeof(buffer))) > 0) {
        char response[1024];
        int returnVal = processRequest(buffer, response);
        if (returnVal == -1) {
            send(clientSocket, response, strlen(response), 0);
            close(clientSocket);
            pthread_mutex_lock(&myCountMutex);
            myActualCount--;
            pthread_mutex_unlock(&myCountMutex);
            pthread_exit(NULL);
        } else {
            send(clientSocket, response, strlen(response), 0);
        }
        memset(buffer, 0, sizeof(buffer));
    }
    close(clientSocket);
    pthread_exit(NULL);
}

void *threadPoolHelper(void *args) {
    struct MyHelper *helper = static_cast<MyHelper *>(args);
    struct MyThread *threadPool = helper->threadArr;
    while (true) {
        pthread_mutex_lock(&myJobQueueMutex);
        int isQueueEmpty = myJobQueue.empty();
        pthread_mutex_unlock(&myJobQueueMutex);
        if (!isQueueEmpty) {
            for (int i = 0; i < helper->size; i++) {
                pthread_mutex_lock(&myPoolMutex);
                int terminationStatus = pthread_tryjoin_np(threadPool[i].id, NULL);
                pthread_mutex_unlock(&myPoolMutex);
                if (terminationStatus == 0) {
                    pthread_mutex_lock(&myJobQueueMutex);
                    int frontElement = myJobQueue.front();
                    myJobQueue.pop();
                    pthread_mutex_unlock(&myJobQueueMutex);
                    pthread_mutex_lock(&myPoolMutex);
                    threadPool[i].fd = frontElement;
                    pthread_create(&threadPool[i].id, NULL, &handleClient, &threadPool[i].fd);
                    pthread_mutex_unlock(&myPoolMutex);
                }
            }
        }
        usleep(10000);
    }
}

int main(int argc, char **argv) {
    int portNumber;
    if (argc != 2) {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }
    portNumber = atoi(argv[1]);
    printf("%d\t", portNumber);

    struct sockaddr_in server, client;
    int clientLength, newSocket;
    clientLength = sizeof(server);
    char buffer[1500];
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    memset((char *)&server, 0, sizeof(server));
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_ANY);
    server.sin_port = htons(portNumber);
    bind(sock, (struct sockaddr *)&server, sizeof(server));
    printf("starting TCP server\n");
    listen(sock, 100);
    pthread_t thread;

    int threadNumber = MAX_WORKERS;
    struct MyThread threadPool[threadNumber];
    struct MyHelper threadArgs;
    threadArgs.threadArr = threadPool;
    threadArgs.size = threadNumber;
    pthread_create(&thread, NULL, &threadPoolHelper, &threadArgs);
    int returnValue = 0;
    while (1) {
        newSocket = accept(sock, (struct sockaddr *)&client, (socklen_t *)&clientLength);
        pthread_mutex_lock(&myCountMutex);
        int actualNo = myActualCount++;
        pthread_mutex_unlock(&myCountMutex);
        if (newSocket >= 0 && actualNo < threadNumber) {
            pthread_mutex_lock(&myPoolMutex);
            threadPool[actualNo].fd = newSocket;
            pthread_create(&threadPool[actualNo].id, NULL, &handleClient, &threadPool[actualNo].fd);
            pthread_mutex_unlock(&myPoolMutex);
        } else {
            pthread_mutex_lock(&myJobQueueMutex);
            printf("pushing and printing the contents of queue\n");
            myJobQueue.push(newSocket);
            pthread_mutex_unlock(&myJobQueueMutex);
        }
    }

    close(sock);
    shutdown(sock, SHUT_RDWR);
    return 0;
}
