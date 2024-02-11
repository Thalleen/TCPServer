#include <iostream>
#include <sstream>
#include <cstring>
#include <map>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include <arpa/inet.h>

std::map<std::string, std::string> KV_DATASTORE;

std::string handle_message(const std::vector<std::string>& ip) {
    std::string response;
    for (int i = 0; i < ip.size(); i++) {
        std::string command = ip[i];

        if (command == "READ") {
            if (i + 1 < ip.size()) {
                std::string key = ip[i + 1];
                if (KV_DATASTORE.find(key) == KV_DATASTORE.end()) {
                    response += "NULL\n";
                } else {
                    response += KV_DATASTORE[key] + "\n";
                }
                i++;  // Skip the key
            }
        } else if (command == "WRITE") {
            if (i + 2 < ip.size()) {
                std::string key = ip[i + 1];
                std::string value = ip[i + 2];
                // Remove trailing colons if present
                key.erase(std::remove(key.begin(), key.end(), ':'), key.end());
                value.erase(std::remove(value.begin(), value.end(), ':'), value.end());
                KV_DATASTORE[key] = value;
                response += "FIN\n";
                i += 2;  // Skip the key and value
            }
        } else if (command == "COUNT") {
            response += std::to_string(KV_DATASTORE.size()) + "\n";
        } else if (command == "DELETE") {
            if (i + 1 < ip.size()) {
                std::string key = ip[i + 1];
                if (KV_DATASTORE.erase(key) > 0) {
                    response += "FIN\n";
                } else {
                    response += "NULL\n";
                }
                i++;  // Skip the key
            }
        } else if (command == "END") {
            response += '\n';
            
        }
    }
    // Append a newline to the response before sending
    
    return response;
}

int main() {
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in server_address{};
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(8080);

    bind(server_socket, (struct sockaddr*)&server_address, sizeof(server_address));
    listen(server_socket, 1);

    std::cout << "Server listening on port 8080..." << std::endl;

    while (true) {
        sockaddr_in client_address{};
        socklen_t client_address_len = sizeof(client_address);
        int client_socket = accept(server_socket, (struct sockaddr*)&client_address, &client_address_len);

        std::cout << "Connected by " << inet_ntoa(client_address.sin_addr) << std::endl;

        while (true) {
            char buffer[1024] = {0};
            ssize_t bytesRead = recv(client_socket, buffer, sizeof(buffer) - 1, 0);

            if (bytesRead <= 0) {
                break;
            }

            buffer[bytesRead] = '\0';  // Null-terminate the received data

            // Parse the received message
            char* receivedMessage = buffer;

            std::istringstream iss(receivedMessage);
            std::string rest;
            std::vector<std::string> ip;

            while (std::getline(iss, rest, '\n')) {
                ip.push_back(rest);
            }

            // Handle the command
            std::string response = handle_message(ip);
            std::cout << response << std::endl;

            send(client_socket, response.c_str(), response.length(), 0);
            close(client_socket);
            shutdown(client_socket,SHUT_RDWR);

            if (response == "Connection closed\n") {
                close(client_socket);
                std::cout << "Connection closed by " << inet_ntoa(client_address.sin_addr) << std::endl;
                break;  // Break from the inner loop to accept a new connection
            }
        }
    }

    close(server_socket);

    return 0;
}
