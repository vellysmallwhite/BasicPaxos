#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <map>
#include <queue>
#include <thread>
#include <chrono>
#include <mutex>
#include <atomic>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <getopt.h>
#include <algorithm>
#include <errno.h>
#include <netdb.h> // Needed for gethostbyname


const int PORT = 8080;

enum MessageType { TOKEN, MARKER };

struct Message {
    MessageType type;
    int snapshotId;
};

class Process {
private:
    int id;
    std::atomic<int> state;
    int predecessor;
    int successor;
    std::map<int, int> incomingConnections;
    std::map<int, int> outgoingConnections;
    std::atomic<bool> hasToken;
    float tokenDelay;
    float markerDelay;
    std::vector<std::string> hosts;
    std::atomic<bool> running;
    std::thread listenThread;
    std::thread tokenThread;
    std::thread snapshotThread;
    std::mutex mutex;
    std::map<int, std::queue<Message>> snapshotQueues;
    std::map<int, bool> channelClosed;
    int currentSnapshotId;
    std::atomic<bool> snapshotInProgress;
    int snapshotTriggerState;
    std::string hostname;
    std::string curRecord[4];
    int serverSocket;  // Single server socket for incoming connections

public:
    Process(int id, const std::vector<std::string>& hosts, bool startWithToken,
            float tokenDelay, float markerDelay, int snapshotTriggerState,
            int snapshotId, const std::string& hostname)
        : id(id), state(startWithToken ? 1 : 0), hasToken(startWithToken),
          tokenDelay(tokenDelay), markerDelay(markerDelay),
          hosts(hosts), running(true), snapshotInProgress(false),
          snapshotTriggerState(snapshotTriggerState), currentSnapshotId(snapshotId),
          hostname(hostname) {
        int numProcesses = hosts.size();
        predecessor = mod(id - 2, numProcesses) + 1;
        successor = mod(id, numProcesses) + 1;
        for (int i = 1; i <= numProcesses; ++i) {
            
                 channelClosed[i] = true;
            
        }
        std::cerr << "{proc_id: " << id << ", state: " << state
                  << ", predecessor: " << predecessor << ", successor: " << successor << "}" << std::endl;
    }

    void run() {
        setupConnections();
        if (hasToken) {
            std::cerr << "{proc_id: " << id << ", state: " << state << "}" << std::endl;
        }
        listenThread = std::thread(&Process::listenForMessages, this);
        tokenThread = std::thread(&Process::tokenPassingTask, this);
        snapshotThread = std::thread(&Process::snapshotInitiationTask, this);

        tokenThread.join();
        snapshotThread.join();
        listenThread.join();
    }

private:
    int mod(int a, int b) {
        return (a % b + b) % b;
    }

    void tokenPassingTask() {
        while (running) {
            if (hasToken && !isOutgoingChannelEmpty()) {
                passToken();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10)); // Short sleep to prevent busy-waiting
        }
    }

    bool isOutgoingChannelEmpty() {
        std::lock_guard<std::mutex> lock(mutex);
        return outgoingConnections.find(successor) == outgoingConnections.end();
    }

    void setupConnections() {
        // Create a single server socket for incoming connections
        serverSocket = socket(AF_INET, SOCK_STREAM, 0);
        if (serverSocket == -1) {
            throw std::runtime_error("Failed to create server socket");
        }

        // Set server address and bind it to the process port (PORT + id)
        sockaddr_in serverAddr;
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_addr.s_addr = inet_addr("0.0.0.0");
        serverAddr.sin_port = htons(PORT + id);  // Unique port for each process

        if (bind(serverSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
            throw std::runtime_error("Failed to bind server socket to port " + std::to_string(PORT + id));
        }

        if (listen(serverSocket, 5) < 0) {
            throw std::runtime_error("Failed to listen on server socket");
        }

        std::cerr << hostname << " Server listening on port " << PORT + id << std::endl;

        // Start a thread to accept incoming connections
        std::thread acceptThread(&Process::acceptConnections, this);
        acceptThread.detach();

        // Establish outgoing connections to other processes
        std::thread connectThread(&Process::establishOutgoingConnections, this);
        connectThread.detach();
    }

    void acceptConnections() {
        while (running) {
            sockaddr_in clientAddr;
            socklen_t clientAddrLen = sizeof(clientAddr);
            int clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddr, &clientAddrLen);
            if (clientSocket < 0) {
                if (errno == EINTR) continue; // Interrupted by signal, retry
                std::cerr << "Failed to accept client connection" << std::endl;
                continue;
            }

            // Receive the client's process ID (senderId)
            int senderId;
            int bytesReceived = recv(clientSocket, &senderId, sizeof(senderId), MSG_WAITALL);
            if (bytesReceived <= 0) {
                close(clientSocket);
                continue;
            }

            // Store the incoming connection based on sender's ID
            {
                std::lock_guard<std::mutex> lock(mutex);
                incomingConnections[senderId] = clientSocket;
                channelClosed[senderId] = false;
            }

            std::cerr << id << ": Established incoming connection from process " << senderId << std::endl;
        }
    }

    void establishOutgoingConnections() {
        int numProcesses = hosts.size();
        while (running) {
            for (int i = 1; i <= numProcesses; ++i) {
                if (i != id) {  // Make sure not to connect to yourself
                    bool needToConnect = false;
                    int clientSocket = -1;

                    // Check if the connection exists
                    {
                        std::lock_guard<std::mutex> lock(mutex);
                        auto it = outgoingConnections.find(i);
                        if (it == outgoingConnections.end()) {
                            // If not found, we need to establish a new connection
                            needToConnect = true;
                        } else {
                            // Check if the existing connection is still valid
                            clientSocket = it->second;
                        }
                    }

                    if (needToConnect) {
                        // Establish a new connection outside of the mutex
                        establishOutgoingConnection(i);
                    } else if (clientSocket != -1) {
                        // Check if the existing connection is still valid
                        char buffer;
                        int result = recv(clientSocket, &buffer, 1, MSG_PEEK | MSG_DONTWAIT);

                        if (result == 0 || (result == -1 && errno != EAGAIN && errno != EWOULDBLOCK)) {
                            std::cerr << hostname << " Connection to process " << i << " is broken. Reconnecting..." << std::endl;
                            close(clientSocket);  // Close the broken socket
                            {
                                std::lock_guard<std::mutex> lock(mutex);
                                outgoingConnections.erase(i);  // Remove the broken connection
                            }
                            // Try to re-establish the connection
                            establishOutgoingConnection(i);
                        }
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));  // Retry every 1 second
        }
    }


    void establishOutgoingConnection(int targetId) {
    if (targetId == id) {
        std::cerr << "Skipping self-connection for process " << id << std::endl;
        return;
    }

    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (clientSocket == -1) {
        std::cerr << "Failed to create client socket" << std::endl;
        return;
    }

    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(PORT + targetId);  // Use targetId to connect to the correct port

    // Use the Docker container name (hostname) directly
    std::string targetHostname = hosts[targetId - 1];

    // Resolve the hostname (container name) using getaddrinfo
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));  // Zero out the hints struct
    hints.ai_family = AF_INET;         // IPv4
    hints.ai_socktype = SOCK_STREAM;   // TCP

    // Attempt to resolve the hostname
    int status = getaddrinfo(targetHostname.c_str(), nullptr, &hints, &res);
    if (status != 0) {
        //std::cerr << hostname << " Failed to resolve address for process " << targetId << ": " << gai_strerror(status) << std::endl;
        close(clientSocket);
        return;
    }

    // Use the first resolved address
    struct sockaddr_in* resolvedAddr = (struct sockaddr_in*)res->ai_addr;
    serverAddr.sin_addr = resolvedAddr->sin_addr;

    // Attempt to connect to the target process
    if (connect(clientSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        //std::cerr << "Failed to connect to process " << targetId 
                  //<< " on port " << PORT + targetId << ". Will retry later." << std::endl;
        close(clientSocket);
        freeaddrinfo(res);  // Free the result of getaddrinfo
        return;
    }

    // Send the current process ID to the target process
    send(clientSocket, &id, sizeof(id), 0);

    // Store the outgoing connection in the map
    {std::lock_guard<std::mutex> lock(mutex);
    outgoingConnections[targetId] = clientSocket;}

    std::cerr << hostname << " Established outgoing connection to process " 
              << targetHostname << " on port " << PORT + targetId << std::endl;

    // Free the result of getaddrinfo
    freeaddrinfo(res);
}

    void listenForMessages() {
        while (running) {
            fd_set readfds;
            FD_ZERO(&readfds);

            int max_sd = -1;

            // Add incoming sockets to the read set
            {
                std::lock_guard<std::mutex> lock(mutex);
                for (const auto& conn : incomingConnections) {
                    FD_SET(conn.second, &readfds);
                    if (conn.second > max_sd) max_sd = conn.second;
                }
            }

            if (max_sd == -1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }

            // Set a timeout for select()
            struct timeval timeout;
            timeout.tv_sec = 0;
            timeout.tv_usec = 100000; // 100ms timeout

            int activity = select(max_sd + 1, &readfds, NULL, NULL, &timeout);

            if (activity < 0) {
                if (errno == EINTR) continue;
                std::cerr << "Select error" << std::endl;
                continue;
            }

            // Handle incoming messages on established client connections
            {
                std::map<int, int> localIncomingConnections;
{
    std::lock_guard<std::mutex> lock(mutex);
    localIncomingConnections = incomingConnections;
}
                std::vector<int> disconnected;
                for (const auto& conn : localIncomingConnections) {
                    if (FD_ISSET(conn.second, &readfds)) {
                        Message msg;
                        int bytesRead = recv(conn.second, &msg, sizeof(msg), 0);
                        if (bytesRead <= 0) {
                            // Handle disconnection or error
                            close(conn.second);
                            disconnected.push_back(conn.first);
                            std::cerr << "Client " << conn.first << " disconnected" << std::endl;
                        } else {
                            handleMessage(msg, conn.first);  // Process the received message
                        }
                    }
                }
                // Remove disconnected sockets
                for (int pid : disconnected) {
                    std::lock_guard<std::mutex> lock(mutex);
                    incomingConnections.erase(pid);
                    channelClosed[pid]=true;
                }
            }
        }
    }

    void handleMessage(const Message& msg, int senderId) {
        std::lock_guard<std::mutex> lock(mutex);
        std::cerr << "{proc_id: " << id << ", received_message: " << (msg.type == TOKEN ? "token" : "marker")
                  << ", from: " << senderId << "}" << std::endl;

        // if (channelClosed[senderId]) {
        //     return;
        // }
        if (msg.type == TOKEN) {
            if (snapshotInProgress&&!channelClosed[senderId]) {
                snapshotQueues[senderId].push(msg);
            }
            hasToken = true;
            std::cerr << "{proc_id: " << id << ", state: " << state << "}" << std::endl;
            state++;
            if (state==snapshotTriggerState){
            handleMarker(-1, currentSnapshotId);
            }
            

        } else if (msg.type == MARKER) {
            handleMarker(senderId, msg.snapshotId);
        }
    }

    void passToken() {
        std::this_thread::sleep_for(std::chrono::duration<float>(tokenDelay));

        std::lock_guard<std::mutex> lock(mutex);
        if (outgoingConnections.find(successor) != outgoingConnections.end()) {
            Message tokenMsg = {TOKEN, 0};
            send(outgoingConnections[successor], &tokenMsg, sizeof(tokenMsg), 0);
            std::cerr << "{proc_id: " << id << ", state: " << state << "}" << std::endl;
            std::cerr << "{proc_id: " << id << ", sender: " << id << ", receiver: " << successor
                      << ", message:\"token\"}" << std::endl;
            hasToken = false;
        } else {
            // Cannot pass token because outgoing connection is not established yet
            std::cerr << "{proc_id: " << id << ", message:\"Cannot pass token, no outgoing connection to " << successor << "\"}" << std::endl;
        }
    }

    void handleMarker(int senderId, int snapshotId) {
        if (!snapshotInProgress) {
            
            snapshotInProgress = true;
            currentSnapshotId = snapshotId;
            recordLocalState();
            std::cerr << "{proc_id:" << id << ", snapshot_id: " << snapshotId << ", snapshot:\"started\"}" << std::endl;
            
            sendMarkers();

        }
        if (senderId==-1){
            return;
        }

        channelClosed[senderId] = true;
        printClosedChannel(senderId);

        if (allChannelsClosed()) {
            completeSnapshot();
        }
    }

    void recordLocalState() {
        curRecord[0] = std::to_string(id);  // proc_id
    curRecord[1] = std::to_string(state);  // state
    curRecord[2] = std::to_string(currentSnapshotId);  // snapshot_id
    curRecord[3] = (hasToken ? "YES" : "NO"); 
    }
    void sendMarkers() {
        std::thread([this]() {
            std::this_thread::sleep_for(std::chrono::duration<float>(markerDelay));

            std::lock_guard<std::mutex> lock(mutex);
            for (const auto& conn : outgoingConnections) {
                if (conn.first != id) {
                    Message markerMsg = {MARKER, currentSnapshotId};
                    send(conn.second, &markerMsg, sizeof(markerMsg), 0);
                    std::cerr << "{proc_id:" << curRecord[0] << ", snapshot_id:" << curRecord[2]
              << ", sender:" << curRecord[0] << ", receiver:" << conn.first << ", msg:\"marker\", "
              << "state:" << curRecord[1] << ", has_token:" << curRecord[3]<< "}" << std::endl;
                }
            }
        }).detach();
    }

    void printClosedChannel(int senderId) {
        std::cerr << "{proc_id:" << id << ", snapshot_id: " << curRecord[2]
                  << ", snapshot:\"channel closed\", channel:" << senderId << "-" << id
                  << ", queue:[";
        bool first = true;
        while (!snapshotQueues[senderId].empty()) {
            if (!first) std::cerr << ",";
            std::cerr << (snapshotQueues[senderId].front().type == TOKEN ? "token" : "marker");
            snapshotQueues[senderId].pop();
            first = false;
        }
        std::cerr << "]}" << std::endl;
    }

    bool allChannelsClosed() {
        for (const auto& closed : channelClosed) {
            if (!closed.second) return false;
        }
        return true;
    }

    void completeSnapshot() {
        std::cerr << "{proc_id:" << id << ", snapshot_id: " << curRecord[2] << ", snapshot:\"complete\"}" << std::endl;
        snapshotInProgress = false;
        for (auto& closed : channelClosed) {
             if (closed.first != id) {  // Don't reset the channel for the process itself
        closed.second = false;
    }
           
        }
        snapshotQueues.clear();
    }

    void snapshotInitiationTask() {
        while (running) {
            if (state == snapshotTriggerState && !snapshotInProgress) {
                initiateSnapshot();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }

    void initiateSnapshot() {
        std::lock_guard<std::mutex> lock(mutex);
        recordLocalState();
        snapshotInProgress = true;
        currentSnapshotId = state;
        sendMarkers();
        std::cerr << "{proc_id:" << id << ", snapshot_id: " << curRecord[2] << ", snapshot:\"started\"}" << std::endl;
        //recordLocalState();
        
    }
};

int main(int argc, char* argv[]) {
    std::string hostfile;
    std::string hostname;
    std::string network;
    float tokenDelay = 0.0;
    float markerDelay = 0.0;
    int snapshotTriggerState = -1;
    int snapshotId = -1;
    bool startWithToken = false;

    static struct option long_options[] = {
        {"name", required_argument, 0, 'n'},
        {"network", required_argument, 0, 'w'},
        {"hostname", required_argument, 0, 'o'},
        {0, 0, 0, 0}
    };

    int opt;
    int option_index = 0;
    while ((opt = getopt_long(argc, argv, "h:t:m:s:p:xn:w:o:", long_options, &option_index)) != -1) {
        switch (opt) {
            case 'h':
                hostfile = optarg;
                break;
            case 't':
                tokenDelay = std::stof(optarg);
                break;
            case 'm':
                markerDelay = std::stof(optarg);
                break;
            case 's':
                snapshotTriggerState = std::stoi(optarg);
                break;
            case 'p':
                snapshotId = std::stoi(optarg);
                break;
            case 'x':
                startWithToken = true;
                break;
            case 'n':
            case 'o':
                hostname = optarg;
                break;
            case 'w':
                network = optarg;
                break;
            default:
                std::cerr << "Usage: " << argv[0] << " --name <hostname> --network <network> --hostname <hostname> -h <hostfile> -t <token_delay> -m <marker_delay> [-s <snapshot_delay> -p <snapshot_id>] [-x]" << std::endl;
                return 1;
        }
    }

    if (hostname.empty()) {
        char* envHostname = getenv("HOSTNAME");
        if (envHostname == nullptr) {
            std::cerr << "HOSTNAME environment variable not set" << std::endl;
            return 1;
        }
        hostname = envHostname;
    }

    std::ifstream file(hostfile);
    std::vector<std::string> hosts;
    std::string host;
    while (std::getline(file, host)) {
        hosts.push_back(host);
    }

    int id = std::distance(hosts.begin(), std::find(hosts.begin(), hosts.end(), hostname)) + 1;

    if (id > hosts.size()) {
        std::cerr << "Hostname not found in hostfile" << std::endl;
        return 1;
    }

    Process process(id, hosts, startWithToken, tokenDelay, markerDelay, snapshotTriggerState, snapshotId, hostname);
    process.run();

    return 0;
}
