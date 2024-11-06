#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <map>
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
#include <netdb.h>
#include <json/json.h>
#include <sstream>
#include <semaphore.h>
#include <fcntl.h>
#include <unistd.h>

const int PORT = 8080;

const char* SEM_NAME = "/console_semaphore"; 
enum Role {
    PROPOSER,
    ACCEPTOR,
    LEARNER
};

enum MessageType {
    PREPARE,
    PROMISE,
    ACCEPT,
    ACCEPTED,
    CHOSEN,
    NACK
};

struct PeerConfig {
    std::string hostname;
    std::vector<std::string> roles;
};

class PaxosProcess {
private:
    int id;
    std::string hostname;
    std::vector<std::string> roles;
    int proposalNum;
    int highestProposalNumSeen;
    int acceptedProposalNum;  // vrnd
    char acceptedValue;       // vval
    int lastPromisedProposalNum;  // last_rnd
    int delay;
    std::vector<std::string> peers;
    std::map<int, PeerConfig> peerConfigs;
    std::vector<int> myAcceptors;
    std::vector<int> myLearners;
    std::map<int, int> incomingConnections;
    std::map<int, int> outgoingConnections;
    std::atomic<bool> running;
    std::thread messageThread;
    std::mutex mtx;
    int serverSocket;
    std::map<int, int> prepareResponses;
    std::map<int, int> acceptResponses;
    std::map<int, bool> channelClosed;
    std::map<int, char> promisedValues;  // Keep track of accepted values from promises
    char valueToPropose;
    sem_t* consoleSemaphore; // Pointer to named semaphore
    std::ofstream logFile; 
public:
    PaxosProcess(const std::string& configFile, int delay, const std::string& hostname)
        : hostname(hostname), delay(delay), running(true), proposalNum(0),
          highestProposalNumSeen(0), acceptedProposalNum(0), acceptedValue('\0'),
          lastPromisedProposalNum(0), valueToPropose('\0') {
        readConfigFile(configFile);
        consoleSemaphore = sem_open(SEM_NAME, O_CREAT, 0644, 1);
        if (consoleSemaphore == SEM_FAILED) {
            perror("sem_open");
            exit(EXIT_FAILURE);
        }

        
    }
      ~PaxosProcess() {
        // Close the semaphore and unlink if needed
        sem_close(consoleSemaphore);
        sem_unlink(SEM_NAME);
    }

    void run(char value) {
        valueToPropose = value;
        setupConnections();
        waitForConnections();
        if (isProposer()) {
            if (delay > 0) {
                std::this_thread::sleep_for(std::chrono::seconds(delay));
            }
            initiateProposal();
        }
        messageThread = std::thread(&PaxosProcess::listenForMessages, this);
        messageThread.join();
    }

private:

    void safePrintJson(const Json::Value& json) {
        // Convert JSON to a single-line string
        std::ostringstream oss;
    oss << "{\"peer_id\":" << id
        << ", \"action\":\"" << json["action"].asString()
        << "\", \"message_type\":\"" << json["message_type"].asString()
        << "\", \"message_value\":\"" << json["message_value"].asString()
        << "\", \"proposal_num\":" << json["proposal_num"].asInt() << "}";

    // Output the formatted JSON string
    std::string output = oss.str() + "\n";  // Add newline for clarity between messages
    write(STDOUT_FILENO, output.c_str(), output.size());
    }


    bool isProposer() {
        for (const auto& role : roles) {
            if (role.find("proposer") != std::string::npos) {
                return true;
            }
        }
        return false;
    }

    bool isAcceptor() {
        for (const auto& role : roles) {
            if (role.find("acceptor") != std::string::npos) {
                return true;
            }
        }
        return false;
    }

    bool isLearner() {
        for (const auto& role : roles) {
            if (role.find("learner") != std::string::npos) {
                return true;
            }
        }
        return false;
    }

    void waitForConnections() {
        bool allConnected = false;
        std::this_thread::sleep_for(std::chrono::seconds(1));
        while (!allConnected && running) {
            std::lock_guard<std::mutex> lock(mtx);
            allConnected = true;

            if (isProposer()) {
                for (int acceptorId : myAcceptors) {
                    if (outgoingConnections.find(acceptorId) == outgoingConnections.end()) {
                        allConnected = false;
                        break;
                    }
                }
            }

            if (!allConnected) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));

        //std::cerr << hostname << " All necessary connections established" << std::endl;
    }

    void readConfigFile(const std::string& configFile) {
        std::ifstream file(configFile);
        if (!file.is_open()) {
            throw std::runtime_error("Unable to open config file: " + configFile);
        }

        std::string line;
        int lineNum = 0;
        while (std::getline(file, line)) {
            lineNum++;
            if (line.empty() || line[0] == '#') continue;

            size_t colonPos = line.find(':');
            if (colonPos == std::string::npos) {
                throw std::runtime_error("Invalid format in config file at line " + std::to_string(lineNum));
            }
            std::string host = line.substr(0, colonPos);
            std::string rolesStr = line.substr(colonPos + 1);

            PeerConfig config;
            config.hostname = host;

            std::istringstream rolesStream(rolesStr);
            std::string roleToken;
            while (std::getline(rolesStream, roleToken, ',')) {
                config.roles.push_back(roleToken);
            }

            int peerId = peerConfigs.size() + 1;
            peerConfigs[peerId] = config;
            peers.push_back(host);

            if (config.hostname == hostname) {
                id = peerId;
                roles = config.roles;
            }
        }

        if (id == 0) {
            throw std::runtime_error("This host (" + hostname + ") not found in config file");
        }

        // For proposers, set myAcceptors based on acceptor roles
        for (const auto& role : roles) {
            if (role.find("proposer") != std::string::npos) {
                std::string proposerRole = role;
                // Find acceptors assigned to this proposer
                for (const auto& peerPair : peerConfigs) {
                    int peerId = peerPair.first;
                    const PeerConfig& config = peerPair.second;
                    if (peerId == id) continue;
                    if (std::find(config.roles.begin(), config.roles.end(), "acceptor" + proposerRole.substr(8)) != config.roles.end()) {
                        myAcceptors.push_back(peerId);
                    }
                }
            }
        }

        // For learners
        for (const auto& peerPair : peerConfigs) {
            int peerId = peerPair.first;
            const PeerConfig& config = peerPair.second;
            if (peerId == id) continue;
            if (std::find(config.roles.begin(), config.roles.end(), "learner") != config.roles.end()) {
                myLearners.push_back(peerId);
            }
        }

        std::cerr << "Configuration loaded for process " << id << " with roles: ";
        for (const auto& role : roles) {
            std::cerr << role << " ";
        }
        std::cerr << std::endl;
    }

    void setupConnections() {
        serverSocket = socket(AF_INET, SOCK_STREAM, 0);
        if (serverSocket == -1) {
            throw std::runtime_error("Server socket creation failed");
        }

        int opt = 1;
        if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            throw std::runtime_error("setsockopt failed");
        }

        sockaddr_in serverAddr;
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_addr.s_addr = INADDR_ANY;
        serverAddr.sin_port = htons(PORT + id);

        if (bind(serverSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
            throw std::runtime_error("Bind failed");
        }

        if (listen(serverSocket, 10) < 0) {
            throw std::runtime_error("Listen failed");
        }

        std::thread(&PaxosProcess::acceptIncomingConnections, this).detach();
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::thread(&PaxosProcess::establishOutgoingConnections, this).detach();
    }

    void acceptIncomingConnections() {
        while (running) {
            sockaddr_in clientAddr;
            socklen_t addrLen = sizeof(clientAddr);
            int clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddr, &addrLen);

            if (clientSocket < 0) {
                if (errno != EINTR) {
                    std::cerr << "Accept failed: " << strerror(errno) << std::endl;
                }
                continue;
            }

            int senderId;
            if (recv(clientSocket, &senderId, sizeof(senderId), MSG_WAITALL) <= 0) {
                close(clientSocket);
                continue;
            }

            {
                std::lock_guard<std::mutex> lock(mtx);
                if (incomingConnections.find(senderId) != incomingConnections.end()) {
                    close(incomingConnections[senderId]);
                }
                incomingConnections[senderId] = clientSocket;
                channelClosed[senderId] = false;
            }
        }
    }

    void establishOutgoingConnections() {
        int numProcesses = peers.size();
        while (running) {
            for (int i = 1; i <= numProcesses; ++i) {
                if (i != id) {
                    bool needToConnect = false;
                    int clientSocket = -1;

                    {
                        std::lock_guard<std::mutex> lock(mtx);
                        auto it = outgoingConnections.find(i);
                        if (it == outgoingConnections.end()) {
                            needToConnect = true;
                        } else {
                            clientSocket = it->second;
                        }
                    }

                    if (needToConnect) {
                        establishOutgoingConnection(i);
                    } else if (clientSocket != -1) {
                        char buffer;
                        int result = recv(clientSocket, &buffer, 1, MSG_PEEK | MSG_DONTWAIT);

                        if (result == 0 || (result == -1 && errno != EAGAIN && errno != EWOULDBLOCK)) {
                            std::cerr << hostname << " Connection to process " << i << " is broken. Reconnecting..." << std::endl;
                            close(clientSocket);
                            {
                                std::lock_guard<std::mutex> lock(mtx);
                                outgoingConnections.erase(i);
                            }
                            establishOutgoingConnection(i);
                        }
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
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
        serverAddr.sin_port = htons(PORT + targetId);

        std::string targetHostname = peers[targetId - 1];

        struct addrinfo hints, *res;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;

        int status = getaddrinfo(targetHostname.c_str(), nullptr, &hints, &res);
        if (status != 0) {
            close(clientSocket);
            return;
        }

        struct sockaddr_in* resolvedAddr = (struct sockaddr_in*)res->ai_addr;
        serverAddr.sin_addr = resolvedAddr->sin_addr;

        if (connect(clientSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
            close(clientSocket);
            freeaddrinfo(res);
            return;
        }

        send(clientSocket, &id, sizeof(id), 0);

        {
            std::lock_guard<std::mutex> lock(mtx);
            outgoingConnections[targetId] = clientSocket;
        }

        std::cerr << hostname << " Established outgoing connection to process "
                  << targetHostname << " on port " << PORT + targetId << std::endl;

        freeaddrinfo(res);
    }

    void initiateProposal() {
        proposalNum = generateProposalNumber();
        highestProposalNumSeen = proposalNum;
        sendPrepare();
    }

    int generateProposalNumber() {
        // Generate a unique proposal number
        static int uniqueId = id * 1000;  // Ensure uniqueness across processes
        return ++uniqueId;
    }

    void sendPrepare() {
        Json::Value msg(Json::objectValue); 
        msg["peer_id"] = id;
        msg["action"] = "sent";
        msg["message_type"] = "prepare";
        msg["message_value"] = std::string(1, valueToPropose);
        msg["proposal_num"] = proposalNum;
        broadcastToAcceptors(msg);
    }

    void broadcastToAcceptors(const Json::Value& msg) {
        std::string msgStr = msg.toStyledString();
        //safePrintJson(msgStr);
        //std::cout << msgStr << std::endl;
        safePrintJson(msg);

        std::lock_guard<std::mutex> lock(mtx);

        for (int acceptorId : myAcceptors) {
            if (outgoingConnections.find(acceptorId) != outgoingConnections.end()) {
                send(outgoingConnections[acceptorId], msgStr.c_str(), msgStr.size(), 0);
            }
        }
    }

    void sendMessage(const Json::Value& msg, int targetId) {
        std::string msgStr = msg.toStyledString();

        //std::cout << msgStr << std::endl;
        safePrintJson(msg);

        std::lock_guard<std::mutex> lock(mtx);
        if (outgoingConnections.find(targetId) != outgoingConnections.end()) {
            send(outgoingConnections[targetId], msgStr.c_str(), msgStr.size(), 0);
        }
    }

    void sendToAll(const Json::Value& msg) {
        std::string msgStr = msg.toStyledString();

        //std::cout << msgStr << std::endl;
        safePrintJson(msg);

        std::lock_guard<std::mutex> lock(mtx);
        for (const auto& conn : outgoingConnections) {
            send(conn.second, msgStr.c_str(), msgStr.size(), 0);
        }
    }

    void listenForMessages() {
        while (running) {
            fd_set readfds;
            FD_ZERO(&readfds);
            int max_sd = -1;

            {
                std::lock_guard<std::mutex> lock(mtx);
                for (const auto& conn : incomingConnections) {
                    FD_SET(conn.second, &readfds);
                    max_sd = std::max(max_sd, conn.second);
                }
            }

            if (max_sd == -1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }

            timeval timeout = {1, 0};
            int activity = select(max_sd + 1, &readfds, nullptr, nullptr, &timeout);

            if (activity < 0 && errno != EINTR) {
                std::cerr << "Select error" << std::endl;
                continue;
            }

            std::map<int, int> currentConnections;
            {
                std::lock_guard<std::mutex> lock(mtx);
                currentConnections = incomingConnections;
            }

            for (const auto& conn : currentConnections) {
    if (FD_ISSET(conn.second, &readfds)) {
        char buffer[1024] = {0};
        int bytesRead = read(conn.second, buffer, 1024);

        if (bytesRead > 0) {
            Json::Reader reader;
            Json::Value receivedMsg;
            if (reader.parse(buffer, receivedMsg)) {
                // Print received message
                receivedMsg["action"] = "received";
                //std::cout << receivedMsg.toStyledString() << std::endl;
                safePrintJson(receivedMsg);

                int senderId = receivedMsg["peer_id"].asInt();
                MessageType msgType = parseMessageType(
                    receivedMsg["message_type"].asString());
                int propNum = receivedMsg["proposal_num"].asInt();
                char value = receivedMsg["message_value"].asString()[0];
                handleMessage(senderId, msgType, propNum, value);
            }
        }
    }
}
        }
    }

    MessageType parseMessageType(const std::string& type) {
        if (type == "prepare") return PREPARE;
        if (type == "prepare_ack") return PROMISE;
        if (type == "accept") return ACCEPT;
        if (type == "accept_ack") return ACCEPTED;
        if (type == "chosen") return CHOSEN;
        if (type == "nack") return NACK;
        throw std::invalid_argument("Unknown message type: " + type);
    }

    void handleMessage(int senderId, MessageType msgType, int propNum, char value) {
        switch (msgType) {
            case PREPARE:
                handlePrepare(senderId, propNum);
                break;
            case PROMISE:
                handlePromise(senderId, propNum, value);
                break;
            case ACCEPT:
                handleAcceptRequest(senderId, propNum, value);
                break;
            case ACCEPTED:
                handleAccepted(senderId, propNum, value);
                break;
            case NACK:
                handleNack(senderId, propNum);
                break;
            case CHOSEN:
                handleChosen(senderId, propNum, value);
                break;
        }
    }

    void handlePrepare(int senderId, int propNum) {
        if (isAcceptor()) {
            if (propNum < lastPromisedProposalNum) {
                sendNack(senderId, lastPromisedProposalNum);
                return;
            }

            lastPromisedProposalNum = propNum;

            Json::Value promiseMsg(Json::objectValue); 
            promiseMsg["peer_id"] = id;
            promiseMsg["action"] = "sent";
            promiseMsg["message_type"] = "prepare_ack";
            promiseMsg["message_value"] = std::string(1, acceptedValue ? acceptedValue : ' ');
            promiseMsg["proposal_num"] = propNum;
            promiseMsg["accepted_proposal_num"] = acceptedProposalNum;

            sendMessage(promiseMsg, senderId);
        }
    }

    void sendNack(int senderId, int propNum) {
        Json::Value nackMsg;
        nackMsg["peer_id"] = id;
        nackMsg["action"] = "sent";
        nackMsg["message_type"] = "nack";
        nackMsg["message_value"] = " ";
        nackMsg["proposal_num"] = lastPromisedProposalNum;

        sendMessage(nackMsg, senderId);
    }

    void handlePromise(int senderId, int propNum, char value) {
    if (!isProposer()) return;

    if (propNum != proposalNum) return;

    bool quorumReached = false;
    char valueToSend = valueToPropose;
    int highestAcceptedProposalNum = 0;

    {
        std::lock_guard<std::mutex> lock(mtx);
        prepareResponses[senderId] = propNum;
        if (value != ' ') {
            promisedValues[senderId] = value;
        }

        // Check if quorum has been reached
        if (prepareResponses.size() ==myAcceptors.size()/2) {
            quorumReached = true;

            // Determine the value to send based on highest accepted proposal number
            for (const auto& pair : promisedValues) {
                acceptedProposalNum = prepareResponses[pair.first];

                if (acceptedProposalNum > highestAcceptedProposalNum) {
                    highestAcceptedProposalNum = acceptedProposalNum+1;
                    valueToSend = pair.second;
                }
            }

            // Clear response tracking after quorum is reached
            prepareResponses.clear();
            promisedValues.clear();
        }
    }

    // Send the accept request outside of the lock to avoid deadlock
    if (quorumReached) {
        sendAcceptRequest(proposalNum, valueToSend);
    }
}


    void handleNack(int senderId, int propNum) {
        if (!isProposer()) return;

        if (propNum != proposalNum) return;

        // Start new proposal with higher proposal number
        proposalNum = generateProposalNumber();
        highestProposalNumSeen = proposalNum;
        sendPrepare();
    }

    void sendAcceptRequest(int propNum, char value) {
        Json::Value msg(Json::objectValue);
        
        msg["peer_id"] = id;
        msg["action"] = "sent";
        msg["message_type"] = "accept";
        msg["message_value"] = std::string(1, value);
        msg["proposal_num"] = propNum;
        broadcastToAcceptors(msg);
    }

    void handleAcceptRequest(int senderId, int propNum, char value) {
        if (isAcceptor()||isLearner()) {
            if (propNum < lastPromisedProposalNum) {
                sendNack(senderId, lastPromisedProposalNum);
                return;
            }

            lastPromisedProposalNum = propNum;
            acceptedProposalNum = propNum;
            acceptedValue = value;

            Json::Value acceptedMsg;
            acceptedMsg["peer_id"] = id;
            acceptedMsg["action"] = "sent";
            acceptedMsg["message_type"] = "accept_ack";
            acceptedMsg["message_value"] = std::string(1, value);
            acceptedMsg["proposal_num"] = propNum;
            //handleChosen(id, propNum,  acceptedValue);

            sendMessage(acceptedMsg, senderId);

            // Notify learners
            
        }
    }

    void handleAccepted(int senderId, int propNum, char value) {
        if (!isProposer()) return;

        if (propNum != proposalNum) {proposalNum=propNum+1; return;}

        std::lock_guard<std::mutex> lock(mtx);
        acceptResponses[senderId] = propNum;

        if (acceptResponses.size() > myAcceptors.size() / 2) {
            // Majority accepted
            Json::Value chosenMsg;
            chosenMsg["peer_id"] = id;
            chosenMsg["action"] = "chosen";
            chosenMsg["message_type"] = "chosen";
            chosenMsg["message_value"] = std::string(1, value);
            chosenMsg["proposal_num"] = propNum;
            safePrintJson(chosenMsg);
            notifyLearners(propNum, value);
            //sendToAll(chosenMsg);
            acceptResponses.clear();
        }
    }

    void handleChosen(int senderId, int propNum, char value) {
        if (isLearner() || isAcceptor() || isProposer()) {
            Json::Value chosenMsg;
            chosenMsg["peer_id"] = id;
            chosenMsg["action"] = "chosen";
            chosenMsg["message_type"] = "chosen";
            chosenMsg["message_value"] = std::string(1, value);
            chosenMsg["proposal_num"] = propNum;
            safePrintJson(chosenMsg);

            //std::cout << chosenMsg.toStyledString() << std::endl;

            //running = false;
        }
    }

    void notifyLearners(int propNum, char value) {
        Json::Value notifyMsg(Json::objectValue);
        notifyMsg["peer_id"] = id;
        notifyMsg["action"] = "sent";
        notifyMsg["message_type"] = "accept";
        notifyMsg["message_value"] = std::string(1, value);
        notifyMsg["proposal_num"] = propNum;

        for (int learnerId : myLearners) {
            sendMessage(notifyMsg, learnerId);
        }
    }
};

int main(int argc, char* argv[]) {
    std::string configFile;
    std::string hostname;
    int delay = 0;
    char value = ' ';

    int opt;
    while ((opt = getopt(argc, argv, "h:v:t:")) != -1) {
        switch (opt) {
            case 'h':
                configFile = optarg;
                break;
            case 'v':
                value = optarg[0];
                break;
            case 't':
                delay = atoi(optarg);
                break;
            default:
                std::cerr << "Usage: " << argv[0] << " -h hostsfile [-v value] [-t delay]" << std::endl;
                exit(EXIT_FAILURE);
        }
    }

    if (configFile.empty()) {
        std::cerr << "Hosts file must be specified with -h" << std::endl;
        exit(EXIT_FAILURE);
    }

    // Get hostname
    char hostbuffer[256];
    if (gethostname(hostbuffer, sizeof(hostbuffer)) == -1) {
        perror("gethostname");
        exit(EXIT_FAILURE);
    }
    hostname = hostbuffer;

    PaxosProcess paxosProcess(configFile, delay, hostname);
    paxosProcess.run(value);

    return 0;
}
