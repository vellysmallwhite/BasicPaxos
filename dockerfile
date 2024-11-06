# Use the latest gcc image as the base
FROM gcc:latest

# Install JsonCpp library
RUN apt-get update && apt-get install -y libjsoncpp-dev && ls /usr/include/jsoncpp


# Set the working directory in the container
WORKDIR /Paxos

# Copy the current directory contents into the container at /ChamdiLamport-1
COPY . .

# Compile the C++ program with JsonCpp
RUN g++ -std=c++11 -o paxos main.cpp -pthread -ljsoncpp -I/usr/include/jsoncpp

# Set the entry point to your application (adjust if needed)
ENTRYPOINT ["./paxos"]

# Use CMD to provide default arguments (can be modified or left empty)
CMD []
