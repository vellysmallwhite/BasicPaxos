# Use the latest gcc image as the base
FROM gcc:latest

# Set the working directory in the container
WORKDIR /ChamdiLamport

# Copy the current directory contents into the container at /app
COPY . .

# Compile the C++ program
RUN g++ -std=c++11 -o passtoken main.cpp -pthread

# Set the entry point to your application
ENTRYPOINT ["./passtoken"]

# Use CMD to provide default arguments
CMD []