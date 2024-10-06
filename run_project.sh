#!/bin/bash

# Step 1: Rebuild the Docker image
docker build . -t prj2

# Step 2: Stop and remove all existing containers
docker stop $(docker ps -aq)
docker rm $(docker ps -aq)

# Step 3: Run the new containers
docker run --name container1 --network prj2_network --hostname container1 prj2 -h hostfile.txt -t 0.2 -m 2 -s 2 -p 1 -x &
docker run --name container2 --network prj2_network --hostname container2 prj2 -h hostfile.txt -t 0.2 -m 2 &
docker run --name container3 --network prj2_network --hostname container3 prj2 -h hostfile.txt -t 0.2 -m 2 -s 30&
docker run --name container4 --network prj2_network --hostname container4 prj2 -h hostfile.txt -t 0.2 -m 2 &
docker run --name container5 --network prj2_network --hostname container5 prj2 -h hostfile.txt -t 0.2 -m 2 &

# Wait for containers to start
sleep 5

# Display logs from both containers
docker logs container1 &
docker logs container2