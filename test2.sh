#!/bin/bash
docker compose -f docker-compose-testcase-2.yml down
# Step 1: Rebuild the Docker image
docker build . -t prj4
docker compose -f docker-compose-testcase-2.yml up