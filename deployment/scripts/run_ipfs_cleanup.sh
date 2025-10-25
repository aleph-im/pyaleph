#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Initial Setup & User Input ---
echo "This script will clean unpinned IPFS objects and reclaim disk space."
echo "--------------------------------------------------------------------"
read -p "Enter your DATABASE_USERNAME (default: aleph): " DB_USER
DB_USER=${DB_USER:-aleph}

read -s -p "Enter your DATABASE_PASSWORD: " DB_PASS
echo
if [ -z "$DB_PASS" ]; then
    echo "❌ Error: Database password cannot be empty."
    exit 1
fi

# --- 1. Measure Initial Space Usage ---
echo -e "\n📊 Checking initial disk space usage..."
IPFS_CONTAINER=$(docker ps -a --format "{{.Names}}" | grep ipfs | head -n 1)
if [ -z "$IPFS_CONTAINER" ]; then
    echo "❌ Error: Could not find the IPFS container."
    exit 1
fi

# Find the volume name mounted to the IPFS container
IPFS_VOLUME=$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/data/ipfs"}}{{.Name}}{{end}}{{end}}' "$IPFS_CONTAINER")
if [ -z "$IPFS_VOLUME" ]; then
    echo "❌ Error: Could not find the IPFS data volume for container '$IPFS_CONTAINER'."
    exit 1
fi

# Use 'docker system df -v' to find the volume's reported size
# We grep for the volume name and get the last column containing the size
INITIAL_SIZE_HR=$(docker system df -v | grep -A 9999 "VOLUME" | grep -w "$IPFS_VOLUME" | awk '{print $NF}')

if [ -z "$INITIAL_SIZE_HR" ]; then
    echo "   - ⚠️  Warning: Could not determine initial size from 'docker system df'."
    INITIAL_SIZE_HR="N/A"
fi
echo "   - IPFS Volume: $IPFS_VOLUME"
echo "   - Initial Size (from docker df): $INITIAL_SIZE_HR"

# --- 2. Download Files ---
echo -e "\n⬇️  Downloading Dockerfile and cleaner script..."
wget -q --show-progress -O cleaner.dockerfile "https://raw.githubusercontent.com/aleph-im/pyaleph/refs/heads/andres-feature-implement_experimental_ipfs_pin_cleaner/deployment/docker-build/cleaner.dockerfile"
wget -q --show-progress -O ipfs_pin_cleaner.py "https://raw.githubusercontent.com/aleph-im/pyaleph/refs/heads/andres-feature-implement_experimental_ipfs_pin_cleaner/deployment/scripts/ipfs_pin_cleaner.py"

# --- 3. Build Docker Image ---
echo -e "\n🛠️  Building 'ipfs-pin-cleaner' Docker image..."
docker build -f cleaner.dockerfile -t ipfs-pin-cleaner . > /dev/null
echo "   - Image built successfully."

# --- 4. Stop Containers ---
echo -e "\n🛑 Stopping non-essential containers..."
docker-compose stop pyaleph pyaleph-api p2p-service rabbitmq redis

# --- 5. Get Network and IPFS Info ---
echo -e "\n🔎 Identifying network and IPFS container details..."
PYALEPH_NETWORK=$(docker network list --format "{{.Name}}" | grep pyaleph | head -n 1)
IPFS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$IPFS_CONTAINER")
echo "   - Network: $PYALEPH_NETWORK, IPFS IP: $IPFS_IP"

# --- 6. Run IPFS Pin Cleaner ---
echo -e "\n🧹 Running the IPFS pin cleaner (this may take a while)..."
docker run --rm --network "$PYALEPH_NETWORK" \
  -e DATABASE_DSN="postgres://${DB_USER}:${DB_PASS}@postgres:5432/aleph" \
  -e IPFS_API="/ip4/${IPFS_IP}/tcp/5001" \
  ipfs-pin-cleaner --unpin

# --- 7. Execute IPFS Garbage Collector ---
echo -e "\n🗑️  Executing IPFS garbage collector..."
docker exec -it "$IPFS_CONTAINER" ipfs repo gc

# --- 8. Measure Final Space ---
echo -e "\n📊 Checking final disk space usage..."
# A small sleep can give Docker's daemon time to update its disk usage stats
sleep 5
FINAL_SIZE_HR=$(docker system df -v | grep -A 9999 "VOLUME" | grep -w "$IPFS_VOLUME" | awk '{print $NF}')

if [ -z "$FINAL_SIZE_HR" ]; then
    echo "   - ⚠️  Warning: Could not determine final size from 'docker system df'."
    FINAL_SIZE_HR="N/A"
fi
echo "   - Final Size (from docker df): $FINAL_SIZE_HR"

# --- 9. Restart All Containers ---
echo -e "\n🚀 Starting all services..."
docker-compose up -d

# --- 10. Cleanup ---
echo -e "\n✨ Cleaning up temporary files..."
rm cleaner.dockerfile ipfs_pin_cleaner.py

# --- Final Summary ---
echo -e "\n------------------- SUMMARY -------------------"
echo -e "Initial size reported:   \033[1;31m$INITIAL_SIZE_HR\033[0m"
echo -e "Final size reported:     \033[1;32m$FINAL_SIZE_HR\033[0m"
echo -e "\nℹ️  Compare the values above to see the reclaimed space."
echo "-----------------------------------------------"
echo "✅ All tasks finished successfully!"
