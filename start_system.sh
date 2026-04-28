#!/bin/bash

echo "========================================"
echo "Clean Room EMS - System Startup"
echo "========================================"
echo ""

# Function to start service in background
start_service() {
    gnome-terminal -- bash -c "$1; exec bash" 2>/dev/null || \
    xterm -e "$1" 2>/dev/null || \
    osascript -e "tell app \"Terminal\" to do script \"$1\"" 2>/dev/null || \
    ($1 &)
}

echo "[1/3] Starting Main Server..."
start_service "python3 main.py"

sleep 3

echo "[2/3] Starting CSV Data Streamer..."
start_service "python3 csv_data_streamer.py"

sleep 2

echo "[3/3] Starting Cloud Backup Service..."
start_service "python3 cloud_backup.py"

echo ""
echo "========================================"
echo "System Started!"
echo "========================================"
echo ""
echo "Services running:"
echo "- Main Server: http://localhost:8000/"
echo "- Dashboard: http://localhost:8000/"
echo "- DDU: http://localhost:8000/ddu?room=Room-1"
echo ""

