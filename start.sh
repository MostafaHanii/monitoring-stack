#!/bin/bash

# Function to launch an exporter in the background
function launch_exporter() {
    local name=$1
    local command=$2
    echo "Starting $name..."
    # Run the command in the background
    $command &
    # Store the Process ID (PID) of the background process
    PIDS+=($!)
}

# --- Launch All 5 Exporters ---
# Replace 'python /app/exporterX.py' with the actual command for each of your 5 exporters.
launch_exporter "BTC Exporter" "python /app/btc_exporter.py --port 9890"
launch_exporter "ETH Exporter" "python /app/eth_exporter.py --port 9894"
launch_exporter "BNB Exporter" "python /app/bnb_exporter.py --port 9892"
launch_exporter "SOL Exporter" "python /app/sol_exporter.py --port 9896"
launch_exporter "XRP Exporter" "python /app/xrp_exporter.py --port 9898"

# --- Main Waiting Loop ---
# This loop is crucial: it waits until one of the background processes exits.
# When one process dies, the script exits, which stops the container.
# This prevents a 'silent' failure where one exporter dies but the container keeps running.
echo "All exporters started. Waiting for processes..."
wait -n

# Capture the exit status of the process that terminated
EXIT_CODE=$?

# Stop all other running processes gracefully
echo "One exporter exited (Status $EXIT_CODE). Stopping all other exporters."
for pid in "${PIDS[@]}"; do
    kill $pid
done

# Exit with the exit code of the failed process
exit $EXIT_CODE