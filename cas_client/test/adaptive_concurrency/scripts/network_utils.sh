#!/bin/bash

# Function to set up network simulation
setup_network() {
    local latency_ms=$1
    local bandwidth_mbps=$2
    
    echo "Setting up network simulation: ${latency_ms}ms latency, ${bandwidth_mbps}Mbps bandwidth"
    
    # Clear any existing rules
    tc qdisc del dev lo root 2>/dev/null || true
    
    # Add network delay and bandwidth limiting
    tc qdisc add dev lo root handle 1: htb default 12
    tc class add dev lo parent 1: classid 1:1 htb rate 1000mbit
    tc class add dev lo parent 1:1 classid 1:12 htb rate ${bandwidth_mbps}mbit ceil ${bandwidth_mbps}mbit
    tc qdisc add dev lo parent 1:12 netem delay ${latency_ms}ms
    
    # Store network configuration for reporting
    echo "NETWORK_CONFIG_LATENCY_MS=${latency_ms}" > /tmp/network_config
    echo "NETWORK_CONFIG_BANDWIDTH_MBPS=${bandwidth_mbps}" >> /tmp/network_config
    
    # Append network configuration to network_stats.json with timestamp
    local timestamp_ms=$(date +%s%3N)
    local stats_json="{\"timestamp\":\"${timestamp_ms}\",\"latency_ms\":${latency_ms},\"bandwidth_mbps\":${bandwidth_mbps}}"
    echo "${stats_json}" >> network_stats.json
    
    echo "Network simulation configured"
}

# Function to clear network simulation
clear_network() {
    echo "Clearing network simulation"
    tc qdisc del dev lo root 2>/dev/null || true
    echo "Network simulation cleared"
}

# Function to show current network status
show_network() {
    echo "Current network configuration:"
    tc qdisc show dev lo
    tc class show dev lo
}

# Export functions for use in scripts
export -f setup_network
export -f clear_network
export -f show_network
