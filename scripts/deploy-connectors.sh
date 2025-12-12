#!/bin/bash
# Deploy Kafka Connect connectors for Phase 2

CONNECT_HOST="${CONNECT_HOST:-localhost:8083}"
CONFIG_DIR="$(dirname "$0")/connect-config"

echo "Deploying Kafka Connect connectors..."
echo "Connect Host: $CONNECT_HOST"
echo "Config Directory: $CONFIG_DIR"
echo ""

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
until curl -s "$CONNECT_HOST/connectors" > /dev/null 2>&1; do
    echo "  Kafka Connect not ready, waiting..."
    sleep 5
done
echo "Kafka Connect is ready!"
echo ""

# Function to deploy a connector
deploy_connector() {
    local config_file=$1
    local connector_name=$(jq -r '.name' "$config_file")

    echo "Deploying connector: $connector_name"

    # Check if connector exists
    if curl -s "$CONNECT_HOST/connectors/$connector_name" > /dev/null 2>&1; then
        echo "  Connector exists, updating..."
        curl -s -X PUT "$CONNECT_HOST/connectors/$connector_name/config" \
            -H "Content-Type: application/json" \
            -d "$(jq '.config' "$config_file")" | jq .
    else
        echo "  Creating new connector..."
        curl -s -X POST "$CONNECT_HOST/connectors" \
            -H "Content-Type: application/json" \
            -d "@$config_file" | jq .
    fi
    echo ""
}

# Deploy all connectors
for config_file in "$CONFIG_DIR"/*.json; do
    if [ -f "$config_file" ]; then
        deploy_connector "$config_file"
    fi
done

echo "Listing all connectors:"
curl -s "$CONNECT_HOST/connectors" | jq .

echo ""
echo "Connector deployment complete!"
