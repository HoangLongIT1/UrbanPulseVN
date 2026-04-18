#!/bin/bash
# =============================================
# Register Debezium PostgreSQL Connector
# =============================================
# Run AFTER Debezium Connect is fully started.
#
# Usage:
#   bash streaming/debezium/configs/register.sh

set -e

CONNECT_URL="http://localhost:8083"
CONNECTOR_CONFIG="streaming/debezium/configs/postgres-connector.json"

echo "⏳ Waiting for Debezium Connect to be ready..."
until curl -sf "$CONNECT_URL/connectors" > /dev/null 2>&1; do
    echo "   Connect not ready, retrying in 5s..."
    sleep 5
done
echo "✅ Debezium Connect is ready"

echo ""
echo "📤 Registering PostgreSQL connector..."
curl -s -X POST \
    -H "Content-Type: application/json" \
    -d @"$CONNECTOR_CONFIG" \
    "$CONNECT_URL/connectors" | python -m json.tool

echo ""
echo "✅ Connector registered! Checking status..."
sleep 3

curl -s "$CONNECT_URL/connectors/urbanpulse-postgres-connector/status" | python -m json.tool

echo ""
echo "📋 Active connectors:"
curl -s "$CONNECT_URL/connectors" | python -m json.tool
