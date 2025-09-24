#!/bin/bash

echo "🚀 Testing Robust Webhook ETL Server"
echo "===================================="

# Start the webhook server in background
echo "Starting webhook server..."
cd /home/sujit/Projects/sql
go run examples/webhook_etl_server.go &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo ""
echo "📡 Testing webhook endpoints..."
echo ""

# Test 1: JSON webhook
echo "Test 1: Sending JSON data"
echo "curl -X POST http://localhost:8080/webhook -H 'Content-Type: application/json' -d '{\"id\": 123, \"name\": \"John Doe\", \"email\": \"john@example.com\"}'"
curl -X POST http://localhost:8080/webhook \
     -H 'Content-Type: application/json' \
     -d '{"id": 123, "name": "John Doe", "email": "john@example.com"}' \
     -w "\nHTTP Status: %{http_code}\n"
echo ""

# Test 2: HL7 webhook
echo "Test 2: Sending HL7 data"
HL7_DATA='MSH|^~\&|Test|Test|Test|Test|20230924120000||ADT^A01|123|P|2.5|||AL|NE
PID|1||123^^^MR||Doe^John||19800101|M|||123 Main St^Anytown^CA^12345||555-1234|||||123-45-6789
PV1|1|I|WARD1^BED1||||DRSMITH|||MED|||||ADM||SELF||||||20230924120000'

echo "curl -X POST http://localhost:8080/webhook -H 'Content-Type: text/plain' -d '[HL7 data]'"
curl -X POST http://localhost:8080/webhook \
     -H 'Content-Type: text/plain' \
     -d "$HL7_DATA" \
     -w "\nHTTP Status: %{http_code}\n"
echo ""

# Test 3: SMPP PDU
echo "Test 3: Sending SMPP bind_transceiver PDU"
# Create a binary SMPP PDU file
python3 -c "
import struct
# SMPP bind_transceiver PDU
# Body: system_id, password, system_type, interface_version, addr_ton, addr_npi, address_range
system_id = b'test\x00'          # 5 bytes
password = b'password\x00'       # 9 bytes
system_type = b'sms\x00'         # 4 bytes
interface_version = 0x34         # 1 byte
addr_ton = 0                     # 1 byte
addr_npi = 0                     # 1 byte
address_range = b'\x00'          # 1 byte

body = system_id + password + system_type + bytes([interface_version, addr_ton, addr_npi]) + address_range
command_length = 16 + len(body)  # Header (16) + body length
command_id = 0x00000009          # bind_transceiver
command_status = 0
sequence_number = 1

pdu = struct.pack('>LLLL', command_length, command_id, command_status, sequence_number) + body
with open('/tmp/smpp_pdu.bin', 'wb') as f:
    f.write(pdu)
"

echo "curl -X POST http://localhost:8080/webhook -H 'Content-Type: application/octet-stream' -H 'X-Data-Type: smpp' --data-binary @/tmp/smpp_pdu.bin"
curl -X POST http://localhost:8080/webhook \
     -H 'Content-Type: application/octet-stream' \
     -H 'X-Data-Type: smpp' \
     --data-binary @/tmp/smpp_pdu.bin \
     -w "\nHTTP Status: %{http_code}\n"
echo ""

# Test 4: Health check
echo "Test 4: Health check"
echo "curl http://localhost:8080/health"
curl http://localhost:8080/health
echo ""

# Cleanup
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo ""
echo "✅ Webhook ETL testing completed!"
echo ""
echo "💡 To run the server continuously:"
echo "   cd /home/sujit/Projects/sql && go run examples/webhook_etl_server.go"
echo ""
echo "💡 To test manually:"
echo "   curl -X POST http://localhost:8080/webhook -H 'Content-Type: application/json' -d '{\"test\": \"data\"}'"
