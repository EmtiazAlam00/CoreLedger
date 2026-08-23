#!/usr/bin/env bash
# Generates a throwaway dev CA plus one server cert (for nginx) and one
# client cert (representing a registered payment-initiation client), all
# signed by that CA. Re-run any time to regenerate — nothing here is a
# real credential.
set -euo pipefail

CERT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/nginx/certs"
mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

openssl req -x509 -newkey rsa:2048 -days 3650 -nodes \
  -keyout ca.key -out ca.crt -subj "/CN=CoreLedger Dev CA" \
  -addext "basicConstraints=critical,CA:true" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
  -subj "/CN=localhost"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 3650
rm server.csr

openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr \
  -subj "/CN=coreledger-test-client"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days 3650
rm client.csr

echo "Generated CA, server, and client certs in $CERT_DIR"
