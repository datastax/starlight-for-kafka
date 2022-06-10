set -x -e
cd my-ca
export CA_HOME=$(pwd)

echo "Generating pulsar certificate ************"
openssl genrsa -out broker.key.pem 2048
openssl pkcs8 -topk8 -inform PEM -outform PEM -in broker.key.pem -out broker.key-pk8.pem -nocrypt
openssl req -config openssl.cnf -key broker.key.pem -new -sha256 -out broker.csr.pem
openssl ca -config openssl.cnf -extensions server_cert -days 1000 -notext -md sha256 -in broker.csr.pem -out broker.cert.pem
