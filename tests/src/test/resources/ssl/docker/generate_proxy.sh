set -x -e
cd my-ca
export CA_HOME=$(pwd)

echo "Generating pulsarproxy certificate ************"
openssl genrsa -out proxy.key.pem 2048
openssl pkcs8 -topk8 -inform PEM -outform PEM -in proxy.key.pem -out proxy.key-pk8.pem -nocrypt
openssl req -config openssl.cnf -key proxy.key.pem -new -sha256 -out proxy.csr.pem
openssl ca -config openssl.cnf -extensions server_cert -days 1000 -notext -md sha256 -in proxy.csr.pem -out proxy.cert.pem
