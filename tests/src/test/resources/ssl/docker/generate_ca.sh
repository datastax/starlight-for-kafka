set -x -e
mkdir my-ca
cd my-ca
cp ../openssl.cnf .
export CA_HOME=$(pwd)
mkdir certs crl newcerts private
chmod 700 private/
touch index.txt
echo 1000 > serial
openssl genrsa -aes256 -out private/ca.key.pem 4096
# You need enter a password in the command above
chmod 400 private/ca.key.pem
openssl req -config openssl.cnf -key private/ca.key.pem \
    -new -x509 -days 7300 -sha256 -extensions v3_ca \
    -out certs/ca.cert.pem
# You must enter the same password in the previous openssl command
chmod 444 certs/ca.cert.pem

keytool -importcert -trustcacerts -file certs/ca.cert.pem -keystore certs/ca.jks -storepass pulsar  -noprompt 


