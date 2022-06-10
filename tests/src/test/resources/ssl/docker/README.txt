If you want to regenerate the certificates follow these steps:
0) run cleanup.sh
1) run generate_ca.sh
answer 'ca' to Common Name
2) run generate_broker.sh
answer 'pulsar' to Common Name

3) run generate_proxy.sh
answer 'pulsarproxy' to Common Name

4) run copy_files.sh
5) run the DockerTest TLS tests and ensure that they pass

Use always 'pulsar' as password for anything
