#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

wait_for_url() {
    URL=$1
    MSG=$2

    if [[ $URL == https* ]]; then
        CMD="curl -k -sL -o /dev/null -w %{http_code} $URL"
    else
        CMD="curl -sL -o /dev/null -w %{http_code} $URL"
    fi

    until [ "200" == "$($CMD)" ]
    do
        echo "$MSG ($URL)"
        sleep 2
    done
}

# Start hydra server
COMPOSE_FILE="ci/hydra/docker-compose.yml"

# Try the “docker compose” subcommand (Docker CLI v2+)
if docker compose version >/dev/null 2>&1; then
  docker compose -f "$COMPOSE_FILE" up -d

# Fall back to the standalone “docker-compose” binary
elif command -v docker-compose >/dev/null 2>&1; then
  docker-compose -f "$COMPOSE_FILE" up -d

else
  echo "Error: Neither 'docker compose' nor 'docker-compose' is available on this system." >&2
  exit 1
fi

# Wait until the hydra server started
wait_for_url "http://localhost:4445/clients" "Waiting for Hydra admin REST to start"
