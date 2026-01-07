#!/usr/bin/env bash
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

set -euo pipefail

NETTY_LEAK_DETECTION="${NETTY_LEAK_DETECTION:-report}"
NETTY_LEAK_DUMP_DIR="${NETTY_LEAK_DUMP_DIR:-${GITHUB_WORKSPACE:-$(pwd)}/target/netty-leak-dumps}"

case "${NETTY_LEAK_DETECTION}" in
  report|fail_on_leak|off)
    ;;
  *)
    echo "::warning::Unknown NETTY_LEAK_DETECTION='${NETTY_LEAK_DETECTION}', falling back to 'report'."
    NETTY_LEAK_DETECTION="report"
    ;;
esac

if [[ "${NETTY_LEAK_DETECTION}" == "off" ]]; then
  echo "Netty leak detection reporting is disabled (NETTY_LEAK_DETECTION=off)."
  exit 0
fi

mkdir -p "${NETTY_LEAK_DUMP_DIR}"
mkdir -p target

tmp_leak_details="$(mktemp -t netty-leak-details.XXXXXX)"
tmp_dump_files_list="$(mktemp -t netty-leak-dumps.XXXXXX)"
trap 'rm -f "${tmp_leak_details}" "${tmp_dump_files_list}"' EXIT

if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
  echo "::group::Netty leak detection report"
fi

echo "NETTY_LEAK_DETECTION=${NETTY_LEAK_DETECTION}"
echo "NETTY_LEAK_DUMP_DIR=${NETTY_LEAK_DUMP_DIR}"

annotation_prefix="::warning::"
if [[ "${NETTY_LEAK_DETECTION}" == "fail_on_leak" ]]; then
  annotation_prefix="::error::"
fi

find "${NETTY_LEAK_DUMP_DIR}" -type f -name "netty_leak_*.txt" -print 2>/dev/null \
  | sort -u > "${tmp_dump_files_list}" || true

if [[ ! -s "${tmp_dump_files_list}" ]]; then
  echo "No Netty leak markers found."
  touch target/netty_leaks_not_found
  if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
    echo "::endgroup::"
  fi
  exit 0
fi

touch target/netty_leaks_found

report_file="${NETTY_LEAK_DUMP_DIR}/leak_report.txt"
{
  echo "${annotation_prefix}Netty leaks found (mode=${NETTY_LEAK_DETECTION})."
  echo
  echo "Detected Netty leak dump files:"
  cat "${tmp_dump_files_list}"
  echo

  echo "Details:"
  while IFS= read -r file; do
    if [[ -f "${file}" ]]; then
      cat "${file}" >> "${tmp_leak_details}"
    fi
  done < "${tmp_dump_files_list}"
  cat "${tmp_leak_details}"
} | tee "${report_file}"

if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
  echo "::endgroup::"
fi

if [[ "${NETTY_LEAK_DETECTION}" == "fail_on_leak" ]]; then
  exit 1
fi
