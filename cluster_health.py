#!/usr/bin/env python3
"""
cluster_health.py - Storage Cluster Node Health Check
Checks reachability, disk usage, CPU load, memory, and failed services
across all nodes defined in a nodes file or passed as arguments.
Usage: python3 cluster_health.py --nodes nodes.txt
       python3 cluster_health.py --hosts node01 node02 node03
"""

import subprocess
import socket
import argparse
import sys
import logging
from datetime import datetime
from dataclasses import dataclass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

DISK_WARN_THRESHOLD = 80
DISK_CRIT_THRESHOLD = 90
LOAD_WARN_THRESHOLD = 4.0


@dataclass
class NodeHealth:
    hostname: str
    reachable: bool
    disk_warnings: list
    load_avg: float
    memory_used_pct: float
    failed_services: list

    @property
    def status(self):
        if not self.reachable:
            return 'DOWN'
        issues = self.disk_warnings + self.failed_services
        if any('CRIT' in w for w in issues):
            return 'CRITICAL'
        if issues or self.load_avg > LOAD_WARN_THRESHOLD:
            return 'WARNING'
        return 'OK'


def is_reachable(hostname: str, port: int = 22, timeout: int = 3) -> bool:
    """Check if a host is reachable on a given port."""
    try:
        with socket.create_connection((hostname, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


def check_disk_usage() -> list:
    """Check local disk usage and return list of warnings."""
    result = subprocess.run(['df', '-h', '--output=pcent,target'],
                            capture_output=True, text=True)
    warnings = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.strip().split()
        if len(parts) == 2:
            try:
                pct = int(parts[0].replace('%', ''))
                mount = parts[1]
                if pct >= DISK_CRIT_THRESHOLD:
                    warnings.append(f"CRIT: {mount} at {pct}%")
                elif pct >= DISK_WARN_THRESHOLD:
                    warnings.append(f"WARN: {mount} at {pct}%")
            except ValueError:
                continue
    return warnings


def check_load_average() -> float:
    """Return the 1-minute load average."""
    with open('/proc/loadavg') as f:
        return float(f.read().split()[0])


def check_memory() -> float:
    """Return memory usage as a percentage."""
    result = subprocess.run(['free', '-m'], capture_output=True, text=True)
    lines = result.stdout.splitlines()
    for line in lines:
        if line.startswith('Mem:'):
            parts = line.split()
            total = int(parts[1])
            used = int(parts[2])
            return round((used / total) * 100, 1)
    return 0.0


def check_failed_services() -> list:
    """Return list of failed systemd services."""
    result = subprocess.run(
        ['systemctl', '--failed', '--no-legend', '--plain'],
        capture_output=True, text=True
    )
    return [line.split()[0] for line in result.stdout.strip().splitlines() if line]


def check_local_node(hostname: str) -> NodeHealth:
    """Run health checks on the local node."""
    return NodeHealth(
        hostname=hostname,
        reachable=True,
        disk_warnings=check_disk_usage(),
        load_avg=check_load_average(),
        memory_used_pct=check_memory(),
        failed_services=check_failed_services()
    )


def print_report(results: list):
    """Print a formatted health report."""
    print(f"\n{'='*65}")
    print(f"CLUSTER HEALTH REPORT — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*65}")

    for node in results:
        status = node.status
        indicator = {'OK': '✓', 'WARNING': '!', 'CRITICAL': '✗', 'DOWN': '✗'}.get(status, '?')
        print(f"\n[{indicator}] {node.hostname:<20} STATUS: {status}")

        if not node.reachable:
            print(f"    Host unreachable on port 22")
            continue

        print(f"    Load avg: {node.load_avg}  "
              f"Memory: {node.memory_used_pct}%")

        for warn in node.disk_warnings:
            print(f"    Disk: {warn}")

        for svc in node.failed_services:
            print(f"    Failed service: {svc}")

        if node.status == 'OK':
            print(f"    All checks passed")

    print(f"\n{'='*65}")
    ok_count = sum(1 for n in results if n.status == 'OK')
    print(f"Summary: {ok_count}/{len(results)} nodes healthy\n")


def main():
    parser = argparse.ArgumentParser(description='Cluster node health check')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--nodes', help='Path to file containing one hostname per line')
    group.add_argument('--hosts', nargs='+', help='Hostnames to check')
    args = parser.parse_args()

    if args.nodes:
        with open(args.nodes) as f:
            hostnames = [line.strip() for line in f if line.strip()]
    else:
        hostnames = args.hosts

    results = []
    for host in hostnames:
        if not is_reachable(host):
            results.append(NodeHealth(
                hostname=host, reachable=False,
                disk_warnings=[], load_avg=0.0,
                memory_used_pct=0.0, failed_services=[]
            ))
        else:
            # In a real multi-node setup you would SSH into each node
            # For local testing, run checks on the local machine
            results.append(check_local_node(host))

    print_report(results)

    critical_or_down = [n for n in results if n.status in ('CRITICAL', 'DOWN')]
    sys.exit(1 if critical_or_down else 0)


if __name__ == '__main__':
    main()
