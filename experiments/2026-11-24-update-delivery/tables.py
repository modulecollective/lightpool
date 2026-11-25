#!/usr/bin/env python3
"""Derive every numerical report table from retained deterministic rows."""
import csv
from decimal import Decimal
from pathlib import Path

record = Path(__file__).resolve().parent
with (record / 'cells.tsv').open() as f:
    rows = list(csv.DictReader(f, delimiter='\t'))
assert len(rows) == 6

def ms(ns):
    return '--' if ns == 'null' else f'{Decimal(ns) / 1000000:.3f}'

print(r'\begin{table}[ht]\centering\small')
print(r'\begin{tabular}{rr|rr|r|rr}')
print(r'Loss & Exchange & Delivered & Missed & Pair p95 & All completed & All p95 \\')
print(r'(\%) & & /620 & /620 & (ms) & /20 & (ms) \\ \hline')
for r in rows:
    name = r['case'].split('-')
    print(' & '.join([name[3], name[5], r['delivered'], r['missed'],
                     ms(r['pairP95NS']), r['allCompleted'], ms(r['allP95NS'])]) + r' \\')
print(r'\end{tabular}')
print(r'\caption{Four-second acceptance. Quantiles condition on completion within four seconds; all-receiver quantiles exclude non-completed updates. One paired replicate per cell.}\end{table}')

print(r'\begin{table}[ht]\centering\small')
print(r'\begin{tabular}{rr|rrr|rr}')
print(r'Loss & Exchange & Measured UDP & IPv4 UDP & Stream & Whole UDP & Stream \\')
print(r'(\%) & & frames (B) & sens. (B) & (B) & frames (B) & (B) \\ \hline')
for r in rows:
    for interval in ['measured', 'wholeRun']:
        for field in ['datagramCount', 'datagramBytes', 'streamBytes', 'ipv4UDPBytes']:
            assert r[f'{interval}_attempted_{field}'] == r[f'{interval}_sent_{field}']
    name = r['case'].split('-')
    keys = ['measured_sent_datagramBytes', 'measured_sent_ipv4UDPBytes',
            'measured_sent_streamBytes', 'wholeRun_sent_datagramBytes', 'wholeRun_sent_streamBytes']
    print(' & '.join([name[3], name[5]] + [r[k] for k in keys]) + r' \\')
print(r'\end{tabular}')
print(r'\caption{All sent traffic: measured closed interval [50,130] s and whole observed run. Offered attempt costs equal sent costs in these six attempts. IPv4 UDP sensitivity adds 28 bytes per datagram; stream bytes are framed bytes, not TCP wire cost.}\end{table}')
