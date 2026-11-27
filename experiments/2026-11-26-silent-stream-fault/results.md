# Results

Cells: 79. Verdict: **not explained**.

- Control gate: pass, all 18 fixed control cells reproduce the 2026-11-20 virtual-cells.tsv row in the seven gate columns.
- Held-out pairs (indices 201–230): 29 valid, 1 dropped: 229 (control invalid-warmup/invalid-warmup; silent invalid-warmup/invalid-warmup).
- R = 10 control witness cells, S = 10 silent witness cells; b = 0 control-only pairs, c = 0 silent-only pairs; P(Bin(0, 1/2) ≥ 0) = 1.0000.
- Silent witness cells: 10 of 31 recorded silent cells (10 held-out, 0 fixed); 45 were declared.

## Fixed cells, control beside silent

| Case | Index | Control witnesses | Silent witnesses | Control failed probes / suspicions / eligible | Silent failed probes / suspicions / eligible | Control stop | Silent stop |
| --- | ---: | ---: | ---: | --- | --- | --- | --- |
| `correlated-dense` | 1 | 0 | 0 | 6 / 4 / 28 | 6 / 4 / 28 | no-qualifying-same-group-shortening-before-refutation | no-qualifying-same-group-shortening-before-refutation |
| `correlated-dense` | 2 | 0 | not launched | 9 / 4 / 27 | - | no-qualifying-same-group-shortening-before-refutation | - |
| `correlated-dense` | 3 | 3 | not launched | 4 / 4 / 21 | - | mechanism-observed | - |
| `correlated-dense-latency-100us` | 1 | 0 | not launched | 6 / 4 / 28 | - | no-qualifying-same-group-shortening-before-refutation | - |
| `correlated-dense-latency-100us` | 2 | 0 | not launched | 9 / 4 / 28 | - | no-qualifying-same-group-shortening-before-refutation | - |
| `correlated-dense-latency-100us` | 3 | 3 | not launched | 4 / 4 / 21 | - | mechanism-observed | - |
| `correlated-dense-latency-10ms` | 1 | 0 | not launched | 5 / 4 / 28 | - | no-qualifying-same-group-shortening-before-refutation | - |
| `correlated-dense-latency-10ms` | 2 | 7 | not launched | 6 / 5 / 13 | - | mechanism-observed | - |
| `correlated-dense-latency-10ms` | 3 | 0 | not launched | 4 / 4 / 21 | - | no-qualifying-same-group-shortening-before-refutation | - |
| `correlated-dense-service-100us` | 1 | 0 | not launched | 6 / 4 / 28 | - | no-qualifying-same-group-shortening-before-refutation | - |
| `correlated-dense-service-100us` | 2 | 0 | not launched | 9 / 4 / 25 | - | no-qualifying-same-group-shortening-before-refutation | - |
| `correlated-dense-service-100us` | 3 | 1 | not launched | 6 / 6 / 27 | - | mechanism-observed | - |
| `correlated-dense-service-1ms` | 1 | 0 | not launched | 5 / 4 / 26 | - | no-qualifying-same-group-shortening-before-refutation | - |
| `correlated-dense-service-1ms` | 2 | 0 | not launched | 9 / 6 / 29 | - | no-qualifying-same-group-shortening-before-refutation | - |
| `correlated-dense-service-1ms` | 3 | 6 | not launched | 7 / 7 / 28 | - | mechanism-observed | - |

## Held-out pairs, `correlated-dense` against `correlated-dense-silent`

| Index | Control witnesses | Silent witnesses | Control failed probes / suspicions / eligible / harmful | Silent failed probes / suspicions / eligible / harmful |
| ---: | ---: | ---: | --- | --- |
| 201 | 7 | 7 | 6 / 5 / 23 / 0 | 6 / 5 / 23 / 0 |
| 202 | 7 | 7 | 6 / 5 / 21 / 0 | 6 / 5 / 21 / 0 |
| 203 | 0 | 0 | 5 / 4 / 24 / 0 | 5 / 4 / 24 / 0 |
| 204 | 0 | 0 | 2 / 2 / 13 / 0 | 2 / 2 / 13 / 0 |
| 205 | 0 | 0 | 7 / 5 / 28 / 0 | 7 / 5 / 28 / 0 |
| 206 | 0 | 0 | 6 / 6 / 41 / 0 | 6 / 6 / 41 / 0 |
| 207 | 0 | 0 | 6 / 3 / 14 / 0 | 6 / 3 / 14 / 0 |
| 208 | 0 | 0 | 2 / 2 / 13 / 0 | 2 / 2 / 13 / 0 |
| 209 | 0 | 0 | 6 / 6 / 37 / 0 | 6 / 6 / 37 / 0 |
| 210 | 0 | 0 | 7 / 5 / 25 / 0 | 7 / 5 / 25 / 0 |
| 211 | 0 | 0 | 5 / 5 / 27 / 0 | 5 / 5 / 27 / 0 |
| 212 | 1 | 1 | 4 / 4 / 21 / 0 | 4 / 4 / 21 / 0 |
| 213 | 13 | 14 | 4 / 4 / 14 / 0 | 4 / 4 / 14 / 0 |
| 214 | 1 | 1 | 5 / 4 / 21 / 0 | 5 / 4 / 21 / 0 |
| 215 | 0 | 0 | 4 / 3 / 20 / 0 | 4 / 3 / 21 / 0 |
| 216 | 0 | 0 | 5 / 4 / 28 / 0 | 5 / 4 / 28 / 0 |
| 217 | 0 | 0 | 2 / 1 / 5 / 0 | 2 / 1 / 5 / 0 |
| 218 | 6 | 6 | 7 / 6 / 34 / 0 | 7 / 6 / 35 / 0 |
| 219 | 4 | 4 | 7 / 6 / 35 / 0 | 7 / 6 / 35 / 0 |
| 220 | 6 | 6 | 7 / 7 / 35 / 0 | 7 / 7 / 35 / 0 |
| 221 | 0 | 0 | 2 / 1 / 7 / 0 | 2 / 1 / 7 / 0 |
| 222 | 0 | 0 | 9 / 7 / 28 / 0 | 9 / 7 / 28 / 0 |
| 223 | 0 | 0 | 3 / 2 / 14 / 0 | 3 / 2 / 14 / 0 |
| 224 | 2 | 2 | 7 / 4 / 18 / 0 | 7 / 4 / 18 / 0 |
| 225 | 0 | 0 | 6 / 4 / 26 / 0 | 6 / 3 / 21 / 0 |
| 226 | 0 | 0 | 3 / 2 / 14 / 0 | 3 / 2 / 14 / 0 |
| 227 | 7 | 7 | 8 / 7 / 30 / 0 | 8 / 7 / 30 / 0 |
| 228 | 0 | 0 | 3 / 3 / 20 / 0 | 3 / 3 / 20 / 0 |
| 229 | - | - | invalid-warmup | invalid-warmup |
| 230 | 0 | 0 | 6 / 5 / 35 / 0 | 6 / 5 / 35 / 0 |

## Description by arm (valid held-out pairs only)

| Arm | Cells | Witness cells | Witnesses | Failed probes | Local suspicions | Eligible episodes | Harmful |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| control | 29 | 10 | 54 | 150 | 122 | 671 | 0 |
| silent | 29 | 10 | 55 | 150 | 121 | 668 | 0 |

