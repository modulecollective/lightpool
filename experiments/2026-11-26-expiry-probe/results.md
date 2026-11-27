# Results

Cells: 208. Verdict: **not rejected; rule 1 inconclusive**.

Rule 1: inconclusive. Rule 2: pass. Rule 3: pass.

- Rule 1 is inconclusive: baseline has 1 W deaths, fewer than 3.
- Incomplete cells, retained and excluded from their comparison: probe-correlated-dense-64-aged-r117: interrupted.

## Working-path expiry deaths (W), `correlated-dense-64-aged`

| Arm | Replicates | Complete | W | Cells with W > 0 | Running suspect s | Running dead s |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `base` | 101–120 | 20 | 1 | 1 | 3264.561 | 4.622 |
| `probe` | 101–120 | 19 | 0 | 0 | 3082.377 | 0.000 |
| `smax8` | 101–110 | 10 | 0 | 0 | 1749.165 | 0.000 |
| `smax12` | 101–120 | 20 | 0 | 0 | 3269.183 | 0.000 |
| `sm5` | 101–110 | 10 | 0 | 0 | 1749.165 | 0.000 |

Base and probe both complete on 19 aged replicates (inconclusive: 117): W 1 against 0. Replicates with a death in base only: 1; in probe only: 0.

## Crash detection (L)

| Fixture | Arm | Complete | Mean L s | Mean p50 s | Completed / eligible | Full dissemination |
| --- | --- | ---: | ---: | ---: | --- | --- |
| `crash` | `base` | 6/6 | 8.550 | 8.414 | 186 / 186 | complete 6 |
| `crash` | `probe` | 6/6 | 8.550 | 8.414 | 186 / 186 | complete 6 |
| `crash` | `smax8` | 6/6 | 8.550 | 8.414 | 186 / 186 | complete 6 |
| `crash` | `smax12` | 6/6 | 8.550 | 8.414 | 186 / 186 | complete 6 |
| `crash` | `sm5` | 6/6 | 10.066 | 9.910 | 186 / 186 | complete 6 |
| `small-double-crash` | `base` | 10/10 | 13.983 | 13.947 | 40 / 50 | complete 10, observer-interrupted 10 |
| `small-double-crash` | `probe` | 10/10 | 13.983 | 13.947 | 40 / 50 | complete 10, observer-interrupted 10 |
| `small-double-crash` | `smax8` | 10/10 | 16.940 | 16.899 | 40 / 50 | complete 10, observer-interrupted 10 |
| `small-double-crash` | `smax12` | 10/10 | 22.843 | 22.805 | 40 / 50 | complete 10, observer-interrupted 10 |
| `small-double-crash` | `sm5` | 10/10 | 7.596 | 7.566 | 40 / 50 | complete 10, observer-interrupted 10 |

## Retunes against the probe

| Retune | Common aged | W retune | W probe | Only retune | Only probe | L crash ratio | L double-crash ratio | Ties or beats probe |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `smax8` | 10 | 0 | 0 | 0 | 0 | 1.0000 | 1.2114 | no |
| `smax12` | 19 | 0 | 0 | 0 | 0 | 1.0000 | 1.6336 | no |
| `sm5` | 10 | 0 | 0 | 0 | 0 | 1.1774 | 0.5433 | no |

## Harm

| Fixture | Base mean datagrams | Probe mean datagrams | Ratio | Base running dead | Probe running dead | Null |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `correlated` | 6781.3 | 6781.3 | 1.0000 | 0 | 0 | same |
| `correlated-dense` | 9319.7 | 9318.0 | 0.9998 | 1 | 0 | - |
| `correlated-dense-64-aged` | 18175.6 | 18175.3 | 1.0000 | 1 | 0 | - |
| `correlated-dense-64-aged-healthy` | 13440.0 | 13440.0 | 1.0000 | 0 | 0 | - |
| `crash` | 7353.5 | 7384.3 | 1.0042 | 0 | 0 | - |
| `healthy` | 6720.0 | 6720.0 | 1.0000 | 0 | 0 | same |
| `interval` | 26544.3 | 26607.3 | 1.0024 | 0 | 0 | - |
| `loss` | 13181.7 | 13181.7 | 1.0000 | 0 | 0 | same |
| `partial-partition` | 6777.0 | 6777.0 | 1.0000 | 0 | 0 | same |
| `small-double-crash` | 453.5 | 457.1 | 1.0079 | 0 | 0 | - |
| `threshold` | 8877.7 | 9029.0 | 1.0170 | 40 | 35 | - |

Mean L in `crash`, probe against base over 6 replicates: 8.550 s against 8.550 s.

Mean L in `small-double-crash`, probe against base over 10 replicates: 13.983 s against 13.983 s.

