# Tests about concurrency control

## Directory structure
- hybrid
  - Tests about a workload: short tx + long tx.
- long_tx
  - Tests about a workload: long tx only.
- long_tx_or_hybrid
  - To allocate long_tx or hybrid directory later. There are so many old tests,
  so allocation work takes much time. So i do on-demand.
- short_tx
  - Tests about a workload: short tx only.