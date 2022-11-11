- Definition of words
  - DML: Api of shirakami (open_scan, next, read_key_from_scan,
  read_value_from_scan, close_scan, delete_record, exist_key, insert, search_key,
  update, upsert
  - LTX: long tx (started by tx_begin api).
  - RTX: read only tx (started by tx_begin api).
  - STX: short tx.

```mermaid
stateDiagram-v2
    WAITING_START
    note right of WAITING_START
        Trigger: LTX and RTX are in this state first. 
        Executable api: tx_check, release_tx_state_handle
        Non allowed: DML
    end note
    STARTED
    note left of STARTED
        Trigger, STX: It is in this state first. 
        Trigger, LTX: It becomes an operable epoch.
        Trigger, RTX: It becomes an operable epoch.
        Executable api: Almost all
    end note
    WAITING_CC_COMMIT
    note right of WAITING_CC_COMMIT
        Trigger: LTX executed commit api but it can't execute that due to 
        existing high priority LTXs.
        Executable api: abort, tx_check, DML, release_tx_state_handle
        Non allowed: commit
    end note
    CC_COMMITTED
    note right of CC_COMMITTED
        Trigger: After notified WAITING_CC_COMMIT status, shirakami executed 
        commit process for the transaction and it succeeded.
        Executable api: tx_begin, DML (will start STX)
        Non allowed:
    end note
    ABORTED
    note right of ABORTED
        Trigger: The tx executed abort api or was aborted internally by other tx.
        Executable api: tx_begin, DML (will start STX)
        Non allowed:
    end note
    WAITING_DURABLE
    note right of WAITING_DURABLE
        Trigger: The commit() api is called for the tx and is successful in the 
        view point of concurrency control,
        and wait to be durable by logging.
        Executable api: tx_check.
        Non allowed: DML
    end note
    DURABLE
    note left of DURABLE
        Trigger: It is durable.
        Executable api: Almost all api. DML (for next STX).
    end note

    [*] --> STARTED: STX
    [*] --> WAITING_START: LTX
    [*] --> WAITING_START: RTX
    WAITING_START --> STARTED
    STARTED --> WAITING_DURABLE: LTX
    STARTED --> WAITING_CC_COMMIT: LTX
    WAITING_CC_COMMIT --> CC_COMMITTED
    WAITING_CC_COMMIT --> ABORTED
    CC_COMMITTED --> WAITING_DURABLE
    STARTED --> WAITING_DURABLE: STX
    STARTED --> WAITING_DURABLE: LTX
    STARTED --> WAITING_DURABLE: RTX
    STARTED --> ABORTED: STX
    STARTED --> ABORTED: LTX
    STARTED --> ABORTED: RTX
    WAITING_DURABLE --> DURABLE
```

- Comment about  state diagram
--