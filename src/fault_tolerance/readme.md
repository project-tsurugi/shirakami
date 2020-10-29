# Fault tolerance

## P-WAL (pwal.h/cpp)

### Outline
P-WAL is parallel write-ahead logging. Each worker thread has own WAL buffer and log file, and writes log to own log 
file independently. 

### Future work
- Shrink log file. : Currently, log files continues to become large size. To prevent, it should read log file, sort, and 
merge regularly.

## Concurrent Prefix Recovery, CPR (cpr.h)

### Outline
Concurrent prefix recovery is checkpointing method ( https://www.microsoft.com/en-us/research/uploads/prod/2019/01/cpr-sigmod19.pdf ).

### Future work
- Integration of epoch framework between silo and cpr : 
Currently Silo's epoch is used for garbage collection, and CPR's epoch is not adopted for a variant whose the 
performance is higher than original.
