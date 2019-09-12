using namespace std;
#define EPOCH_TIME (40) // ms
#define CLOCK_PER_US (1000)
#define LOG_FILE "/tmp/LogFile"

extern uint64_t GlobalEpoch;
extern pthread_mutex_t MutexThreadTable;
extern pthread_mutex_t MutexLogList;
extern pthread_mutex_t MutexDB;
extern pthread_mutex_t MutexToken;
