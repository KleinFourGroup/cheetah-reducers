#include "cilk/sentinel.h"

#if HASH_REDUCER
#include "reducer/cilkred_map_hash.c"
#else
#include "reducer/cilkred_map_spa.c"
#endif
