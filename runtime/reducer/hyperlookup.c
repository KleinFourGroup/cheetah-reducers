#include "cilk/sentinel.h"

#if HASH_REDUCER
#include "hyperlookup_hash.c"
#else
#include "hyperlookup_spa.c"
#endif
