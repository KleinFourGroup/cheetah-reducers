#include "cilk/sentinel.h"

#if HASH_REDUCER
#include "reducer/reducer_impl_hash.c"
#else
#include "reducer/reducer_impl_spa.c"
#endif
