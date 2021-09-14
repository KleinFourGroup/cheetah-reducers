#ifndef _CILKRED_MAP_STUB_H
#define _CILKRED_MAP_STUB_H

#include "cilk/sentinel.h"

#if HASH_REDUCER
#include "reducer/cilkred_map_hash.h"
#else
#include "reducer/cilkred_map_spa.h"
#endif

#endif