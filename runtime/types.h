#ifndef _CILK_TYPES_H
#define _CILK_TYPES_H

#include <stdint.h>

// Might be redundant
#include "cilk/sentinel.h"

typedef uint32_t worker_id;
#define WORKER_ID_FMT PRIu32
typedef struct __cilkrts_worker __cilkrts_worker;
typedef struct __cilkrts_stack_frame __cilkrts_stack_frame;
typedef struct global_state global_state;
typedef struct cilkred_map cilkred_map;

#if COMM_REDUCER && ! HASH_REDUCER
typedef struct com_cilkred_map com_cilkred_map;
#endif

#define NO_WORKER 0xffffffffu /* type worker_id */

#endif /* _CILK_TYPES_H */
