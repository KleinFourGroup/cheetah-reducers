/**
 * Support for reducers
 */

#include "reducer_impl.h"
#include "cilk/hyperobject_base.h"
#include "cilk/sentinel.h"
#include "global.h"
#include "init.h"
#include "internal-malloc.h"
#include "mutex.h"
#include "scheduler.h"
#include <assert.h>
#include <dlfcn.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include <limits.h>

#define USE_INTERNAL_MALLOC 1

#if INLINE_ALL_TLS
extern __thread __cilkrts_worker *tls_worker;
#endif

const char *UNSYNCED_REDUCER_MSG =
    "Destroying a reducer while it is visible to unsynced child tasks, or\n"
    "calling CILK_C_UNREGISTER_REDUCER() on an unregistered reducer.\n"
    "Did you forget a _Cilk_sync or CILK_C_REGISTER_REDUCER()?";

// =================================================================
// Init / deinit functions
// =================================================================

void reducers_init(global_state *g) {
    // Empty for hashmap
}

void reducers_deinit(global_state *g) {
    cilkrts_alert(BOOT, NULL, "(reducers_deinit) Cleaning up reducers");
    // Empty for hashmap
}

CHEETAH_INTERNAL void reducers_import(global_state *g, __cilkrts_worker *w) {
    // Empty for hashmap
}

cilkred_map* install_new_reducer_map(__cilkrts_worker *w) {
    cilkred_map *h;
    h = cilkred_map_make_map(w);
    w->reducer_map = h;
    return h;
}

/* remove the reducer from the current reducer map.  If the reducer
   exists in maps other than the current one, the behavior is
   undefined. */
void __cilkrts_hyper_destroy(__cilkrts_hyperobject_base *hb)
{
    __cilkrts_worker* w = __cilkrts_get_tls_worker();
    if (__builtin_expect(!w, 0)) {
        w = default_cilkrts->workers[default_cilkrts->exiting_worker];
    }

    cilkred_map* h = w->reducer_map;
    if (NULL == h)
	cilkrts_bug(w, UNSYNCED_REDUCER_MSG); // Does not return

    if (h->merging) {
        CILK_ASSERT(w, w == __cilkrts_get_tls_worker());
        cilkrts_bug(w, "User error: hyperobject used by another hyperobject");
    }

    void* key = get_hyperobject_key(hb);
    elem *el = cilkred_map_lookup(h, key);

    // Verify that the reducer is being destroyed from the leftmost strand for
    // which the reducer is defined.
    if (! (el && elem_is_leftmost(el)))
	cilkrts_bug(w, UNSYNCED_REDUCER_MSG);
	
    // Remove the element from the hash bucket.  Do not bother shrinking
    // the bucket. Note that the destroy() function does not actually
    // call the destructor for the leftmost view.
    elem_destroy(el);
    do {
        el[0] = el[1];
        ++el;
    } while (el->key);
    --h->nelem;
}
    
void __cilkrts_hyper_create(__cilkrts_hyperobject_base *hb)
{
    // This function registers the specified hyperobject in the current
    // reducer map and registers the initial value of the hyperobject as the
    // leftmost view of the reducer.
    __cilkrts_worker *w = __cilkrts_get_tls_worker();
    if (__builtin_expect(!w, 0)) {
        w = default_cilkrts->workers[default_cilkrts->exiting_worker];
    }

    void* key = get_hyperobject_key(hb);
    void* view = get_leftmost_view(key);
    cilkred_map *h = w->reducer_map;

    if (__builtin_expect(!h, 0)) {
	    h = install_new_reducer_map(w);
    }

    /* Must not exist. */
    CILK_ASSERT(w, cilkred_map_lookup(h, key) == NULL);

    if (h->merging)
        cilkrts_bug(w, "User error: hyperobject used by another hyperobject");

    CILK_ASSERT(w, w->reducer_map == h);
    // The address of the leftmost value is the same as the key for lookup.
    (void) cilkred_map_rehash_and_insert(h, w, view, hb, view);
}

void inline_cilkrts_bug(__cilkrts_worker *w, char *s) {
    cilkrts_bug(w, s);
}

void inline_promote_own_deque(__cilkrts_worker *w) {
    CILK_ASSERT(w, w->g->nworkers == 1);
    promote_own_deque(w);
}

#if !INLINE_FULL_LOOKUP
#include "hyperlookup.c"
#endif

void *__cilkrts_hyper_alloc(__cilkrts_hyperobject_base *key, size_t bytes) {
    if (USE_INTERNAL_MALLOC) {
#if INLINE_ALL_TLS
        __cilkrts_worker *w = tls_worker;
#else
        __cilkrts_worker *w = __cilkrts_get_tls_worker();
#endif
        if (!w)
            // Use instead the worker from the default CilkRTS that last exited
            // a Cilkified region
            w = default_cilkrts->workers[default_cilkrts->exiting_worker];
        return cilk_internal_malloc(w, bytes, IM_REDUCER_MAP);
    } else
        return cilk_aligned_alloc(16, bytes);
}

void __cilkrts_hyper_dealloc(__cilkrts_hyperobject_base *key, void *view) {
    if (USE_INTERNAL_MALLOC) {
#if INLINE_ALL_TLS
        __cilkrts_worker *w = tls_worker;
#else
        __cilkrts_worker *w = __cilkrts_get_tls_worker();
#endif
        if (!w)
            // Use instead the worker from the default CilkRTS that last exited
            // a Cilkified region
            w = default_cilkrts->workers[default_cilkrts->exiting_worker];
        cilk_internal_free(w, view, key->__view_size, IM_REDUCER_MAP);
    } else
        free(view);
}

/* No-op destroy function */
void __cilkrts_hyperobject_noop_destroy(void* ignore, void* ignore2)
{
}

// =================================================================
// Helper function for the scheduler
// =================================================================

#if DL_INTERPOSE
#define START_DL_INTERPOSABLE(func, type)                               \
    if (__builtin_expect(dl_##func == NULL, false)) {                   \
        dl_##func = (type)dlsym(RTLD_DEFAULT, #func);                   \
        if (__builtin_expect(dl_##func == NULL, false)) {               \
            char *error = dlerror();                                    \
            if (error != NULL) {                                        \
                fputs(error, stderr);                                   \
                fflush(stderr);                                         \
                abort();                                                \
            }                                                           \
        }                                                               \
    }

typedef cilkred_map *(*merge_two_rmaps_t)(__cilkrts_worker *const,
                                          cilkred_map *,
                                          cilkred_map *);
static merge_two_rmaps_t dl___cilkrts_internal_merge_two_rmaps = NULL;

cilkred_map *merge_two_rmaps(__cilkrts_worker *const ws,
                             cilkred_map *left,
                             cilkred_map *right) {
    START_DL_INTERPOSABLE(__cilkrts_internal_merge_two_rmaps,
                          merge_two_rmaps_t);

    return dl___cilkrts_internal_merge_two_rmaps(ws, left, right);
}
#endif // DL_INTERPOSE

cilkred_map *__cilkrts_internal_merge_two_rmaps(__cilkrts_worker *const ws,
		   cilkred_map *left_map,
		   cilkred_map *right_map)
{
    if (!left_map) {
        return right_map;
    }

    if (!right_map) {
        return left_map;
    }
    
    /* Special case, if left_map is leftmost, then always merge into it.
       For C reducers this forces lazy creation of the leftmost views. */
    if (left_map->is_leftmost || left_map->nelem > right_map->nelem) {	
	    cilkred_map_merge(left_map, ws, right_map, MERGE_INTO_LEFT);
        return left_map;
    } else {
        cilkred_map_merge(right_map, ws, left_map, MERGE_INTO_RIGHT);
        return right_map;
    }
}
