#ifndef _CILKRED_MAP_H
#define _CILKRED_MAP_H

#include "cilk-internal.h"
#include "debug.h"
#include <cilk/hyperobject_base.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

enum merge_kind {
    MERGE_UNORDERED, ///< Assertion fails
    MERGE_INTO_LEFT, ///< Merges the argument from the right into the left
    MERGE_INTO_RIGHT ///< Merges the argument from the left into the right
};
typedef enum merge_kind merge_kind;

/**
 * @brief Element for a hyperobject
 */
struct elem {
    void *key; // Shared key for this hyperobject
    __cilkrts_hyperobject_base *hb; // Base of the hyperobject.
    void *view; // Strand-private view of this hyperobject
};
typedef struct elem elem;

/// Destroy and deallocate the view object for this element and set view to
/// null.
void elem_destroy(elem * el);

/// Returns true if this element contains a leftmost view.
bool elem_is_leftmost(elem *const el);

/** Bucket containing at most NMAX elements */
struct bucket {
    size_t nmax; // Size of the array of elements for this bucket

    /**
     * We use the ``struct hack'' to allocate an array of variable
     * dimension at the end of the struct.  However, we allocate a
     * total of NMAX+1 elements instead of NMAX.  The last one always
     * has key == 0, which we use as a termination criterion
     */
    elem el[1];
};
typedef struct bucket bucket;

/**
 * Class that implements the map for reducers so we can find the
 * view for a strand.
 */
struct cilkred_map {
    global_state *g; // Handy pointer to the global state
    size_t nelem; // Number of elements in table
    size_t nbuckets; // Number of buckets
    bucket **buckets; // Array of pointers to buckets
    bool merging; // Set true if merging (for debugging purposes)
    bool is_leftmost; // Set true for leftmost reducer map

};
typedef struct cilkred_map cilkred_map;

/** @brief Return element mapped to 'key' or null if not found. */
CHEETAH_INTERNAL
elem * cilkred_map_lookup(cilkred_map * this_map, void *key);

/**
 * Construct an empty reducer map from the memory pool associated with the
 * given worker.  This reducer map must be destroyed before the worker's
 * associated global context is destroyed.
 *
 * @param w __cilkrts_worker the cilkred_map is being created for.
 *
 * @return Pointer to the initialized cilkred_map.
 */
CHEETAH_INTERNAL
cilkred_map *cilkred_map_make_map(__cilkrts_worker *w);

/**
 * Destroy a reducer map.  The map must have been allocated from the worker's
 * global context and should have been allocated from the same worker.
 *
 * @param w __cilkrts_worker the cilkred_map was created for.
 * @param h The cilkred_map to be deallocated.
 */
CHEETAH_INTERNAL
void cilkred_map_destroy_map(__cilkrts_worker *w, cilkred_map *h);

/**
    * @brief Insert key/value element into hash map without rehashing.
    * Does not check for duplicate key.
    */
elem * cilkred_map_insert_no_rehash(cilkred_map * this_map,
            __cilkrts_worker           *w,
            void                       *key,
            __cilkrts_hyperobject_base *hb,
            void                       *value);

/**
    * @brief Insert key/value element into hash map, rehashing if necessary.
    * Does not check for duplicate key.
    */
elem * cilkred_map_rehash_and_insert(cilkred_map * this_map,
                __cilkrts_worker           *w,
                void                       *key,
                __cilkrts_hyperobject_base *hb,
                void                       *value);

/** @brief Grow bucket by one element, reallocating bucket if necessary */
static elem * cilkred_map_grow(__cilkrts_worker *w, bucket **bp);

/** @brief Rehash a worker's reducer map */
void  cilkred_map_rehash(cilkred_map * this_map, __cilkrts_worker * w);

/**
    * @brief Returns true if a rehash is needed due to the number of elements that
    * have been inserted.
    */
inline bool cilkred_map_need_rehash_p(cilkred_map *const this_map);

/** @brief Allocate and initialize the buckets */
void  cilkred_map_make_buckets(cilkred_map * this_map, __cilkrts_worker *w, size_t nbuckets);

/**
    * @brief Merge another reducer map into this one, destroying the other map in
    * the process.
    */
CHEETAH_INTERNAL
__cilkrts_worker*  cilkred_map_merge(cilkred_map * this_map,
            __cilkrts_worker *current_wkr,
            cilkred_map      *other_map,
            merge_kind   kind);

/** @brief check consistency of a reducer map */
void  cilkred_map_check(cilkred_map * this_map, bool allow_null_view);

/** @brief Test whether the cilkred_map is empty */
CHEETAH_INTERNAL
bool  cilkred_map_is_empty(cilkred_map * this_map);

// Static inlines
// Given a __cilkrts_hyperobject_base, return the key to that hyperobject in
// the reducer map.
static inline void* get_hyperobject_key(__cilkrts_hyperobject_base *hb)
{
    // The current implementation uses the address of the lefmost view as the
    // key.
    return ((char *)hb) + hb->__view_offset;
}

// Given a hyperobject key, return a pointer to the leftmost object.  In the
// current implementation, the address of the leftmost object IS the key, so
// this function is an effective noop.
static inline void* get_leftmost_view(void *key)
{
    return key;
}

static inline size_t hashfun(const cilkred_map *h, void *key)
{
    size_t k = (size_t) key;

    k ^= k >> 21;
    k ^= k >> 8;
    k ^= k >> 3;

    return k & (h->nbuckets - 1);
}

#endif
