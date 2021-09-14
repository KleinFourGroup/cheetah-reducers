/**
 * Support for reducers
 */

#include "cilkred_map.h"


#define DBG if(0) // if(1) enables some internal checks

// elem functions

void elem_destroy(elem * el)
{
    if (!elem_is_leftmost(el)) {

        // Call destroy_fn and deallocate_fn on the view, but not if it's the
        // leftmost view.
        cilk_c_monoid *monoid = &(el->hb->__c_monoid);
        // cilk_destroy_fn_t    destroy_fn    = monoid->destroy_fn;
        cilk_deallocate_fn_t deallocate_fn = monoid->deallocate_fn;
	
        // destroy_fn((void*) el->hb, el->view);
        deallocate_fn((void*) el->hb, el->view);
    }

    el->view = 0;
}

bool elem_is_leftmost(elem *const el)
{
    // implementation uses the address of the leftmost view as the key, so if
    // key == view, then this element refers to the leftmost view.
    return el->key == el->view;
}

// bucket functions

static size_t sizeof_bucket(size_t nmax)
{
    bucket *b = 0;
    return (sizeof(*b) + nmax * sizeof(b->el[0]));
}

static bucket *alloc_bucket(__cilkrts_worker *w, size_t nmax)
{
    bucket *b = (bucket *)
        cilk_internal_malloc(w, sizeof_bucket(nmax), IM_REDUCER_MAP);
    b->nmax = nmax;
    return b;
}

static void free_bucket(__cilkrts_worker *w, bucket **bp)
{
    bucket *b = *bp;
    if (b) {
        cilk_internal_free(w, b, sizeof_bucket(b->nmax), IM_REDUCER_MAP);
        *bp = 0;
    }
}

/* round up nmax to fill a memory allocator block completely */
static size_t roundup(size_t nmax)
{
    size_t sz = sizeof_bucket(nmax);

    /* round up size to a full malloc block */
    // This should be the same thing!
    //sz = __cilkrts_frame_malloc_roundup(sz);
    if (sz < 64) sz = 64;
    else if (sz < 2048) {
        sz--;
        sz |= sz >> 1;
        sz |= sz >> 2;
        sz |= sz >> 4;
        sz |= sz >> 8;
        sz |= sz >> 16;
        sz++;
    }
    /* invert sizeof_bucket() */
    nmax = ((sz - sizeof(bucket)) / sizeof(elem));
     
    return nmax;
}

// cilkred_map functions

static bool is_power_of_2(size_t n)
{
    return (n & (n - 1)) == 0;
}

void cilkred_map_make_buckets(cilkred_map * this_map,
                              __cilkrts_worker *w, 
                              size_t new_nbuckets) {     
    this_map->nbuckets = new_nbuckets;

    CILK_ASSERT(w, is_power_of_2(this_map->nbuckets));
    bucket **new_buckets = (bucket **)
        cilk_internal_malloc(w, this_map->nbuckets * sizeof(*(this_map->buckets)), IM_REDUCER_MAP);

    for (size_t i = 0; i < new_nbuckets; ++i)
        new_buckets[i] = 0;
    this_map->buckets = new_buckets;
    this_map->nelem = 0;
}

static void free_buckets(__cilkrts_worker  *w, 
                         bucket           **buckets,
                         size_t             nbuckets)
{
    size_t i;

    for (i = 0; i < nbuckets; ++i)
        free_bucket(w, buckets + i);

    cilk_internal_free(w, buckets, nbuckets * sizeof(*buckets), IM_REDUCER_MAP);
}

static size_t minsz(size_t nelem)
{
    return 1U + nelem + nelem / 8U;
}

static size_t nextsz(size_t nelem)
{
    return 2 * nelem;
}

bool cilkred_map_need_rehash_p(cilkred_map *const this_map)
{
    return minsz(this_map->nelem) > this_map->nbuckets;
}

/* debugging support: check consistency of a reducer map */
void cilkred_map_check(cilkred_map * this_map, bool allow_null_view)
{
    size_t count = 0;

    CILK_ASSERT_G(this_map->buckets);
    for (size_t i = 0; i < this_map->nbuckets; ++i) {
        bucket *b = this_map->buckets[i];
        if (b) 
            for (elem *el = b->el; el->key; ++el) {
                CILK_ASSERT_G(allow_null_view || el->view);
                ++count;
            }
    }
    CILK_ASSERT_G(this_map->nelem == count);
    /*global_reducer_map::check();*/
}             

/* grow bucket by one element, reallocating bucket if necessary */
elem *cilkred_map_grow(__cilkrts_worker *w, 
                       bucket          **bp)
{
    size_t i, nmax, nnmax;
    bucket *b, *nb;

    //printf("(cilkred_map_grow) w %p; bp %p\n", (void *)w, (void *)bp);

    b = *bp;
    //printf("(cilkred_map_grow) b %p\n", (void *)b);

    if (b) {
        //printf("(cilkred_map_grow) b->nmax %d\n", b->nmax);
        nmax = b->nmax;
        /* find empty element if any */
        for (i = 0; i < nmax; ++i) 
            if (b->el[i].key == 0) 
                return &(b->el[i]);
        /* do not use the last one even if empty */
    } else {
        nmax = 0;
    }

    //printf("(cilkred_map_grow) nmax %d\n", nmax);


    //CILK_ASSERT(w, w == __cilkrts_get_tls_worker());
    /* allocate a new bucket */
    nnmax = roundup(2 * nmax);
    //printf("(cilkred_map_grow) nnmax %d\n", nnmax);
    nb = alloc_bucket(w, nnmax);


    /* copy old bucket into new */
    for (i = 0; i < nmax; ++i)
        nb->el[i] = b->el[i];
     
    free_bucket(w, bp);
    *bp = nb;

    /* zero out extra elements */
    for (; i < nnmax; ++i)
        nb->el[i].key = 0;

    /* zero out the last one */
    nb->el[i].key = 0;
  
    return &(nb->el[nmax]);
}

elem *cilkred_map_insert_no_rehash(cilkred_map * this_map,
                                   __cilkrts_worker *w,
                                   void *key,
                                   __cilkrts_hyperobject_base *hb,
                                   void *view)
{
    
    CILK_ASSERT(w, (w == 0 && this_map->g == 0) || w->g == this_map->g);
    CILK_ASSERT(w, key != 0);
    CILK_ASSERT(w, view != 0);
	    
    elem *el = cilkred_map_grow(w, &(this_map->buckets[hashfun(this_map, key)]));

    el->key = key;
    el->hb  = hb;
    el->view = view;
    ++(this_map->nelem);

    return el;
}

void cilkred_map_rehash(cilkred_map * this_map, __cilkrts_worker *w)
{
    CILK_ASSERT(w, (w == 0 && this_map->g == 0) || w->g == this_map->g);
    
    size_t onbuckets = this_map->nbuckets;
    size_t onelem = this_map->nelem;
    bucket **obuckets = this_map->buckets;
    size_t i;
    bucket *b;

    cilkred_map_make_buckets(this_map, w, nextsz(this_map->nbuckets));
     
    for (i = 0; i < onbuckets; ++i) {
        b = obuckets[i];
        if (b) {
            elem *oel;
            for (oel = b->el; oel->key; ++oel)
                cilkred_map_insert_no_rehash(this_map, w, oel->key, oel->hb, oel->view);
        }
    }

    CILK_ASSERT(w, this_map->nelem == onelem);

    free_buckets(w, obuckets, onbuckets);
}

elem *cilkred_map_rehash_and_insert(cilkred_map * this_map,
                                     __cilkrts_worker           *w,
                                     void                       *key,
                                     __cilkrts_hyperobject_base *hb,
                                     void                       *view)
{

    if (cilkred_map_need_rehash_p(this_map)) 
        cilkred_map_rehash(this_map, w);

    return cilkred_map_insert_no_rehash(this_map, w, key, hb, view);
}


elem *cilkred_map_lookup(cilkred_map * this_map, void *key)
{
    bucket *b = this_map->buckets[hashfun(this_map, key)];

    if (b) {
        elem *el;
        for (el = b->el; el->key; ++el) {
            if (el->key == key) {
                CILK_ASSERT_G(el->view);
                return el;
            }
        }
    }

    return 0;
}

cilkred_map * cilkred_map_make_map(__cilkrts_worker *w)
{
    CILK_ASSERT_G(w);

    cilkred_map *h;
    size_t nbuckets = 1; /* default value */
    
    h = (cilkred_map *)cilk_internal_malloc(w, sizeof(*h), IM_REDUCER_MAP);


    h->g = w ? w->g : 0;
    cilkred_map_make_buckets(h, w, nbuckets);
    h->merging = false;
    h->is_leftmost = false;

    return h;
}

/* Destroy a reducer map.  The map must have been allocated
   from the worker's global context and should have been
   allocated from the same worker. */
void cilkred_map_destroy_map(__cilkrts_worker *w, cilkred_map *h)
{
    CILK_ASSERT_G((w == 0 && h->g == 0) || w->g == h->g);
    // Not true for deinit
    // CILK_ASSERT(w, w == __cilkrts_get_tls_worker());

    /* the reducer map is allowed to contain el->view == NULL here (and
       only here).  We set el->view == NULL only when we know that the
       map will be destroyed immediately afterwards. */
    DBG cilkred_map_check(h, /*allow_null_view=*/true);

    bucket *b;
    size_t i;

    for (i = 0; i < h->nbuckets; ++i) {
        b = h->buckets[i];
        if (b) {
            elem *el;
            for (el = b->el; el->key; ++el) {
                if (el->view)
                    elem_destroy(el);
            }
        }
    }

    free_buckets(w, h->buckets, h->nbuckets);
    
    cilk_internal_free(w, h, sizeof(*h), IM_REDUCER_MAP);
}

/* Set the specified reducer map as the leftmost map if is_leftmost is true,
   otherwise, set it to not be the leftmost map. */
void __cilkrts_set_leftmost_reducer_map(cilkred_map *h, int is_leftmost)
{
    h->is_leftmost = is_leftmost;
}


__cilkrts_worker* cilkred_map_merge(cilkred_map * this_map,
                     __cilkrts_worker *w,
				     cilkred_map *other_map,
				     enum merge_kind kind)
{
    // Remember the current stack frame.
    __cilkrts_stack_frame *current_sf = w->current_stack_frame;
    this_map->merging = true;
    other_map->merging = true;

    // Merging to the leftmost view is a special case because every leftmost
    // element must be initialized before the merge.
    CILK_ASSERT(w, !other_map->is_leftmost /* || kind == MERGE_UNORDERED */);
    bool merge_to_leftmost = (this_map->is_leftmost
                              /* && !other_map->is_leftmost */);

    DBG cilkred_map_check(this_map, /*allow_null_view=*/false);
    DBG cilkred_map_check(other_map, /*allow_null_view=*/false);

    for (size_t i = 0; i < other_map->nbuckets; ++i) {
        bucket *b = other_map->buckets[i];
        if (b) {
            for (elem *other_el = b->el; other_el->key; ++other_el) {
                /* Steal the value from the other map, which will be
                   destroyed at the end of this operation. */
                void *other_view = other_el->view;
                CILK_ASSERT(w, other_view);

                void *key = other_el->key;
		        __cilkrts_hyperobject_base *hb = other_el->hb;
                elem *this_el = cilkred_map_lookup(this_map, key);

                if (this_el == 0 && merge_to_leftmost) {
                    /* Initialize leftmost view before merging. */
                    void* leftmost = get_leftmost_view(key);
                    // leftmost == other_view can be true if the initial view
                    // was created in other than the leftmost strand of the
                    // spawn tree, but then made visible to subsequent strands
                    // (E.g., the reducer was allocated on the heap and the
                    // pointer was returned to the caller.)  In such cases,
                    // parallel semantics says that syncing with earlier
                    // strands will always result in 'this_el' being null,
                    // thus propagating the initial view up the spawn tree
                    // until it reaches the leftmost strand.  When synching
                    // with the leftmost strand, leftmost == other_view will be
                    // true and we must avoid reducing the initial view with
                    // itself.
                    if (leftmost != other_view)
                        this_el = cilkred_map_rehash_and_insert(this_map, w, key, hb, leftmost);
                }

                if (this_el == 0) {
                    /* move object from other map into this one */
                    cilkred_map_rehash_and_insert(this_map, w, key, hb, other_view);
                    other_el->view = 0;
                    continue; /* No element-level merge necessary */
                }

                /* The same key is present in both maps with values
                   A and B.  Three choices: fail, A OP B, B OP A. */
                switch (kind)
                {
                case MERGE_UNORDERED:
                    cilkrts_bug(w, "TLS Reducer race");
                    break;
                case MERGE_INTO_RIGHT:
                    /* Swap elements in order to preserve object
                       identity */
                    other_el->view = this_el->view;
                    this_el->view = other_view;
                    /* FALL THROUGH */
                case MERGE_INTO_LEFT: {
                    /* Stealing should be disabled during reduce
                       (even if force-reduce is enabled). */

                    {			
                    CILK_ASSERT(w, current_sf->worker == w);
                    CILK_ASSERT(w, w->current_stack_frame == current_sf);

                    /* TBD: if reduce throws an exception we need to stop it
                    here. */
                    hb->__c_monoid.reduce_fn((void*)hb,
                                this_el->view,
                                other_el->view);
                    w = current_sf->worker;
                    }

                  } break;
                }
            }
        }
    }
    this_map->is_leftmost = this_map->is_leftmost || other_map->is_leftmost;
    this_map->merging = false;
    other_map->merging = false;
    CILK_ASSERT(w, w == __cilkrts_get_tls_worker());
    cilkred_map_destroy_map(w, other_map);
    return w;
}