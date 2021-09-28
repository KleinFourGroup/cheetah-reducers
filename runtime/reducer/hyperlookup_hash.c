#if INLINE_TLS && !INLINE_ALL_TLS
extern __thread __cilkrts_worker *tls_worker;
#endif

#if INLINE_FULL_LOOKUP
__attribute__((always_inline))
#endif
void* __cilkrts_hyper_lookup(__cilkrts_hyperobject_base *hb)
{
#if INLINE_TLS
    __cilkrts_worker *w = tls_worker;
#else
    __cilkrts_worker *w = __cilkrts_get_tls_worker();
#endif
    void* key = get_hyperobject_key(hb);

    if (! w)
        return get_leftmost_view(key);

#if !PRUNE_BRANCHES
    if (__builtin_expect(w->g->options.force_reduce, 0)) {
#if SLOWPATH_LOOKUP || INLINE_FULL_LOOKUP
        inline_promote_own_deque(w);
#else
        promote_own_deque(w);
#endif
    }
#endif

    cilkred_map* h = w->reducer_map;

    if (__builtin_expect(!h, 0)) {
	    h = install_new_reducer_map(w);
    }

#if !PRUNE_BRANCHES
    if (h->merging)
        inline_cilkrts_bug(w, "User error: hyperobject used by another hyperobject");
#endif

#if !INLINE_MAP_LOOKUP
    elem* el = cilkred_map_lookup(h, key);
#else
    elem* el = 0;

    bucket *b = h->buckets[hashfun(h, key)];

    if (b) {
        for (el = b->el; el->key; ++el) {
            if (el->key == key) {
                CILK_ASSERT_G(el->view);
                break;
            }
        }
        if (! el->key) el = 0;
    }
#endif

    if (! el) {
        /* lookup failed; insert a new default element */
        void *rep;

        {
            if (h->is_leftmost)
            {
                // This special case is called only if the reducer was not
                // registered using __cilkrts_hyper_create, e.g., if this is a
                // C reducer in global scope or if there is no bound worker.
                rep = get_leftmost_view(key);
            }
            else
            {
                rep = hb->__c_monoid.allocate_fn((void*)hb,
						 hb->__view_size);
                // TBD: Handle exception on identity function
                hb->__c_monoid.identity_fn((void*)hb, rep);
            }
        }
        el = cilkred_map_rehash_and_insert(h, w, key, hb, rep);
    }

    return el->view;
}