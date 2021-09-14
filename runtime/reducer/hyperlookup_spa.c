#if INLINE_TLS && !INLINE_ALL_TLS
extern __thread __cilkrts_worker *tls_worker;
#endif

#if INLINE_FULL_LOOKUP
__attribute__((always_inline))
#endif
void *__cilkrts_hyper_lookup(__cilkrts_hyperobject_base *key) {
#if INLINE_TLS
    __cilkrts_worker *w = tls_worker;
#else
    __cilkrts_worker *w = __cilkrts_get_tls_worker();
#endif
    hyper_id_t id = key->__id_num;

    if (!__builtin_expect(id & HYPER_ID_VALID, HYPER_ID_VALID)) {
        inline_cilkrts_bug(w, "User error: reference to unregistered hyperobject");
    }
    id &= ~HYPER_ID_VALID;

    if (!w) {
        return (char *)key + key->__view_offset;
    }

    /* TODO: If this is the first reference to a reducer created at
       global scope, install the leftmost view. */
#if !PRUNE_BRANCHES
    if (w->g->options.force_reduce) {
#if SLOWPATH_LOOKUP || INLINE_FULL_LOOKUP
        inline_promote_own_deque(w);
#else
        CILK_ASSERT(w, w->g->nworkers == 1);
        promote_own_deque(w);
#endif
    }
#endif

    cilkred_map *h = w->reducer_map;

    if (__builtin_expect(!h, 0)) {
        h = install_new_reducer_map(w);
    }

#if !PRUNE_BRANCHES
    if (h->merging)
        inline_cilkrts_bug(w, "User error: hyperobject used by another hyperobject");
#endif

#if !INLINE_MAP_LOOKUP
    ViewInfo *vinfo = cilkred_map_lookup(h, key);
#else
    ViewInfo *vinfo;
    
    if (id >= h->spa_cap) {
        vinfo = NULL; /* TODO: grow map */
        inline_cilkrts_bug(w, "Error: illegal reducer ID (exceeds SPA cap)");
    } else {
        vinfo = h->vinfo + id;
        if (vinfo->key == NULL) {
        //if (vinfo->key == NULL && vinfo->val == NULL) {
            CILK_ASSERT(w, vinfo->val == NULL);
            vinfo = NULL;
        }
    }
#endif

    if (vinfo == NULL) {
#if SLOWPATH_LOOKUP || INLINE_FULL_LOOKUP
        hyperlookup_slowpath(key, w, h, vinfo, id);
#else
        CILK_ASSERT(w, id < h->spa_cap);
        vinfo = &h->vinfo[id];
        CILK_ASSERT(w, vinfo->key == NULL && vinfo->val == NULL);

        void *val = key->__c_monoid.allocate_fn(key, key->__view_size);
        key->__c_monoid.identity_fn(key, val);

        // allocate space for the val and initialize it to identity
        vinfo->key = key;
        vinfo->val = val;
        cilkred_map_log_id(w, h, id);
#endif
    }
    return vinfo->val;
}