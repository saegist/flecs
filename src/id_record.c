#include "private_api.h"

static
ecs_id_record_elem_t* flecs_id_record_elem(
    ecs_id_record_t *head,
    ecs_id_record_elem_t *list,
    ecs_id_record_t *idr)
{
    return ECS_OFFSET(idr, (uintptr_t)list - (uintptr_t)head);
}

static
void flecs_id_record_elem_insert(
    ecs_id_record_t *head,
    ecs_id_record_t *idr,
    ecs_id_record_elem_t *elem)
{
    ecs_id_record_elem_t *head_elem = flecs_id_record_elem(idr, elem, head);
    ecs_id_record_t *cur = head_elem->next;
    elem->next = cur;
    elem->prev = head;
    if (cur) {
        ecs_id_record_elem_t *cur_elem = flecs_id_record_elem(idr, elem, cur);
        cur_elem->prev = idr;
    }
    head_elem->next = idr;
}

static
void flecs_id_record_elem_remove(
    ecs_id_record_t *idr,
    ecs_id_record_elem_t *elem)
{
    ecs_id_record_t *prev = elem->prev;
    ecs_id_record_t *next = elem->next;
    ecs_assert(prev != NULL, ECS_INTERNAL_ERROR, NULL);

    ecs_id_record_elem_t *prev_elem = flecs_id_record_elem(idr, elem, prev);
    prev_elem->next = next;
    if (next) {
        ecs_id_record_elem_t *next_elem = flecs_id_record_elem(idr, elem, next);
        next_elem->prev = prev;
    }
}

static
void flecs_insert_id_elem(
    ecs_world_t *world,
    ecs_id_record_t *idr,
    ecs_id_t wildcard)
{
    ecs_assert(ecs_id_is_wildcard(wildcard), ECS_INTERNAL_ERROR, NULL);
    ecs_id_record_t *widr = flecs_ensure_id_record(world, wildcard);
    ecs_assert(widr != NULL, ECS_INTERNAL_ERROR, NULL);

    if (ECS_PAIR_SECOND(wildcard) == EcsWildcard) {
        ecs_assert(ECS_PAIR_FIRST(wildcard) != EcsWildcard, 
            ECS_INTERNAL_ERROR, NULL);
        flecs_id_record_elem_insert(widr, idr, &idr->first);
    } else {
        ecs_assert(ECS_PAIR_FIRST(wildcard) == EcsWildcard, 
            ECS_INTERNAL_ERROR, NULL);
        flecs_id_record_elem_insert(widr, idr, &idr->second);

        if (idr->flags & EcsIdAcyclic) {
            flecs_id_record_elem_insert(widr, idr, &idr->acyclic);
        }
    }
}

static
void flecs_remove_id_elem(
    ecs_world_t *world,
    ecs_id_record_t *idr,
    ecs_id_t wildcard)
{
    ecs_assert(ecs_id_is_wildcard(wildcard), ECS_INTERNAL_ERROR, NULL);
    ecs_id_record_t *widr = flecs_get_id_record(world, wildcard);
    if (!widr) {
        return;
    }

    if (ECS_PAIR_SECOND(wildcard) == EcsWildcard) {
        ecs_assert(ECS_PAIR_FIRST(wildcard) != EcsWildcard, 
            ECS_INTERNAL_ERROR, NULL);
        flecs_id_record_elem_remove(idr, &idr->first);
    } else {
        ecs_assert(ECS_PAIR_FIRST(wildcard) == EcsWildcard, 
            ECS_INTERNAL_ERROR, NULL);
        flecs_id_record_elem_remove(idr, &idr->second);

        if (idr->flags & EcsIdAcyclic) {
            flecs_id_record_elem_remove(idr, &idr->acyclic);
        }
    }
}

static
void flecs_id_reachable_revalidate_target(
    ecs_world_t *world,
    ecs_id_record_t *idr);

static
bool flecs_id_reachable_valid(
    ecs_id_record_t *idr)
{
    return idr->reachable_changed.prev == NULL;
}

static
void flecs_id_reachable_cache_clean(
    ecs_reachable_ids_t *cache)
{
    if (cache->table) {
        cache->table = NULL;
    }
    if (cache->sources) {
        ecs_os_free(cache->sources);
        cache->sources = NULL;
    }
}

static
ecs_id_record_t* flecs_new_id_record(
    ecs_world_t *world,
    ecs_id_t id)
{
    ecs_id_record_t *idr = ecs_os_calloc_t(ecs_id_record_t);
    ecs_table_cache_init(&idr->cache);

    idr->id = id;

    bool is_wildcard = ecs_id_is_wildcard(id);

    ecs_entity_t rel = 0, obj = 0, role = id & ECS_ROLE_MASK;
    if (role == ECS_PAIR) {
        rel = ecs_pair_first(world, id);
        ecs_assert(rel != 0, ECS_INTERNAL_ERROR, NULL);

        /* Relation object can be 0, as tables without a ChildOf relation are
         * added to the (ChildOf, 0) id record */
        obj = ECS_PAIR_SECOND(id);
        if (obj) {
            obj = ecs_get_alive(world, obj);
            ecs_assert(obj != 0, ECS_INTERNAL_ERROR, NULL);
        }

        /* Check constraints */
        if (obj && !ecs_id_is_wildcard(obj)) {
            ecs_entity_t oneof = flecs_get_oneof(world, rel);
            ecs_check( !oneof || ecs_has_pair(world, obj, EcsChildOf, oneof),
                ECS_CONSTRAINT_VIOLATED, NULL);
            (void)oneof;
        }

        if (!is_wildcard) {
            /* Inherit flags from (relation, *) record */
            ecs_id_record_t *idr_r = flecs_get_id_record(
                world, ecs_pair(rel, EcsWildcard));
            if (idr_r) {
                idr->flags = idr_r->flags;
            }

            /* If pair is not a wildcard, append it to wildcard lists. These 
             * allow for quickly enumerating all relations for an object, or all 
             * objecs for a relation. */
            flecs_insert_id_elem(world, idr, ecs_pair(rel, EcsWildcard));
            flecs_insert_id_elem(world, idr, ecs_pair(EcsWildcard, obj));
        }
    } else {
        rel = id & ECS_COMPONENT_MASK;
        rel = ecs_get_alive(world, rel);
        ecs_assert(rel != 0, ECS_INTERNAL_ERROR, NULL);
    }

    /* Initialize type info if id is not a tag */
    if (!is_wildcard && (!role || (role == ECS_PAIR))) {
        if (!(idr->flags & EcsIdTag)) {
            const ecs_type_info_t *ti = flecs_get_type_info(world, rel);
            if (!ti && obj) {
                ti = flecs_get_type_info(world, obj);
            }
            idr->type_info = ti;
        }
    }

    /* Mark entities that are used as component/pair ids. When a tracked
     * entity is deleted, cleanup policies are applied so that the store
     * won't contain any tables with deleted ids. */

    /* Flag for OnDelete policies */
    flecs_add_flag(world, rel, EcsEntityObservedId);
    if (obj) {
        /* Flag for OnDeleteObject policies */
        flecs_add_flag(world, obj, EcsEntityObservedObject);
        if (ecs_has_id(world, rel, EcsAcyclic)) {
            /* Flag used to determine if object should be traversed when
             * propagating events or with super/subset queries */
            flecs_add_flag(world, obj, EcsEntityObservedAcyclic);
        }
    }

    if (ecs_should_log_1()) {
        char *id_str = ecs_id_str(world, id);
        ecs_dbg_1("#[green]id#[normal] %s #[green]created", id_str);
        ecs_os_free(id_str);
    }

    /* Update counters */
    world->info.id_create_total ++;

    if (!is_wildcard) {
        world->info.id_count ++;

        if (idr->type_info) {
            world->info.component_id_count ++;
        } else {
            world->info.tag_id_count ++;
        }

        if (ECS_HAS_ROLE(id, PAIR)) {
            world->info.pair_id_count ++;
        }
    } else {
        world->info.wildcard_id_count ++;
    }

    return idr;
error:
    return NULL;
}

static
void flecs_free_id_record_reachable(
    ecs_id_record_t *idr)
{
    /* Free reachable id caches */
    if (idr->reachable) {
        ecs_map_iter_t it = ecs_map_iter(idr->reachable);
        ecs_reachable_ids_t *elem;
        while ((elem = ecs_map_next(&it, ecs_reachable_ids_t, 0))) {
            flecs_id_reachable_cache_clean(elem);
        }
        ecs_map_free(idr->reachable);
        idr->reachable = NULL;
    }
}

/* Cleanup id index */
static
bool flecs_free_id_record(
    ecs_world_t *world,
    ecs_id_t id,
    ecs_id_record_t *idr)
{
    ecs_assert(world != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(id != 0, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(idr != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_poly_assert(world, ecs_world_t);
    (void)id;

    /* Force the empty table administration to be consistent if the non-empty
     * list of the id record has elements */
    if (ecs_table_cache_count(&idr->cache)) {
        ecs_run_aperiodic(world, EcsAperiodicEmptyTableEvents);
    }

    /* If there are still tables in the non-empty list they're really not empty.
     * We can't free the record yet. */
    if (ecs_table_cache_count(&idr->cache)) {
        return false;
    }

    /* If id record contains no more empty tables, free it */
    if (ecs_table_cache_empty_count(&idr->cache) == 0) {
        if (ecs_should_log_1()) {
            char *id_str = ecs_id_str(world, id);
            ecs_dbg_1("#[green]id#[normal] %s #[red]deleted", id_str);
            ecs_os_free(id_str);
        }

        if (ECS_HAS_ROLE(id, PAIR)) {
            if (!ecs_id_is_wildcard(id)) {
                ecs_entity_t rel = ecs_pair_first(world, id);
                ecs_entity_t obj = ECS_PAIR_SECOND(id);
                flecs_remove_id_elem(world, idr, ecs_pair(rel, EcsWildcard));
                flecs_remove_id_elem(world, idr, ecs_pair(EcsWildcard, obj));
            }
        }

        /* Update counters */
        world->info.id_delete_total ++;

        if (!ecs_id_is_wildcard(id)) {
            world->info.id_count --;

            if (ECS_HAS_ROLE(id, PAIR)) {
                world->info.pair_id_count --;
            }

            if (idr->type_info) {
                world->info.component_id_count --;
            } else {
                world->info.tag_id_count --;
            }
        } else {
            world->info.wildcard_id_count --;
        }

        ecs_table_cache_fini(&idr->cache);
        flecs_name_index_free(idr->name_index);
        ecs_os_free(idr);
        return true;
    }

    /* Delete empty tables */
    ecs_table_cache_iter_t cache_it;
    flecs_table_cache_empty_iter(&idr->cache, &cache_it);

    const ecs_table_record_t *tr;
    int32_t unreleased_count = 0;
    while ((tr = flecs_table_cache_next(&cache_it, ecs_table_record_t))) {
        if (!flecs_table_release(world, tr->hdr.table)) {
            /* Releasing the table did not free it, which means that something
             * is keeping the table alive. Cleanup of the id record will happen
             * when the last reference(s) to the table are released */
            unreleased_count ++;
        }
    }
    if (unreleased_count) {
        return false;
    }

    /* If all tables were deleted for this id record, the last deleted table
     * should have removed the record from the world. */
    ecs_assert(flecs_get_id_record(world, id) == NULL, 
        ECS_INTERNAL_ERROR, NULL);

    return true;
}

ecs_id_record_t* flecs_ensure_id_record(
    ecs_world_t *world,
    ecs_id_t id)
{
    ecs_poly_assert(world, ecs_world_t);

    ecs_id_record_t **idr_ptr = ecs_map_ensure(&world->id_index, 
        ecs_id_record_t*, ecs_strip_generation(id));
    ecs_id_record_t *idr = idr_ptr[0];
    if (!idr) {
        idr = flecs_new_id_record(world, id);
        idr_ptr = ecs_map_get(&world->id_index, 
            ecs_id_record_t*, ecs_strip_generation(id));
        ecs_assert(idr_ptr != NULL, ECS_INTERNAL_ERROR, NULL);
        idr_ptr[0] = idr;
    }

    return idr;
}

ecs_id_record_t* flecs_get_id_record(
    const ecs_world_t *world,
    ecs_id_t id)
{
    ecs_poly_assert(world, ecs_world_t);
    return ecs_map_get_ptr(&world->id_index, ecs_id_record_t*,
        ecs_strip_generation(id));
}

void flecs_remove_id_record(
    ecs_world_t *world,
    ecs_id_t id,
    ecs_id_record_t *idr)
{
    ecs_poly_assert(world, ecs_world_t);

    /* Free id record resources */
    if (flecs_free_id_record(world, id, idr)) {
        /* Remove record from world index */
        ecs_map_remove(&world->id_index, ecs_strip_generation(id));
    }
}

void flecs_clear_id_record(
    ecs_world_t *world,
    ecs_id_t id,
    ecs_id_record_t *idr)
{
    ecs_poly_assert(world, ecs_world_t);
    
    if (world->is_fini) {
        return;
    }

    ecs_table_cache_fini_delete_all(world, &idr->cache);

    flecs_remove_id_record(world, id, idr);
}

bool flecs_set_type_info_for_id_record(
    ecs_world_t *world,
    ecs_id_record_t *idr,
    const ecs_type_info_t *ti)
{
    if (ti) {
        if (!idr->type_info) {
            world->info.tag_id_count --;
            world->info.component_id_count ++;
        }
    } else {
        if (idr->type_info) {
            world->info.tag_id_count ++;
            world->info.component_id_count --;
        }
    }

    bool changed = idr->type_info != ti;
    idr->type_info = ti;
    return changed;
}

ecs_hashmap_t* flecs_ensure_id_name_index(
    ecs_world_t *world,
    ecs_id_t id)
{
    ecs_poly_assert(world, ecs_world_t);

    ecs_id_record_t *idr = flecs_get_id_record(world, id);
    ecs_assert(idr != NULL, ECS_INTERNAL_ERROR, NULL);

    ecs_hashmap_t *map = idr->name_index;
    if (!map) {
        map = idr->name_index = flecs_name_index_new();
    }

    return map;
}

ecs_hashmap_t* flecs_get_id_name_index(
    const ecs_world_t *world,
    ecs_id_t id)
{
    ecs_poly_assert(world, ecs_world_t);

    ecs_id_record_t *idr = flecs_get_id_record(world, id);
    if (!idr) {
        return NULL;
    }

    return idr->name_index;
}

ecs_table_record_t* flecs_get_table_record(
    const ecs_world_t *world,
    const ecs_table_t *table,
    ecs_id_t id)
{
    ecs_poly_assert(world, ecs_world_t);

    ecs_id_record_t* idr = flecs_get_id_record(world, id);
    if (!idr) {
        return NULL;
    }

    return (ecs_table_record_t*)ecs_table_cache_get(&idr->cache, table);
}

const ecs_table_record_t* flecs_id_record_table(
    ecs_id_record_t *idr,
    ecs_table_t *table)
{
    if (!idr) {
        return NULL;
    }
    return (ecs_table_record_t*)ecs_table_cache_get(&idr->cache, table);
}

void flecs_register_for_id_record(
    ecs_world_t *world,
    ecs_id_t id,
    const ecs_table_t *table,
    ecs_table_record_t *tr)
{
    ecs_poly_assert(world, ecs_world_t);

    ecs_id_record_t *idr = flecs_ensure_id_record(world, id);
    if (!idr) {
        return;
    }

    ecs_table_cache_insert(&idr->cache, table, &tr->hdr);
}

void flecs_fini_id_records(
    ecs_world_t *world)
{
    ecs_map_iter_t it = ecs_map_iter(&world->id_index);
    ecs_id_record_t *idr;
    ecs_map_key_t key;
    while ((idr = ecs_map_next_ptr(&it, ecs_id_record_t*, &key))) {
        flecs_free_id_record(world, key, idr);
    }

    ecs_map_fini(&world->id_index);
    flecs_sparse_free(world->pending_tables);
    flecs_sparse_free(world->pending_buffer);
}

ecs_id_record_t* flecs_table_iter(
    ecs_world_t *world,
    ecs_id_t id,
    ecs_table_cache_iter_t *out)
{
    ecs_id_record_t *idr = flecs_get_id_record(world, id);
    if (!idr) {
        return NULL;
    }

    flecs_process_pending_tables(world);
    ecs_assert( flecs_sparse_count(world->pending_tables) == 0, 
        ECS_INTERNAL_ERROR, NULL);

    flecs_table_cache_iter(&idr->cache, out);
    return idr;
}

ecs_id_record_t* flecs_empty_table_iter(
    ecs_world_t *world,
    ecs_id_t id,
    ecs_table_cache_iter_t *out)
{
    ecs_id_record_t *idr = flecs_get_id_record(world, id);
    if (!idr) {
        return NULL;
    }

    flecs_process_pending_tables(world);
    ecs_assert( flecs_sparse_count(world->pending_tables) == 0, 
        ECS_INTERNAL_ERROR, NULL);

    flecs_table_cache_empty_iter(&idr->cache, out);
    return idr;
}

static
void flecs_id_reachable_cache_add(
    ecs_map_t *map,
    ecs_table_t *table,
    ecs_entity_t target,
    ecs_id_record_t *idr)
{
    ecs_entity_t r = ECS_PAIR_FIRST(idr->id);
    ecs_id_t *ids = ecs_vector_first(table->type, ecs_id_t);
    int32_t i, count = ecs_vector_count(table->type);

    for (i = 0; i < count; i ++) {
        ecs_id_t id = ids[i];
        if (ecs_id_is_pair(id)) {
            if (ECS_PAIR_FIRST(id) == r) {
                /* Don't add the relation instances itself as this would bloat
                 * the cache for deeply nested trees */
                continue;
            }
        }
        ecs_entity_t *source = ecs_map_ensure(map, ecs_entity_t, id);
        if (!*source) {
            *source = target;
        }
    }

    /* Add ids to cache for nested instances of relation R */
    ecs_table_t *acyclic_table = table->acyclic_table;
    if (!acyclic_table) {
        return;
    }

    if (!idr->reachable) {
        return;
    }

    ecs_reachable_ids_t *cache = ecs_map_get(
        idr->reachable, ecs_reachable_ids_t, acyclic_table->id);
    if (!cache) {
        return;
    }

    ids = ecs_vector_first(cache->table->type, ecs_id_t);
    count = ecs_vector_count(cache->table->type);
    for (i = 0; i < count; i ++) {
        ecs_entity_t *source = ecs_map_ensure(map, ecs_entity_t, ids[i]);
        if (!*source) {
            *source = cache->sources[i];
        }
    }
}

static
void flecs_id_reachable_cache_build(
    ecs_world_t *world,
    ecs_reachable_ids_t *cache,
    ecs_map_t *map)
{
    flecs_id_reachable_cache_clean(cache);

    /* Create sorted list of ids so we can create table for it */
    int32_t i = 0, count = ecs_map_count(map);
    if (!count) {
        return;
    }

    ecs_vector_t *idv = ecs_vector_new(ecs_id_t, count);
    ecs_vector_set_count(&idv, ecs_id_t, count);
    ecs_id_t *ids = ecs_vector_first(idv, ecs_id_t);
    ecs_entity_t *sources = ecs_os_malloc_n(ecs_entity_t, count);

    ecs_map_iter_t mit = ecs_map_iter(map);
    uint64_t key;
    ecs_entity_t *elem;
    while ((elem = ecs_map_next(&mit, ecs_entity_t, &key))) {
        ids[i] = key;
        sources[i] = *elem;
        i ++;
    }

    ecs_id_t id_tmp;
    ecs_entity_t source_tmp;

    /* Sort ids so we can use the array to create a table for the cache. Use the
     * QSORT macro as it lets us sort both id & source arrays. */
    #define LESS(i, j) (ids[i] < ids[j])
    #define SWAP(i, j) ( \
        id_tmp = ids[i], \
        ids[i] = ids[j], \
        ids[j] = id_tmp, \
        source_tmp = sources[i], \
        sources[i] = sources[j], \
        sources[j] = source_tmp)

    QSORT(count, LESS, SWAP);

    #undef SWAP
    #undef LESS

    /* Create table for cache with reachable ids. Using a table vs. a map has
     * advantages, as the list of ids is only stored once. The reason we create
     * the map first vs. finding a table by traversing the table graph is that
     * with this approach we only create one table, whereas traversing the graph
     * could (lazily) create as many tables as there are reachable ids */
    ecs_ids_t ids_array = { .array = ids, .count = count };
    cache->table = flecs_table_find_or_create_w_vector(world, &ids_array, idv);
    cache->sources = sources;
}

static
void flecs_id_reachable_revalidate_cache(
    ecs_world_t *world,
    ecs_reachable_ids_t *cache,
    ecs_table_t *acyclic_table,
    ecs_id_record_t *idr)
{
    ecs_map_t map = {0};
    ecs_map_init(&map, ecs_entity_t, 1);

    const ecs_table_record_t *tr = ecs_table_cache_get(
        &idr->cache, acyclic_table);
    ecs_assert(tr != NULL, ECS_INTERNAL_ERROR, NULL);

    ecs_id_t *ids = ecs_vector_first(acyclic_table->type, ecs_id_t);
    int32_t i = tr->column, end = i + tr->count;
    for (; i < end; i ++) {
        ecs_id_t id = ids[i];
        ecs_entity_t target = ecs_pair_second(world, id);
        ecs_record_t *record = ecs_eis_get(world, target);
        ecs_table_t *table;
        if (record && (table = record->table)) {
            flecs_id_reachable_cache_add(&map, table, target, idr);
        }
    }

    /* Build the cache from the map */
    flecs_id_reachable_cache_build(world, cache, &map);

    ecs_map_fini(&map);
}

static
void flecs_id_reachable_revalidate_target(
    ecs_world_t *world,
    ecs_id_record_t *idr)
{
    ecs_assert(ECS_PAIR_FIRST(idr->id) == EcsWildcard, 
        ECS_INTERNAL_ERROR, NULL);

    /* Find all acyclic relations R for entity in id record (*, E) */
    while ((idr = idr->acyclic.next)) {
        ecs_entity_t r = ecs_pair_first(world, idr->id);

        /* Iterate tables to find all acyclic tables. The reachable cache is
         * only stored on acyclic tables to reduce redundancy */
        ecs_table_cache_iter_t it;
        if (!flecs_table_cache_iter(&idr->cache, &it)) {
            continue; /* id record contains no non-empty tables */
        }

        ecs_table_record_t *tr;
        while ((tr = flecs_table_cache_next(&it, ecs_table_record_t))) {
            ecs_table_t *table = tr->hdr.table;
            ecs_table_t *acyclic_table = table->acyclic_table;
            ecs_assert(acyclic_table != NULL, ECS_INTERNAL_ERROR, NULL);

            /* Find reachable id cache record for relation R and acyclic
             * table in id record (R, *) */
            ecs_id_record_t *idr_r_wildcard = flecs_get_id_record(world,
                ecs_pair(r, EcsWildcard));
            ecs_assert(idr_r_wildcard != NULL, ECS_INTERNAL_ERROR, NULL);

            /* Make sure table index for cache exists on record */
            ecs_map_t *cache_for_tables = idr_r_wildcard->reachable;
            if (!cache_for_tables) {
                cache_for_tables = idr_r_wildcard->reachable = 
                    ecs_map_new(ecs_reachable_ids_t, 1);
            }

            /* Make sure cache for acyclic table exists */
            ecs_reachable_ids_t *cache = ecs_map_ensure(cache_for_tables,
                ecs_reachable_ids_t, acyclic_table->id);
            if (cache->counter != world->reachable_counter) {
                /* Multiple tables can share the same acyclic table. 
                 * Only regenerate cache for acyclic table once */
                cache->counter = world->reachable_counter;

                /* Rebuild cache for acyclic relation instances of R */
                flecs_id_reachable_revalidate_cache(world, cache, 
                    acyclic_table, idr_r_wildcard);

                /* Revalidate the cache for children */
                ecs_vector_t *rsv = table->data.record_ptrs;
                ecs_record_t **rs = ecs_vector_first(rsv, ecs_record_t*);
                int32_t i, count = ecs_vector_count(rsv);
                ecs_entity_t *entities = ecs_vector_first(
                    table->data.entities, ecs_entity_t);
                for (i = 0; i < count; i ++) {
                    ecs_record_t *record = rs[i];
                    if (!record) {
                        continue; /* Unusual, but can happen during bulk new */
                    }

                    /* Traverse children marked with EcsEntityObservedAcyclic
                     * to skip entities not used with acyclic relation */
                    ecs_flags32_t flags = ECS_RECORD_TO_ROW_FLAGS(record->row);
                    if (flags & EcsEntityObservedAcyclic) {
                        ecs_id_record_t *idr_child = flecs_get_id_record(
                            world, ecs_pair(EcsWildcard, entities[i]));
                        flecs_id_reachable_revalidate_target(
                            world, idr_child);
                    }
                }
            }
        }
    }
}

static
bool flecs_id_reachable_has_dirty_parent(
    ecs_world_t *world,
    ecs_entity_t entity)
{
    ecs_record_t *r = ecs_eis_get(world, entity);
    ecs_table_t *table;
    if (!r || !(table = r->table)) {
        return false;
    }

    /* Only check acyclic relations */
    ecs_table_t *acyclic_table = table->acyclic_table;
    if (!acyclic_table) {
        return false;
    }

    ecs_id_t *ids = ecs_vector_first(acyclic_table->type, ecs_id_t);
    int32_t i, count = ecs_vector_count(acyclic_table->type);

    for (i = 0; i < count; i ++) {
        ecs_id_t id = ids[i];
        ecs_entity_t target = ecs_pair_second(world, id);
        ecs_id_record_t *idr = flecs_get_id_record(world, 
            ecs_pair(EcsWildcard, target));
        if (idr->reachable_counter == world->reachable_counter) {
            /* Path to parent was already checked and marked as dirty */
            return true;
        }

        if (idr->reachable_changed.prev) {
            /* If the record is in th dirty list, the parent is dirty */
            return true;
        }
        
        /* Keep searching upwards to find dirty parents */
        if (flecs_id_reachable_has_dirty_parent(world, target)) {
            /* If a dirty parent was found, mark this path as traversed and 
             * dirty so we don't have to keep doing it for subsequent dirty
             * child records. */
            idr->reachable_counter = world->reachable_counter;
            return true;
        }
    }

    return false;
}

void flecs_id_reachable_invalidate(
    ecs_world_t *world,
    ecs_entity_t e)
{
    ecs_poly_assert(world, ecs_world_t);
    ecs_assert(e != 0, ECS_INTERNAL_ERROR, NULL);

    ecs_id_record_t *idr = flecs_get_id_record(world, ecs_pair(EcsWildcard, e));
    if (!idr) {
        /* Entity is not used as second element of pair */
        return;
    }

    /* Append element to list to keep track of for which entities the reachable
     * id cache needs to be updated. Ensure to append only once per entitiy. */
    if (!flecs_id_reachable_valid(idr)) {
        return; /* Element was already marked for invalidation */
    }

    flecs_id_record_elem_insert(world->idr_wildcard_wildcard, idr, 
        &idr->reachable_changed);
}

void flecs_id_reachable_revalidate(
    ecs_world_t *world)
{
    ecs_poly_assert(world, ecs_world_t);

    ecs_id_record_elem_t *head = 
        &world->idr_wildcard_wildcard->reachable_changed;
    ecs_id_record_t *cur, *next = head->next;

    if (!next) {
        return;
    }

    world->reachable_counter ++;
    
    /* First remove invalidated child records of another invalidated entity from 
     * the list. This ensures that each entity is only processed once, as an 
     * invalidated parent already iterates and updates all of its children */
    while ((cur = next)) {
        ecs_assert(cur->reachable_counter != world->reachable_counter, 
            ECS_INTERNAL_ERROR, NULL);
        ecs_entity_t e = ecs_pair_second(world, cur->id);
        ecs_assert(e != 0, ECS_INTERNAL_ERROR, NULL);

        next = cur->reachable_changed.next;

        if (flecs_id_reachable_has_dirty_parent(world, e)) {
            flecs_id_record_elem_remove(cur, &cur->reachable_changed);
        }
    }

    next = head->next;
    head->next = NULL;

    /* Now iterate remaining invalidated records to rebuild reachable caches */
    while ((cur = next)) {
        flecs_id_reachable_revalidate_target(world, cur);

        next = cur->reachable_changed.next;

        /* No need to call remove() as the entire list will be cleaned up */
        cur->reachable_changed.prev = NULL;
        cur->reachable_changed.next = NULL;
    }
}

void flecs_fini_id_reachable(
    ecs_world_t *world)
{
    ecs_table_cache_iter_t it;
    if (flecs_table_iter(world, EcsAcyclic, &it)) {
        /* Find all Acyclic relations */
        ecs_table_record_t *tr;
        while ((tr = flecs_table_cache_next(&it, ecs_table_record_t))) {
            ecs_table_t *table = tr->hdr.table;
            ecs_vector_t *entitiesv = table->data.entities;
            ecs_entity_t *entities = ecs_vector_first(entitiesv, ecs_entity_t);
            int32_t i, count = ecs_vector_count(entitiesv);
            for (i = 0; i < count; i ++) {
                ecs_entity_t r = entities[i];
                ecs_id_record_t *idr = flecs_get_id_record(world, 
                    ecs_pair(r, EcsWildcard));
                if (idr) {
                    flecs_free_id_record_reachable(idr);
                }
            }
        }
    }
}
