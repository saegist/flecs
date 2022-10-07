#include "private_api.h"

static
ecs_trav_down_t* flecs_trav_table_down_build(
    ecs_world_t *world,
    ecs_trav_cache_t *cache,
    ecs_trav_down_t *dst,
    const ecs_trav_key_t *key,
    ecs_entity_t root,
    ecs_entity_t trav,
    const ecs_table_t *table,
    ecs_id_record_t *idr_with,
    uint32_t generation);

static
void flecs_trav_entity_isa_build(
    ecs_world_t *world,
    ecs_trav_cache_t *cache,
    ecs_trav_down_t *dst,
    const ecs_trav_key_t *key,
    ecs_entity_t root,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_record_t *idr_with,
    uint32_t generation);

static
ecs_trav_cache_t* flecs_trav_cache_get(
    ecs_world_t **world)
{
    ecs_stage_t *stage = flecs_stage_from_world(world);
    ecs_poly_assert(world[0], ecs_world_t);
    ecs_poly_assert(stage, ecs_stage_t);
    if (stage->async || world[0]->stage_count > 1) {
        return &stage->trav;
    } else {
        return &world[0]->stages[0].trav;
    }
}

static
int32_t flecs_trav_compress_trav(
    ecs_trav_cache_t *cache,
    ecs_entity_t trav)
{
    ecs_trav_id_index_t *index = flecs_sparse_ensure(
        &cache->trav_map, ecs_trav_id_index_t, (uint32_t)trav);
    int32_t result = index->value;
    if (!result) {
        result = index->value = flecs_sparse_count(&cache->trav_map);
    }
    return result;
}

static
int32_t flecs_trav_compress_with(
    ecs_trav_cache_t *cache,
    ecs_id_t with)
{
    ecs_trav_id_index_t *index = ecs_map_ensure(
        &cache->with_map, ecs_trav_id_index_t, with);
    int32_t result = index->value;
    if (!result) {
        result = index->value = ecs_map_count(&cache->with_map);
    }
    return result;
}

ecs_trav_key_t flecs_trav_get_key(
    ecs_trav_cache_t *cache,
    uint64_t id,
    ecs_entity_t trav,
    ecs_id_t with)
{
    return (ecs_trav_key_t) {
        .id = id,
        .trav = flecs_trav_compress_trav(cache, trav),
        .with = flecs_trav_compress_with(cache, with)
    };
}

static
ecs_trav_down_t* flecs_trav_ensure_down_cache(
    ecs_trav_cache_t *cache,
    ecs_sparse_t *down_for,
    const ecs_trav_key_t *key)
{
    ecs_trav_down_for_t *df = flecs_sparse_ensure(
        down_for, ecs_trav_down_for_t, key->id);

    int32_t key_trav = key->trav;
    if (key_trav >= ecs_vec_count(&df->trav)) {
        ecs_vec_set_count_t(cache->allocator, &df->trav, 
            ecs_trav_down_for_trav_t, key_trav);
    }

    ecs_trav_down_for_trav_t *dft = ecs_vec_get_t(&df->trav, 
        ecs_trav_down_for_trav_t, key_trav);

    int32_t key_with = key->with;
    if (key_with >= ecs_vec_count(&dft->with)) {
        ecs_vec_set_count_t(cache->allocator, &dft->with, 
            ecs_trav_down_t, key_with);
    }

    return ecs_vec_get_t(&dft->with, ecs_trav_down_t, key_with);
}

static
ecs_trav_up_t* flecs_trav_up_ensure(
    ecs_trav_cache_t *cache,
    ecs_entity_t trav,
    const ecs_table_t *table,
    ecs_id_t with,
    ecs_trav_stats_t *stats)
{
    ecs_id_t id = ecs_pair(trav, table->id);
    ecs_map_init_w_params_if(&cache->up, &cache->trav_up_for_params);
    ecs_trav_up_for_t *df = ecs_map_ensure(&cache->up, ecs_trav_up_for_t, id);
    ecs_map_init_w_params_if(&df->with, &cache->trav_up_params);
    return ecs_map_ensure(&df->with, ecs_trav_up_t, with);
}

static
ecs_trav_down_t* flecs_trav_table_ensure(
    ecs_trav_cache_t *cache,
    const ecs_trav_key_t *key)
{
    return flecs_trav_ensure_down_cache(cache, &cache->table_down, key);
}

static
ecs_trav_down_t* flecs_trav_entity_ensure(
    ecs_trav_cache_t *cache,
    const ecs_trav_key_t *key)
{
    return flecs_trav_ensure_down_cache(cache, &cache->entity_down, key);
}

static
void flecs_trav_append(
    ecs_allocator_t *a,
    ecs_trav_down_t *dst,
    ecs_trav_down_t *src,
    ecs_entity_t root)
{
    if (dst == src) {
        return;
    }

    int32_t i, count = ecs_vec_count(&src->elems);
    if (!count) {
        return;
    }

    ecs_trav_elem_t *src_elems = ecs_vec_first(&src->elems);
    ecs_trav_elem_t *dst_elems = ecs_vec_grow_t(
        a, &dst->elems, ecs_trav_elem_t, count);

    if (src_elems) {
        ecs_os_memcpy_n(dst_elems, src_elems, ecs_trav_elem_t, count);
        for (i = 0; i < count; i ++) {
            ecs_assert(root != 0, ECS_INTERNAL_ERROR, NULL);
            dst_elems[i].source = root;
        }
    }
}

static
void flecs_trav_log_entity(
    ecs_world_t *world,
    ecs_entity_t entity,
    ecs_entity_t trav,
    ecs_id_t id,
    const char *name,
    bool miss)
{
    (void)entity;
    (void)trav;
    (void)name;
    if (ecs_should_log_2()) {
        char *id_str = ecs_id_str(world, id);
        if (miss) {
            ecs_dbg_2("#[magenta]%s cache#[reset]: "
                "#[red]cache miss#[reset] for trav '%s', id '%s', entity '%u'",
                name, ecs_get_name(world, trav), id_str, (uint32_t)entity);
        } else {
            ecs_dbg_2("#[magenta]%s cache#[reset]: "
                "#[green]cache hit#[reset] for trav '%s', id '%s', entity '%u'",
                name, ecs_get_name(world, trav), id_str, (uint32_t)entity);
        }
        ecs_os_free(id_str);
    }
}

static
void flecs_trav_log_table(
    ecs_world_t *world,
    const ecs_table_t *table,
    ecs_entity_t trav,
    ecs_id_t id,
    const char *name,
    bool miss)
{
    (void)table;
    (void)trav;
    (void)name;
    if (ecs_should_log_2()) {
        char *id_str = ecs_id_str(world, id);
        if (miss) {
            ecs_dbg_2("#[magenta]%s cache#[reset]: "
                "#[red]cache miss#[reset] for trav '%s', id '%s', table '%u'",
                name, ecs_get_name(world, trav), id_str, (uint32_t)table->id);
        } else {
            ecs_dbg_2("#[magenta]%s cache#[reset]: "
                "#[green]cache hit#[reset] for trav '%s', id '%s', table '%u'",
                name, ecs_get_name(world, trav), id_str, (uint32_t)table->id);
        }
        ecs_os_free(id_str);
    }
}

static
ecs_trav_down_t* flecs_trav_entity_down_build(
    ecs_world_t *world,
    ecs_trav_cache_t *cache,
    ecs_trav_down_t *dst,
    const ecs_trav_key_t *key,
    ecs_entity_t root,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_record_t *idr_with,
    uint32_t generation)
{
    ecs_trav_down_t *src = flecs_trav_entity_ensure(cache, key);
    ecs_assert(src != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_id_t with = idr_with->id;
    ecs_allocator_t *a = cache->allocator;

    if (!dst) {
        dst = src;
    }

    if (src->generation == generation) {
        flecs_trav_log_entity(world, entity, trav, idr_with->id, "down", false);
        cache->entity_down_stats.cache_hit ++;
        if (dst == src) {
            return dst;
        }
    } else {
        flecs_trav_log_entity(world, entity, trav, idr_with->id, "down", true);
        ecs_log_push_2();
        cache->entity_down_stats.cache_miss ++;
        ecs_vec_reset_t(a, &src->elems, ecs_trav_elem_t);

        flecs_trav_entity_isa_build(
            world, cache, dst, key, root, trav, entity, idr_with, generation);

        ecs_id_record_t *idr = flecs_id_record_get(world, 
            ecs_pair(trav, entity));
        if (!idr) {
            ecs_log_pop_2();
            return dst;
        }

        ecs_table_cache_iter_t it;
        if (flecs_table_cache_all_iter(&idr->cache, &it)) {
            ecs_table_record_t *tr; 
            while ((tr = flecs_table_cache_next(&it, ecs_table_record_t))) {
                ecs_assert(tr->count == 1, ECS_INTERNAL_ERROR, NULL);
                ecs_table_t *table = tr->hdr.table;
                bool leaf = false;
                if (idr_with) {
                    if (flecs_id_record_get_table(idr_with, table) != NULL) {
                        leaf = true;
                    }
                }

                /* If table has multiple instances of relationship, test if id
                 * is reachable by following previous instance */
                int32_t column = tr->column;
                if (column) {
                    ecs_id_t id = table->type.array[column];
                    if (ECS_IS_PAIR(id) && ECS_PAIR_FIRST(id) == trav) {
                        int32_t first = ecs_search_relation(world, table, 0, 
                            with, trav, EcsUp, 0, 0, 0);
                        ecs_assert(first != -1, ECS_INTERNAL_ERROR, NULL);
                        if (first < column) {
                            continue;
                        }
                    }
                }

                ecs_trav_elem_t *elem = ecs_vec_append_t(
                    a, &src->elems, ecs_trav_elem_t);
                elem->table = table;
                if (dst == src) {
                    elem->source = root;
                } else {
                    elem->source = 0;
                }
                elem->leaf = leaf;
            }
        }

        /* Breadth first walk */
        int32_t t, last = ecs_vec_count(&src->elems);
        for (t = 0; t < last; t ++) {
            ecs_trav_elem_t *elem = ecs_vec_get_t(
                &src->elems, ecs_trav_elem_t, t);
            if (!elem->leaf) {
                flecs_trav_table_down_build(world, cache, src, key, root, trav, 
                    elem->table, idr_with, generation);
            }
        }

        src->generation = generation;
        ecs_log_pop_2();
    }

    flecs_trav_append(a, dst, src, root);
    return dst;
}

static
void flecs_trav_entity_isa_build(
    ecs_world_t *world,
    ecs_trav_cache_t *cache,
    ecs_trav_down_t *dst,
    const ecs_trav_key_t *key,
    ecs_entity_t root,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_record_t *idr_with,
    uint32_t generation)
{
    if (trav == EcsIsA) {
        return;
    }

    ecs_id_record_t *idr = flecs_id_record_get(world, ecs_pair(EcsIsA, entity));
    if (!idr) {
        return;
    }

    ecs_table_cache_iter_t it;
    if (flecs_table_cache_all_iter(&idr->cache, &it)) {
        ecs_table_record_t *tr;
        while ((tr = flecs_table_cache_next(&it, ecs_table_record_t))) {
            ecs_table_t *table = tr->hdr.table;
            if (!table->observed_count) {
                continue;
            }

            bool is_root = root == 0;
            ecs_entity_t *entities = ecs_vec_first(&table->data.entities);
            ecs_record_t **records = ecs_vec_first(&table->data.records);
            int32_t i, count = ecs_table_count(table);
            for (i = 0; i < count; i ++) {
                ecs_record_t *record = records[i];
                if (!record) {
                    continue;
                }

                uint32_t flags = ECS_RECORD_TO_ROW_FLAGS(record->row);
                if (flags & EcsEntityObservedAcyclic) {
                    ecs_entity_t e = entities[i];
                    if (is_root) {
                        root = e;
                    }
                    flecs_trav_entity_down_build(world, cache, dst, key, root, 
                        trav, e, idr_with, generation);
                }
            }
        }
    }
}

static
ecs_trav_down_t* flecs_trav_table_down_build(
    ecs_world_t *world,
    ecs_trav_cache_t *cache,
    ecs_trav_down_t *dst,
    const ecs_trav_key_t *key,
    ecs_entity_t root,
    ecs_entity_t trav,
    const ecs_table_t *table,
    ecs_id_record_t *idr_with,
    uint32_t generation)
{
    ecs_assert(table->id != 0, ECS_INTERNAL_ERROR, NULL);
    if (!table->observed_count) {
        return dst;
    }

    ecs_assert(idr_with != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_trav_down_t *src = flecs_trav_table_ensure(cache, key);
    ecs_assert(src != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_allocator_t *a = cache->allocator;

    if (!dst) {
        dst = src;
    }

    if (src->generation == generation) {
        flecs_trav_log_table(world, table, trav, idr_with->id, "down", false);
        cache->table_down_stats.cache_hit ++;
        if (dst == src) {
            return dst;
        }
    } else {
        flecs_trav_log_table(world, table, trav, idr_with->id, "down", true);
        ecs_log_push_2();
        cache->table_down_stats.cache_miss ++;
        ecs_vec_reset_t(a, &src->elems, ecs_trav_elem_t);

        bool is_root = root == 0;
        ecs_entity_t *entities = ecs_vec_first(&table->data.entities);
        ecs_record_t **records = ecs_vec_first(&table->data.records);
        int32_t i, count = ecs_table_count(table);
        for (i = 0; i < count; i ++) {
            ecs_record_t *record = records[i];
            if (!record) {
                continue;
            }

            uint32_t flags = ECS_RECORD_TO_ROW_FLAGS(record->row);
            if (flags & EcsEntityObservedAcyclic) {
                ecs_entity_t entity = entities[i];
                if (is_root) {
                    root = entity;
                }

                ecs_assert(root != 0, ECS_INTERNAL_ERROR, NULL);
                flecs_trav_entity_down_build(world, cache, src,
                    key, root, trav, entity, idr_with, generation);
            }
        }

        ecs_log_pop_2();

        src->generation = generation;
    }

    flecs_trav_append(a, dst, src, root);

    return dst;
}

static
ecs_trav_up_t* flecs_trav_up_build(
    ecs_world_t *world,
    ecs_trav_cache_t *cache,
    ecs_trav_up_t *dst,
    ecs_entity_t trav,
    const ecs_table_t *table,
    ecs_id_record_t *idr_trav,
    ecs_id_record_t *idr_with,
    uint32_t generation)
{
    const ecs_table_record_t *tr, *tr_with;
    if (!(tr = flecs_id_record_get_table(idr_trav, table))) {
        return NULL;
    }

    ecs_assert(idr_with != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_trav_up_t *src = flecs_trav_up_ensure(
        cache, trav, table, idr_with->id, &cache->up_stats);
    ecs_assert(src != NULL, ECS_INTERNAL_ERROR, NULL);

    if (!dst) {
        dst = src;
    }

    int32_t column = -1;
    if (src->generation == generation) {
        flecs_trav_log_table(world, table, trav, idr_with->id, "up", false);
        cache->up_stats.cache_hit ++;
        if (dst == src) {
            goto done;
        }
    } else {
        flecs_trav_log_table(world, table, trav, idr_with->id, "up", true);
        cache->up_stats.cache_miss ++;

        ecs_id_t *ids = table->type.array;
        column = tr->column; 
        int32_t end = column + tr->count;
        
        ecs_id_record_t *idr_excl = NULL;
        ecs_flags32_t id_flags = idr_with->flags;
        ecs_id_t with = idr_with->id;
        bool dont_inherit = false;

        if (trav == EcsIsA) {
            if (id_flags & EcsIdDontInherit) {
                column = end; /* Can't inherit, don't search IsA */
            } else if (id_flags & EcsIdExclusive && ECS_IS_PAIR(with)) {
                ecs_entity_t er = ECS_PAIR_FIRST(with);
                idr_excl = flecs_id_record_get(world, ecs_pair(er, EcsWildcard));
                ecs_assert(idr_excl != NULL, ECS_INTERNAL_ERROR, NULL);
                if (flecs_id_record_get_table(idr_excl, table)) {
                    /* Table already has an instance of an exclusive 
                     * relationship, can't inherit */
                    column = end;
                }
            }
        }

        for (; column < end; column ++) {
            ecs_id_t id = ids[column];
            ecs_entity_t tgt = ecs_pair_second(world, id);
            ecs_record_t *tgt_record = flecs_entities_get(world, tgt);
            ecs_table_t *tgt_table;
            if (!tgt_record || !(tgt_table = tgt_record->table)) {
                continue;
            }

            tr_with = flecs_id_record_get_table(idr_with, tgt_table);
            if (tr_with) { /* Table has id */
                src->source = tgt;
                src->tr = tr_with;
                src->generation = generation;
                src->column = column;
                src->id = tgt_table->type.array[tr_with->column];
                break;
            }

            if (trav != EcsIsA && (tgt_table->flags & EcsTableHasIsA)) {
                if (dont_inherit) {
                    continue;
                }

                if (id_flags & EcsIdDontInherit) {
                    dont_inherit = true;
                    continue;
                }

                if ((id_flags & EcsIdExclusive) && ECS_HAS_ID_FLAG(id, PAIR)) {
                    if (!idr_excl) {
                        ecs_entity_t er = ECS_PAIR_FIRST(with);
                        idr_excl = flecs_id_record_get(world, 
                            ecs_pair(er, EcsWildcard));
                        ecs_assert(idr_excl != NULL, ECS_INTERNAL_ERROR, NULL);
                        if (flecs_id_record_get_table(idr_excl, table)) {
                            dont_inherit = true;
                            continue;
                        }
                    }
                }

                ecs_id_record_t *idr_isa = world->idr_isa_wildcard;
                if (flecs_trav_up_build(world, cache, src, EcsIsA, tgt_table,
                    idr_isa, idr_with, idr_isa->generation))
                { /* Table inherits id */
                    break;
                }
            }

            if (flecs_trav_up_build(world, cache, src, trav, tgt_table,
                idr_trav, idr_with, generation))
            { /* Id is reachable by following relationship */
                break;
            }
        }
        if (column == end) {
            column = -1;
            src->source = 0;
            src->id = 0;
            src->tr = NULL;
            src->generation = generation;
            src->column = -1;
        }
    }

    if (dst != src) {
        dst->source = src->source;
        dst->id = src->id;
        dst->tr = src->tr;
        dst->generation = generation;
    }
    dst->column = column;

done:
    if (dst->source) {
        return dst;
    }

    return NULL;
}

static
void flecs_trav_fini_down(
    ecs_allocator_t *a,
    ecs_sparse_t *down_cache)
{

}

static
void flecs_trav_fini_up(
    ecs_map_t *up_cache)
{
    if (ecs_map_is_initialized(up_cache)) {
        ecs_map_iter_t it = ecs_map_iter(up_cache);
        ecs_trav_up_for_t *up_for;
        while ((up_for = ecs_map_next(&it, ecs_trav_up_for_t, 0))) {
            ecs_map_fini(&up_for->with);
        }
        ecs_map_fini(up_cache);
    }
}

const ecs_trav_down_t* flecs_trav_entity_down(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_t with)
{
    ecs_id_record_t *idr_with = NULL;
    if (with) {
        idr_with = flecs_id_record_get(world, with);
        if (!idr_with) {
            return NULL;
        }
    }
    return flecs_trav_entity_down_w_idr(world, trav, entity, idr_with);
}

const ecs_trav_down_t* flecs_trav_entity_down_w_idr(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_record_t *idr_with)
{
    ecs_assert(ecs_is_alive(world, entity), ECS_INTERNAL_ERROR, NULL);
    ecs_assert(idr_with != NULL, ECS_INTERNAL_ERROR, NULL);

    if (idr_with->flags & EcsIdDontInherit) {
        return NULL; /* Don't build cache for uninheritable ids */
    }

    ecs_trav_cache_t *cache = flecs_trav_cache_get(&world);
    ecs_id_record_t *idr = flecs_id_record_get(world, 
        ecs_pair(trav, EcsWildcard));
    if (!idr) {
        /* If (trav, *) does not exist, there are no tables for it */
        return NULL;
    }

    flecs_process_pending_tables(world);

    ecs_trav_key_t key = flecs_trav_get_key(cache, entity, trav, idr_with->id);
    ecs_trav_down_t* result = flecs_trav_entity_down_build(
        world, cache, NULL, &key, 0, trav, entity, idr_with, idr->generation);
    ecs_assert(result != NULL, ECS_INTERNAL_ERROR, NULL);
    if (!ecs_vec_count(&result->elems)) {
        result = NULL;
    }
    return result;
}

const ecs_trav_down_t* flecs_trav_table_down(
    ecs_world_t *world,
    ecs_entity_t trav,
    const ecs_table_t *table,
    ecs_id_t with)
{
    if (!table->observed_count) {
        return NULL;
    }

    ecs_trav_cache_t *cache = flecs_trav_cache_get(&world);
    ecs_id_record_t *idr_with = NULL;
    if (with) {
        idr_with = flecs_id_record_get(world, with);
        if (!idr_with) {
            return NULL;
        }

        if (idr_with->flags & EcsIdDontInherit) {
            return NULL; /* Don't build cache for uninheritable ids */
        }
    }

    ecs_id_record_t *idr = flecs_id_record_get(world, 
        ecs_pair(trav, EcsWildcard));
    if (!idr) {
        return NULL;
    }

    flecs_process_pending_tables(world);

    ecs_trav_key_t key = flecs_trav_get_key(cache, table->id, trav, idr_with->id);
    ecs_trav_down_t *result = flecs_trav_table_down_build(
        world, cache, NULL, &key, 0, trav, table, idr_with, idr->generation);
    ecs_assert(result != NULL, ECS_INTERNAL_ERROR, NULL);
    if (!ecs_vec_count(&result->elems)) {
        result = NULL;
    }
    return result;
}

const ecs_trav_up_t* flecs_trav_up_w_idr(
    ecs_world_t *world,
    ecs_entity_t trav,
    const ecs_table_t *table,
    ecs_id_record_t *idr_with)
{
    ecs_trav_cache_t *cache = flecs_trav_cache_get(&world);
    ecs_id_record_t *idr = flecs_id_record_get(world, 
        ecs_pair(trav, EcsWildcard));
    if (!idr) {
        return NULL;
    }

    flecs_process_pending_tables(world);

    return flecs_trav_up_build(
        world, cache, NULL, trav, table, idr, idr_with, idr->generation);
}

const ecs_trav_up_t* flecs_trav_up(
    ecs_world_t *world,
    ecs_entity_t trav,
    const ecs_table_t *table,
    ecs_id_t with)
{
    ecs_id_record_t *idr_with = NULL;
    if (with) {
        idr_with = flecs_id_record_get(world, with);
        if (!idr_with) {
            return NULL;
        }
    }

    return flecs_trav_up_w_idr(world, trav, table, idr_with);
}

void flecs_trav_entity_modified(
    ecs_world_t *world,
    ecs_entity_t entity)
{
    ecs_poly_assert(world, ecs_world_t);
    ecs_id_t pair = ecs_pair(EcsWildcard, entity);
    ecs_id_record_t *idr = flecs_id_record_get(world, pair);
    if (!idr) {
        return;
    }

    ecs_id_record_t *cur = idr;
    while ((cur = cur->acyclic.next)) {
        ecs_assert(!ecs_id_is_wildcard(cur->id), ECS_INTERNAL_ERROR, NULL);
        flecs_id_record_inc_generation(cur->parent);
    }
}

static
void flecs_trav_down_cache_clear(
    ecs_trav_cache_t *cache,
    ecs_sparse_t *down_cache,
    uint64_t id)
{
    ecs_trav_down_for_t *df = flecs_sparse_get(
        down_cache, ecs_trav_down_for_t, id);
    if (!df) {
        return;
    }

    

    ecs_map_t *map = &cache->entity_down;
    if (!ecs_map_is_initialized(map)) {
        return;
    }

    ecs_id_t pair = ecs_pair(trav, id);
    ecs_trav_down_for_t *down_for = ecs_map_get(map, ecs_trav_down_for_t, pair);
    if (!down_for) {
        return;
    }

    if (ecs_map_is_initialized(&down_for->with)) {
        ecs_map_iter_t mit = ecs_map_iter(&down_for->with);
        ecs_trav_down_t *down;
        while ((down = ecs_map_next(&mit, ecs_trav_down_t, 0))) {
            ecs_vec_fini_t(cache->allocator, &down->elems, ecs_trav_elem_t);
        }
        ecs_map_fini(&down_for->with);
    }

    ecs_map_remove(map, pair);
}

static
void flecs_trav_down_cache_clear_entity(
    ecs_trav_cache_t *cache,
    ecs_entity_t trav,
    ecs_entity_t id)
{
    flecs_trav_down_cache_clear(cache, &cache->entity_down, id);
}

static
void flecs_trav_down_cache_clear_table(
    ecs_trav_cache_t *cache,
    ecs_entity_t trav,
    ecs_table_t *table)
{
    flecs_trav_down_cache_clear(cache, &cache->entity_down, table->id);
}

static
void flecs_trav_up_cache_table_clear(
    ecs_trav_cache_t *cache,
    ecs_entity_t trav,
    ecs_table_t *table)
{
    ecs_map_t *map = &cache->up;
    if (!ecs_map_is_initialized(map)) {
        return;
    }

    ecs_id_t pair = ecs_pair(trav, table->id);
    ecs_trav_up_for_t *up_for = ecs_map_get(map, ecs_trav_up_for_t, pair);
    if (!up_for) {
        return;
    }

    ecs_map_fini(&up_for->with);
    ecs_map_remove(map, pair);

}

void flecs_trav_entity_clear(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_entity_t entity)
{
    ecs_poly_assert(world, ecs_world_t);
    int32_t i, count = world->stage_count;
    for (i = 0; i < count; i ++) {
        ecs_trav_cache_t *cache = &world->stages[i].trav;
        flecs_trav_down_cache_clear_entity(cache, trav, entity);
    }
}

void flecs_trav_table_clear(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_table_t *table)
{
    ecs_poly_assert(world, ecs_world_t);
    int32_t i, count = world->stage_count;
    for (i = 0; i < count; i ++) {
        ecs_trav_cache_t *cache = &world->stages[i].trav;
        flecs_trav_down_cache_clear_table(cache, trav, table);
        flecs_trav_up_cache_table_clear(cache, trav, table);
    }
}

void flecs_trav_init(
    ecs_allocator_t *a,
    ecs_trav_cache_t *cache)
{
    cache->allocator = a;
    ecs_map_params_init(&cache->trav_down_for_params, a, ecs_trav_down_for_t);
    ecs_map_params_init(&cache->trav_up_for_params, a, ecs_trav_up_for_t);
    ecs_map_params_init(&cache->trav_down_params, a, ecs_trav_down_t);
    ecs_map_params_init(&cache->trav_up_params, a, ecs_trav_up_t);

    flecs_sparse_init(&cache->table_down, a, NULL, ecs_trav_down_for_t);
    flecs_sparse_init(&cache->entity_down, a, NULL, ecs_trav_down_for_t);
    // flecs_sparse_init(&cache->id_map, a, NULL, ecs_trav_down_for_t);
}

void flecs_trav_fini(
    ecs_trav_cache_t *cache)
{
    flecs_trav_fini_down(cache->allocator, &cache->entity_down);
    flecs_trav_fini_down(cache->allocator, &cache->table_down);
    flecs_trav_fini_up(&cache->up);
    ecs_map_params_fini(&cache->trav_down_for_params);
    ecs_map_params_fini(&cache->trav_up_for_params);
    ecs_map_params_fini(&cache->trav_down_params);
    ecs_map_params_fini(&cache->trav_up_params);
}
