#include "private_api.h"

static
ecs_trav_down_t* flecs_trav_table_down_build(
    ecs_allocator_t *a,
    ecs_world_t *world,
    ecs_trav_cache_t *cache,
    ecs_trav_down_t *dst,
    ecs_entity_t root,
    ecs_entity_t trav,
    ecs_table_t *table,
    ecs_id_record_t *idr_with,
    int32_t generation);

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
ecs_allocator_t* flecs_trav_allocator_get(
    ecs_world_t *world)
{
    ecs_stage_t *stage = flecs_stage_from_world(&world);
    ecs_poly_assert(world, ecs_world_t);
    ecs_poly_assert(stage, ecs_stage_t);
    if (stage->async || world->stage_count > 1) {
        return &stage->allocator;
    } else {
        return &world->allocator;
    }
}

static
ecs_trav_down_t* flecs_trav_ensure_down_cache(
    ecs_allocator_t *a,
    ecs_map_t *down_for,
    ecs_entity_t trav,
    uint64_t tgt,
    ecs_id_t with)
{
    ecs_id_t id = ecs_pair(trav, tgt);
    ecs_map_init_if(down_for, ecs_trav_down_for_t, a, 1);
    ecs_trav_down_for_t *df = ecs_map_ensure(down_for, ecs_trav_down_for_t, id);
    ecs_map_init_if(&df->with, ecs_trav_down_t, a, 1);
    return ecs_map_ensure(&df->with, ecs_trav_down_t, with);
}

static
ecs_trav_up_t* flecs_trav_up_ensure(
    ecs_allocator_t *a,
    ecs_trav_cache_t *cache,
    ecs_entity_t trav,
    ecs_table_t *table,
    ecs_id_t with)
{
    ecs_id_t id = ecs_pair(trav, table->id);
    ecs_map_init_if(&cache->up, ecs_trav_up_for_t, a, 1);
    ecs_trav_up_for_t *df = ecs_map_ensure(&cache->up, ecs_trav_up_for_t, id);
    ecs_map_init_if(&df->with, ecs_trav_up_t, a, 1);
    return ecs_map_ensure(&df->with, ecs_trav_up_t, with);
}

static
ecs_trav_down_t* flecs_trav_table_ensure(
    ecs_allocator_t *a,
    ecs_trav_cache_t *cache,
    ecs_entity_t trav,
    ecs_table_t *table,
    ecs_id_t with)
{
    return flecs_trav_ensure_down_cache(
        a, &cache->table_down, trav, table->id, with);
}

static
ecs_trav_down_t* flecs_trav_entity_ensure(
    ecs_allocator_t *a,
    ecs_trav_cache_t *cache,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_t with)
{
   return flecs_trav_ensure_down_cache(
    a, &cache->entity_down, trav, entity, with);
}

static
ecs_vector_t* flecs_trav_get_vector(
    ecs_trav_cache_t *cache,
    ecs_vector_t *vector)
{
    if (vector) {
        ecs_vector_clear(vector);
        return vector;
    }

    if (ecs_vector_pop(cache->vector_stack, ecs_vector_t*, &vector)) {
        return vector;
    }

    return NULL;
}


static
void flecs_trav_append(
    ecs_trav_down_t *dst,
    ecs_trav_down_t *src,
    ecs_entity_t root)
{
    if (dst == src) {
        return;
    }

    int32_t i, count = ecs_vector_count(src->elems);
    ecs_trav_elem_t *src_elems = ecs_vector_first(
        src->elems, ecs_trav_elem_t);
    ecs_trav_elem_t *dst_elems = ecs_vector_addn(
        &dst->elems, ecs_trav_elem_t, count);
    ecs_os_memcpy_n(dst_elems, src_elems, ecs_trav_elem_t, count);

    for (i = 0; i < count; i ++) {
        dst_elems[i].source = root;
    }
}

static
ecs_trav_down_t* flecs_trav_entity_down_build(
    ecs_allocator_t *a,
    ecs_world_t *world,
    ecs_trav_cache_t *cache,
    ecs_trav_down_t *dst,
    ecs_entity_t root,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_record_t *idr_with,
    int32_t generation)
{
    ecs_trav_down_t *src = flecs_trav_entity_ensure(
        a, cache, trav, entity, idr_with->id);
    ecs_assert(src != NULL, ECS_INTERNAL_ERROR, NULL);

    if (!dst) {
        dst = src;
    }

    if (src->generation == generation) {
        if (dst == src) {
            return dst;
        }
    } else {
        src->elems = flecs_trav_get_vector(cache, src->elems);

        ecs_id_record_t *idr = flecs_id_record_get(world, ecs_pair(trav, entity));
        if (!idr) {
            return dst;
        }

        ecs_table_cache_iter_t it;
        if (flecs_table_cache_iter(&idr->cache, &it)) {
            ecs_table_record_t *tr; 
            while ((tr = flecs_table_cache_next(&it, ecs_table_record_t))) {
                ecs_assert(tr->count == 1, ECS_INTERNAL_ERROR, NULL);
                ecs_table_t *table = tr->hdr.table;
                if (idr_with) {
                    if (flecs_id_record_get_table(idr_with, table) != NULL) {
                        continue;
                    }
                }

                ecs_trav_elem_t *elem = ecs_vector_add(
                    &src->elems, ecs_trav_elem_t);
                if (dst == src) {
                    elem->source = root;
                } else {
                    elem->source = 0;
                }
                elem->table = table;
            }
        }

        /* Breadth first walk */
        int32_t t, last = ecs_vector_count(dst->elems);
        for (t = 0; t < last; t ++) {
            ecs_trav_elem_t *elem = ecs_vector_get(
                dst->elems, ecs_trav_elem_t, t);
            flecs_trav_table_down_build(a, world, cache, dst, root, trav, 
                elem->table, idr_with, generation);
        }

        src->generation = generation;
    }

    flecs_trav_append(dst, src, root);
    return dst;
}

static
ecs_trav_down_t* flecs_trav_table_down_build(
    ecs_allocator_t *a,
    ecs_world_t *world,
    ecs_trav_cache_t *cache,
    ecs_trav_down_t *dst,
    ecs_entity_t root,
    ecs_entity_t trav,
    ecs_table_t *table,
    ecs_id_record_t *idr_with,
    int32_t generation)
{
    if (!table->observed_count) {
        return dst;
    }

    ecs_assert(idr_with != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_trav_down_t *src = flecs_trav_table_ensure(
        a, cache, trav, table, idr_with->id);
    ecs_assert(src != NULL, ECS_INTERNAL_ERROR, NULL);

    if (!dst) {
        dst = src;
    }

    if (src->generation == generation) {
        if (dst == src) {
            return dst;
        }
    } else {
        src->elems = flecs_trav_get_vector(cache, src->elems);

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

                flecs_trav_entity_down_build(a, world, cache, src,
                    root, trav, entity, idr_with, generation);
            }
        }

        src->generation = generation;
    }

    flecs_trav_append(dst, src, root);

    return dst;
}

static
ecs_trav_up_t* flecs_trav_up_build(
    ecs_allocator_t *a,
    ecs_world_t *world,
    ecs_trav_cache_t *cache,
    ecs_trav_up_t *dst,
    ecs_entity_t trav,
    ecs_table_t *table,
    ecs_id_record_t *idr_trav,
    ecs_id_record_t *idr_with,
    int32_t generation)
{
    const ecs_table_record_t *tr, *tr_with;
    if (!(tr = flecs_id_record_get_table(idr_trav, table))) {
        return NULL;
    }
    
    ecs_assert(idr_with != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_trav_up_t *src = flecs_trav_up_ensure(
        a, cache, trav, table, idr_with->id);
    ecs_assert(src != NULL, ECS_INTERNAL_ERROR, NULL);

    if (!dst) {
        dst = src;
    }

    if (src->generation == generation) {
        if (dst == src) {
            goto done;
        }
    } else {
        ecs_id_t *ids = table->type.array;
        int32_t i = tr->column, end = i + tr->count;
        for (; i < end; i ++) {
            ecs_id_t id = ids[i];
            ecs_entity_t tgt = ecs_pair_second(world, id);
            ecs_record_t *tgt_record = flecs_entities_get(world, tgt);
            ecs_table_t *tgt_table;
            if (!tgt_record || !(tgt_table = tgt_record->table)) {
                continue;
            }

            tr_with = flecs_id_record_get_table(
                idr_with, tgt_table);
            if (tr_with) { /* Table has id */
                src->source = tgt;
                src->tr = tr_with;
                src->generation = generation;
                break;
            }

            if (trav != EcsIsA && tgt_table->flags & EcsTableHasIsA) {
                ecs_id_record_t *idr_isa = world->idr_isa_wildcard;
                if (flecs_trav_up_build(a, world, cache, src, EcsIsA, tgt_table,
                    idr_isa, idr_with, idr_isa->generation))
                { /* Table inherits id */
                    break;
                }
            }

            if (flecs_trav_up_build(a, world, cache, src, trav, tgt_table,
                idr_trav, idr_with, generation))
            { /* Id is reachable by following relationship */
                break;
            }
        }
        if (i == end) {
            src->source = 0;
            src->tr = NULL;
            src->generation = generation;
        }
    }

    if (dst != src) {
        dst->source = src->source;
        dst->tr = src->tr;
        dst->generation = generation;
    }

done:
    if (dst->source) {
        return dst;
    }

    return NULL;
}

static
void flecs_trav_fini_down(
    ecs_map_t *down_cache)
{
    if (ecs_map_is_initialized(down_cache)) {
        ecs_map_iter_t it = ecs_map_iter(down_cache);
        ecs_trav_down_for_t *down_for;
        while ((down_for = ecs_map_next(&it, ecs_trav_down_for_t, 0))) {
            if (ecs_map_is_initialized(&down_for->with)) {
                ecs_map_iter_t mit = ecs_map_iter(&down_for->with);
                ecs_trav_down_t *down;
                while ((down = ecs_map_next(&mit, ecs_trav_down_t, 0))) {
                    ecs_vector_free(down->elems);
                }
                ecs_map_fini(&down_for->with);
            }
        }
        ecs_map_fini(down_cache);
    }
}

static
void flecs_trav_cache_entity_clear(
    ecs_trav_cache_t *cache,
    ecs_entity_t trav,
    ecs_entity_t entity)
{
    ecs_map_t *map = &cache->entity_down;
    if (!ecs_map_is_initialized(map)) {
        return;
    }

    ecs_id_t pair = ecs_pair(trav, entity);
    ecs_trav_down_for_t *down_for = ecs_map_get(map, ecs_trav_down_for_t, pair);
    if (!down_for) {
        return;
    }

    if (ecs_map_is_initialized(&down_for->with)) {
        ecs_map_iter_t mit = ecs_map_iter(&down_for->with);
        ecs_trav_down_t *down;
        while ((down = ecs_map_next(&mit, ecs_trav_down_t, 0))) {
            ecs_vector_add(
                &cache->vector_stack, ecs_vector_t*)[0] = down->elems;
        }
        ecs_map_fini(&down_for->with);
    }
}

const ecs_trav_down_t* flecs_trav_entity_down(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_t with)
{
    ecs_assert(ecs_is_alive(world, entity), ECS_INTERNAL_ERROR, NULL);
    ecs_allocator_t *a = flecs_trav_allocator_get(world);
    ecs_trav_cache_t *cache = flecs_trav_cache_get(&world);
    ecs_id_record_t *idr_with = NULL;
    if (with) {
        idr_with = flecs_id_record_get(world, with);
        if (!idr_with) {
            return NULL;
        }
    }

    ecs_id_record_t *idr = flecs_id_record_get(world, 
        ecs_pair(trav, EcsWildcard));
    if (!idr) {
        return NULL;
    }

    flecs_process_pending_tables(world);

    return flecs_trav_entity_down_build(
        a, world, cache, NULL, 0, trav, entity, idr_with, idr->generation);
}

const ecs_trav_down_t* flecs_trav_table_down(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_table_t *table,
    ecs_id_t with)
{
    ecs_allocator_t *a = flecs_trav_allocator_get(world);
    ecs_trav_cache_t *cache = flecs_trav_cache_get(&world);
    ecs_id_record_t *idr_with = NULL;
    if (with) {
        idr_with = flecs_id_record_get(world, with);
        if (!idr_with) {
            return NULL;
        }
    }

    ecs_id_record_t *idr = flecs_id_record_get(world, 
        ecs_pair(trav, EcsWildcard));
    if (!idr) {
        return NULL;
    }

    flecs_process_pending_tables(world);

    return flecs_trav_table_down_build(
        a, world, cache, NULL, 0, trav, table, idr_with, idr->generation);
}

const ecs_trav_up_t* flecs_trav_up(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_table_t *table,
    ecs_id_t with)
{
    ecs_allocator_t *a = flecs_trav_allocator_get(world);
    ecs_trav_cache_t *cache = flecs_trav_cache_get(&world);
    ecs_id_record_t *idr_with = NULL;
    if (with) {
        idr_with = flecs_id_record_get(world, with);
        if (!idr_with) {
            return NULL;
        }
    }

    ecs_id_record_t *idr = flecs_id_record_get(world, 
        ecs_pair(trav, EcsWildcard));
    if (!idr) {
        return NULL;
    }

    flecs_process_pending_tables(world);

    return flecs_trav_up_build(
        a, world, cache, NULL, trav, table, idr, idr_with, idr->generation);
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
        cur->generation ++; /* Increase for each affected relationship */
    }
}

void flecs_trav_entity_clear(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_entity_t entity)
{
    ecs_poly_assert(world, ecs_world_t);
    int32_t i, count = world->stage_count;
    for (i = 0; i < count; i ++) {
        flecs_trav_cache_entity_clear(&world->stages[i].trav, trav, entity);
    }
}

void flecs_trav_fini(
    ecs_trav_cache_t *cache)
{
    flecs_trav_fini_down(&cache->entity_down);
    flecs_trav_fini_down(&cache->table_down);

    ecs_vector_t *vector_stack = cache->vector_stack;
    int32_t i, count = ecs_vector_count(vector_stack);
    ecs_vector_t **vectors = ecs_vector_first(vector_stack, ecs_vector_t*);
    for (i = 0; i < count; i ++) {
        ecs_vector_free(vectors[i]);
    }

    ecs_vector_free(vector_stack);
    cache->vector_stack = NULL;
}
