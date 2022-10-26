#include "private_api.h"

static
void flecs_trav_entity_down_isa(
    ecs_world_t *world,
    ecs_allocator_t *a,
    ecs_trav_down_cache_t *cache,
    ecs_trav_down_t *dst,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_record_t *idr_with);

static
ecs_trav_down_t* flecs_trav_entity_down(
    ecs_world_t *world,
    ecs_allocator_t *a,
    ecs_trav_down_cache_t *cache,
    ecs_trav_down_t *dst,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_record_t *idr_trav,
    ecs_id_record_t *idr_with);

static
ecs_trav_down_t* flecs_trav_down_ensure(
    ecs_allocator_t *a,
    ecs_trav_down_cache_t *cache,
    ecs_id_t with)
{
    ecs_map_init_if(&cache->with, ecs_trav_down_t, a, 1);
    ecs_trav_down_t *trav = ecs_map_ensure(&cache->with, ecs_trav_down_t, with);
    if (!trav->current) {
        trav->current = -1;
    }
    return trav;
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
    if (ecs_should_log_3()) {
        char *id_str = ecs_id_str(world, id);
        if (miss) {
            ecs_dbg_3("trav(%s): cache miss for (%s, %u), id '%s'",
                name, ecs_get_name(world, trav), (uint32_t)entity, id_str);
        } else {
            ecs_dbg_3("trav(%s): cache hit for (%s, %u), id '%s'",
                name, ecs_get_name(world, trav), (uint32_t)entity, id_str);
        }
        ecs_os_free(id_str);
    }
}

static
ecs_trav_down_t* flecs_trav_table_down(
    ecs_world_t *world,
    ecs_allocator_t *a,
    ecs_trav_down_cache_t *cache,
    ecs_trav_down_t *dst,
    ecs_entity_t trav,
    const ecs_table_t *table,
    ecs_id_record_t *idr_with)
{
    ecs_assert(table->id != 0, ECS_INTERNAL_ERROR, NULL);

    if (!table->observed_count) {
        return dst;
    }

    ecs_assert(idr_with != NULL, ECS_INTERNAL_ERROR, NULL);

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
            ecs_id_record_t *idr_trav = flecs_id_record_get(world, 
                ecs_pair(trav, entity));
            if (!idr_trav) {
                continue;
            }

            if (ecs_should_log_3()) {
                char *idstr = ecs_id_str(world, idr_trav->id);
                ecs_dbg_3("trav(down): traverse %s", idstr);
                ecs_os_free(idstr);
            }

            ecs_log_push_3();

            flecs_trav_entity_down(world, a, &idr_trav->trav.down, dst, 
                trav, entity, idr_trav, idr_with);

            ecs_log_pop_3();
        }
    }

    return dst;
}

static
void flecs_trav_entity_down_isa(
    ecs_world_t *world,
    ecs_allocator_t *a,
    ecs_trav_down_cache_t *cache,
    ecs_trav_down_t *dst,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_record_t *idr_with)
{
    if (trav == EcsIsA) {
        return;
    }

    ecs_id_record_t *idr_isa = flecs_id_record_get(
        world, ecs_pair(EcsIsA, entity));
    if (!idr_isa) {
        return;
    }

    if (ecs_should_log_3()) {
        char *idstr = ecs_id_str(world, idr_isa->id);
        ecs_dbg_3("trav(down): traverse inheritance for %s", idstr);
        ecs_os_free(idstr);
    }
    ecs_log_push_3();

    ecs_table_cache_iter_t it;
    if (flecs_table_cache_all_iter(&idr_isa->cache, &it)) {
        ecs_table_record_t *tr;
        while ((tr = flecs_table_cache_next(&it, ecs_table_record_t))) {
            ecs_table_t *table = tr->hdr.table;
            if (!table->observed_count) {
                continue;
            }

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
                    ecs_id_record_t *idr_trav = flecs_id_record_get(world, 
                        ecs_pair(trav, e));
                    flecs_trav_entity_down(world, a, cache, dst, trav, e,
                        idr_trav, idr_with);
                }
            }
        }
    }

    ecs_log_pop_3();
}

static
ecs_trav_down_t* flecs_trav_entity_down(
    ecs_world_t *world,
    ecs_allocator_t *a,
    ecs_trav_down_cache_t *cache,
    ecs_trav_down_t *dst,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_record_t *idr_trav,
    ecs_id_record_t *idr_with)
{
    ecs_assert(dst != NULL, ECS_INTERNAL_ERROR, NULL);

    flecs_trav_entity_down_isa(world, a, cache, dst, trav, entity, idr_with);

    int32_t first = ecs_vec_count(&dst->elems);

    ecs_table_cache_iter_t it;
    if (flecs_table_cache_all_iter(&idr_trav->cache, &it)) {
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

            /* If table is not the first instance of (trav, *), set column
             * so code can test if component can be reached through an
             * earlier (trav, *) pair. The reason this information is not
             * cached is that a component can be added to a preceding pair
             * without invalidating the cache for this pair. */
            int32_t column = tr->column;
            int32_t trav_column = 0;
            if (column) {
                ecs_id_t id = table->type.array[column - 1];
                if (ECS_IS_PAIR(id) && ECS_PAIR_FIRST(id) == trav) {
                    trav_column = column;
                }
            }

            ecs_trav_elem_t *elem = ecs_vec_append_t(
                a, &dst->elems, ecs_trav_elem_t);
            elem->table = table;
            elem->trav_column = trav_column;
            elem->leaf = leaf;

            if (ecs_should_log_3()) {
                char *str = ecs_table_str(world, table);
                ecs_dbg_3("trav(down): inserted entry for table [%s]", str);
                ecs_os_free(str);
            }
        }
    }

    /* Breadth first walk */
    int32_t t, last = ecs_vec_count(&dst->elems);
    for (t = first; t < last; t ++) {
        ecs_trav_elem_t *elem = ecs_vec_get_t(
            &dst->elems, ecs_trav_elem_t, t);
        if (!elem->leaf) {
            flecs_trav_table_down(world, a, cache, dst, trav,
                elem->table, idr_with);
        }
    }

    return dst;
}

const ecs_trav_down_t* flecs_trav_down(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_t with)
{
    ecs_poly_assert(world, ecs_world_t);
    ecs_assert(ecs_id_is_wildcard(with) || ecs_has_id(world, entity, with), 
        ECS_INTERNAL_ERROR, NULL);
    ecs_assert(with != EcsWildcard, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(with != EcsAny, ECS_INTERNAL_ERROR, NULL);

    ecs_id_record_t *idr_with = flecs_id_record_get(world, with);
    if (!idr_with) {
        /* There are no entities with the id */
        return NULL;
    }

    ecs_id_record_t *idr_trav = flecs_id_record_try(world, 
        ecs_pair(trav, entity));
    if (!idr_trav) {
        /* Id violates constraint */
        return NULL;
    }

    ecs_flags32_t id_flags = idr_trav->flags;
    if (!(id_flags & EcsIdAcyclic)) {
        /* Non-acyclic ids can't be traversed */
        return NULL;
    }

    ecs_allocator_t *a = &world->allocator;
    ecs_trav_down_cache_t *cache = &idr_trav->trav.down;
    ecs_trav_down_t *result = flecs_trav_down_ensure(a, cache, with);
    if (result->current != cache->generation) {
        flecs_trav_log_entity(world, entity, trav, with, "down", true);
        ecs_log_push_3();
        ecs_vec_reset_t(a, &result->elems, ecs_trav_elem_t);
        flecs_trav_entity_down(world, a, 
            cache, result, trav, entity, idr_trav, idr_with);
        ecs_log_pop_3();
        result->current = cache->generation;
    } else {
        flecs_trav_log_entity(world, entity, trav, with, "down", false);
    }

    if (!ecs_vec_count(&result->elems)) {
        return NULL;
    }

    return result;
}

void flecs_trav_down_invalidate(
    ecs_world_t *world,
    ecs_id_record_t *idr)
{
    ecs_poly_assert(world, ecs_world_t);
    ecs_assert(idr != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(idr->flags & EcsIdAcyclic, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(!ecs_id_is_wildcard(idr->id), ECS_INTERNAL_ERROR, NULL);

    if (ecs_should_log_3()) {
        char *idstr = ecs_id_str(world, idr->id);
        ecs_dbg_3("trav(down): invalidate id %s", idstr);
        ecs_os_free(idstr);
    }

    ecs_log_push_3();

    ecs_trav_down_cache_t *cache = &idr->trav.down;
    cache->generation ++;

    ecs_table_t *table = NULL;
    ecs_entity_t tgt = ECS_PAIR_SECOND(idr->id);
    ecs_record_t *tgt_record = flecs_entities_get_any(world, tgt);
    if (tgt_record) {
        if ((tgt != EcsWildcard) && ECS_PAIR_FIRST(idr->id) == EcsIsA) {
            /* There could be edges for other relationships connected through an
            * IsA edge, which means those edges indirectly inherit from this 
            * target. An example is an entity requesting a component from a 
            * parent, where the parent inherits that component.
            * Make sure to therefore mark all relationships for the target as 
            * dirty if the invalidated relatioship is IsA. */
            ecs_id_record_t *trav_idr = tgt_record->idr, *cur = trav_idr;
            ecs_assert(cur != NULL, ECS_INTERNAL_ERROR, NULL);
            while ((cur = cur->acyclic.next)) {
                if (cur != idr) {
                    flecs_trav_down_invalidate(world, cur);
                }
            }
        }

        table = tgt_record->table;
    }

    if (!table) {
        goto done;
    }

    if (!(table->flags & EcsTableHasAcyclic)) {
        goto done;
    }

    const ecs_table_record_t *tr = flecs_id_record_get_table(idr, table);
    if (!tr) {
        goto done;
    }

    int32_t i = tr->column, end = i + tr->count;
    for (; i < end; i ++) {
        ecs_table_record_t *parent_tr = &table->records[i];
        flecs_trav_down_invalidate(world, 
            (ecs_id_record_t*)parent_tr->hdr.cache);
    }

done:
    ecs_log_pop_3();
}

void flecs_trav_down_invalidate_table(
    ecs_world_t *world,
    ecs_table_t *table)
{
    if (!table->observed_count) {
        return;
    }

    if (!(table->flags & EcsTableHasAcyclic)) {
        return;
    }

    ecs_dbg_3("trav(down): invalidate cache for table %u", 
        (uint32_t)table->id);
    ecs_log_push_3();

    ecs_id_t *ids = table->type.array;
    int32_t i, count = table->acyclic_count;
    for (i = 0; i < count; i ++) {
        int32_t column = table->acyclic[i];
        ecs_id_t id = ids[column];
        ecs_assert(ECS_IS_PAIR(id), ECS_INTERNAL_ERROR, NULL);
        (void)id;

        ecs_table_record_t *tr = &table->records[column];
        ecs_id_record_t *idr = (ecs_id_record_t*)tr->hdr.cache;
        ecs_assert(idr->flags & EcsIdAcyclic, ECS_INTERNAL_ERROR, NULL);

        flecs_trav_down_invalidate(world, idr);
    }

    ecs_log_pop_3();
}

bool flecs_trav_down_invalidate_range(
    ecs_world_t *world,
    ecs_table_t *table,
    int32_t offset,
    int32_t count)
{
    bool has_observed = false;

    if (table->observed_count) {
        ecs_record_t **records = ecs_vec_first_t(
            &table->data.records, ecs_record_t*);
        int32_t i = offset, end = i + count;
        for (; i < end; i ++) {
            ecs_record_t *r = records[i];
            if (r->row & EcsEntityObservedAcyclic) {
                has_observed = true;

                /* Iterate acyclic pairs for target, invalidate down cache */
                ecs_id_record_t *idr_wc = r->idr, *cur = idr_wc;
                while ((cur = cur->acyclic.next)) {
                    cur->trav.down.generation ++;
                }
            }
        }
    }

    return has_observed;
}

static
void flecs_trav_down_fini(
    ecs_allocator_t *a,
    ecs_trav_down_cache_t *cache)
{
    ecs_map_iter_t it = ecs_map_iter(&cache->with);
    ecs_trav_down_t *entry;
    while ((entry = ecs_map_next(&it, ecs_trav_down_t, 0))) {
        ecs_vec_fini_t(a, &entry->elems, ecs_trav_elem_t);
    }
    ecs_map_fini(&cache->with);
}

void flecs_trav_init(
    ecs_trav_cache_t *trav)
{
    (void)trav;
    return;
}

void flecs_trav_fini(
    ecs_world_t *world,
    ecs_trav_cache_t *trav)
{
    ecs_poly_assert(world, ecs_world_t);
    flecs_trav_down_fini(&world->allocator, &trav->down);
}
