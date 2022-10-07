#include "private_api.h"

static
int32_t flecs_type_search(
    const ecs_table_t *table,
    ecs_id_record_t *idr,
    ecs_id_t *ids,
    ecs_id_t *id_out,
    ecs_table_record_t **tr_out)
{    
    ecs_table_record_t *tr = ecs_table_cache_get(&idr->cache, table);
    if (tr) {
        int32_t r = tr->column;
        if (tr_out) tr_out[0] = tr;
        if (id_out) {
            id_out[0] = flecs_to_public_id(ids[r]);
        }
        return r;
    }

    return -1;
}

static
int32_t flecs_type_offset_search(
    int32_t offset,
    ecs_id_t id,
    ecs_id_t *ids,
    int32_t count,
    ecs_id_t *id_out)
{
    ecs_assert(ids != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(count > 0, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(offset > 0, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(id != 0, ECS_INVALID_PARAMETER, NULL);

    while (offset < count) {
        ecs_id_t type_id = ids[offset ++];
        if (ecs_id_match(type_id, id)) {
            if (id_out) {
                id_out[0] = flecs_to_public_id(type_id);
            }
            return offset - 1;
        }
    }

    return -1;
}

int32_t ecs_search_relation(
    const ecs_world_t *world,
    const ecs_table_t *table,
    int32_t offset,
    ecs_id_t id,
    ecs_entity_t trav,
    ecs_flags32_t flags,
    ecs_entity_t *subject_out,
    ecs_id_t *id_out,
    struct ecs_table_record_t **tr_out)
{
    if (!table) return -1;

    flags = flags ? flags : (EcsSelf|EcsUp);

    if (flags & EcsSelf) {
        int32_t col;
        if (!offset) {
            col = ecs_search(world, table, id, id_out, tr_out);
        } else {
            col = ecs_search_offset(world, table, offset, id, id_out);
        }

        if (col != -1) {
            if (subject_out) subject_out[0] = 0;
            return col;
        }

        if (!(flags & EcsUp)) {
            return -1;
        }
    }

    const ecs_trav_up_t* up;
    if (offset) {
        /* Offset queries are not cached, so manually iterate targets for the
         * relationship and query the cache */
        ecs_id_record_t *idr_trav = flecs_id_record_get(world, 
            ecs_pair(trav, EcsWildcard));
        if (!idr_trav) {
            return -1;
        }

        const ecs_table_record_t *tr = flecs_id_record_get_table(
            idr_trav, table);
        if (!tr) {
            return -1;
        }

        if (offset > tr->column) {
            const ecs_type_t *type = &table->type;
            int32_t i = offset, end = tr->column + tr->count;
            for (; i < end; i ++) {
                ecs_id_t pair = type->array[offset];
                if (ECS_PAIR_FIRST(pair) != trav) {
                    return -1;
                }

                ecs_entity_t tgt = ecs_pair_second(world, pair);
                ecs_assert(tgt != 0, ECS_INTERNAL_ERROR, NULL);

                ecs_table_t *tgt_table = ecs_get_table(world, tgt);
                if (tgt_table) {
                    /* Recursively search. Include EcsSelf in search mask, as
                     * target table could have the requested id.
                     * Recursion will never go more than one level deep because
                     * we're passing 0 to offset. */
                    if (ecs_search_relation(world, tgt_table, 0, id, trav, 
                        EcsSelf|EcsUp, subject_out, id_out, tr_out) != -1)
                    {
                        if (subject_out) {
                            *subject_out = tgt;
                        }
                        return i;
                    }
                }
            }

            return -1;
        }

        /* Fallthrough, if offset is smaller than the first occurrence of the
         * relationship, querying the trav cache will return the first match */
    }

    ecs_entity_t union_first = 0;
    ecs_id_record_t *idr = flecs_id_record_get(world, id);
    if (ecs_id_is_pair(id) && (!idr || ((idr->flags & EcsIdUnion)))) {
        /* If id is a (potential) union, search for (Union, Rel) instead of 
         * (Rel, Tgt), as the target isn't stored in the table type for
         * union relationships. */
        union_first = ECS_PAIR_FIRST(id);
        if (union_first != EcsUnion) {
            id = ecs_pair(EcsUnion, union_first);
            idr = flecs_id_record_get(world, id);
        } else {
            union_first = 0;
        }
    }

    if (!idr) {
        return -1;
    }

    up = flecs_trav_up_w_idr((ecs_world_t*)world, trav, table, idr);
    if (!up) {
        return -1;
    }

    const ecs_table_record_t *tr = up->tr;
    if (subject_out) {
        *subject_out = up->source;
    }

    if (id_out) {
        if (union_first) {
            *id_out = ecs_pair(union_first, EcsWildcard);
        } else {
            *id_out = up->id;
        }
    }

    if (tr_out) {
        *tr_out = (ecs_table_record_t*)tr;
    }

    return up->column;
}

int32_t ecs_search(
    const ecs_world_t *world,
    const ecs_table_t *table,
    ecs_id_t id,
    ecs_id_t *id_out,
    struct ecs_table_record_t **tr_out)
{
    if (!table) return -1;

    ecs_poly_assert(world, ecs_world_t);
    ecs_assert(id != 0, ECS_INVALID_PARAMETER, NULL);

    ecs_id_record_t *idr = flecs_query_id_record_get(world, id);
    if (!idr) {
        return -1;
    }

    ecs_type_t type = table->type;
    ecs_id_t *ids = type.array;
    return flecs_type_search(table, idr, ids, id_out, tr_out);
}

int32_t ecs_search_offset(
    const ecs_world_t *world,
    const ecs_table_t *table,
    int32_t offset,
    ecs_id_t id,
    ecs_id_t *id_out)
{
    if (!offset) {
        return ecs_search(world, table, id, id_out, 0);
    }

    if (!table) return -1;

    ecs_poly_assert(world, ecs_world_t);

    ecs_type_t type = table->type;
    ecs_id_t *ids = type.array;
    int32_t count = type.count;
    return flecs_type_offset_search(offset, id, ids, count, id_out);
}

static
int32_t flecs_relation_depth_walk(
    const ecs_world_t *world,
    ecs_id_record_t *idr,
    ecs_table_t *first,
    ecs_table_t *table)
{
    int32_t result = 0;

    const ecs_table_record_t *tr = flecs_id_record_get_table(idr, table);
    if (!tr) {
        return 0;
    }

    int32_t i = tr->column, end = i + tr->count;
    for (; i != end; i ++) {
        ecs_entity_t o = ecs_pair_second(world, table->type.array[i]);
        ecs_assert(o != 0, ECS_INTERNAL_ERROR, NULL);

        ecs_table_t *ot = ecs_get_table(world, o);
        if (!ot) {
            continue;
        }
        
        ecs_assert(ot != first, ECS_CYCLE_DETECTED, NULL);
        int32_t cur = flecs_relation_depth_walk(world, idr, first, ot);
        if (cur > result) {
            result = cur;
        }
    }
    
    return result + 1;
}

int32_t flecs_relation_depth(
    const ecs_world_t *world,
    ecs_entity_t r,
    ecs_table_t *table)
{
    ecs_id_record_t *idr = flecs_id_record_get(world, ecs_pair(r, EcsWildcard));
    if (!idr) {
        return 0;
    }
    return flecs_relation_depth_walk(world, idr, table, table);
}
