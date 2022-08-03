#include "private_api.h"

void flecs_reachable_invalidate(
    ecs_world_t *world,
    ecs_entity_t entity)
{
    ecs_id_record_t *idr = flecs_id_record_get(world, 
        ecs_pair(EcsWildcard, entity));

    if (!idr) {
        return;
    }

    printf("invalidate for %s\n", ecs_id_str(world, idr->id));
    while ((idr = idr->acyclic.next)) {
        ecs_id_record_t *parent = idr->parent;
        ecs_assert(parent != NULL, ECS_INTERNAL_ERROR, NULL);
        printf(" - invalidate %s\n", ecs_id_str(world, parent->id));
        parent->reachable_generation ++;
    }
}

static
void flecs_reachable_node_init(
    ecs_reachable_node_t *node,
    ecs_id_t id,
    ecs_entity_t src)
{
    ecs_os_zeromem(node);
    node->id = id;
    node->src = src;
}

static
ecs_reachable_node_t* flecs_reachable_node_new(
    ecs_id_t id,
    ecs_entity_t src)
{
    ecs_reachable_node_t *result = ecs_os_calloc_t(ecs_reachable_node_t);
    flecs_reachable_node_init(result, id, src);
    return result;
}

static
ecs_reachable_node_t* flecs_reachable_node_next(
    ecs_reachable_node_t *node,
    ecs_entity_t src)
{
    if (ecs_map_is_initialized(&node->next)) {
        return ecs_map_get_ptr(&node->next, ecs_reachable_node_t*, src);
    } else {
        return NULL;
    }
}

static
void flecs_reachable_node_append(
    ecs_reachable_node_t *node,
    ecs_reachable_node_t *next,
    ecs_entity_t src)
{
    if (!ecs_map_is_initialized(&node->next)) {
        ecs_map_init(&node->next, ecs_reachable_node_t*, 1);
    }
    if (!ecs_map_is_initialized(&next->prev)) {
        ecs_map_init(&next->prev, ecs_reachable_node_t*, 1);
    }

    /* Point node->next to next */
    ecs_reachable_node_t **node_next = ecs_map_ensure(
        &node->next, ecs_reachable_node_t*, src);
    ecs_reachable_node_t *old_node_next = *node_next;
    *node_next = next;

    /* Point next->prev to node */
    ecs_map_set_ptr(&next->prev, src, node);

    /* If node->next was set, update links */
    if (old_node_next) {
        /* Point node->next->prev to next */
        ecs_map_set_ptr(&old_node_next->prev, src, next);

        /* Point next->next to node->next */
        ecs_map_set_ptr(&node->next, src, old_node_next);
    }
}

static
void flecs_reachable_node_remove(
    ecs_reachable_node_t *node)
{
    if (!ecs_map_is_initialized(&node->prev)) {
        return; /* Node must at least point to head */
    }

    /* Connect in- and outgoing edges with each other */
    ecs_map_iter_t it = ecs_map_iter(&node->prev);
    ecs_map_key_t key;
    ecs_reachable_node_t *prev, *next;
    while ((prev = ecs_map_next_ptr(&it, ecs_reachable_node_t*, &key))) {
        next = ecs_map_get_ptr(&node->next, ecs_reachable_node_t*, key);

        /* Connect prev->next with next */
        ecs_map_set_ptr(&prev->next, key, next);

        /* Connect next->prev with prev */
        ecs_map_set_ptr(&next->prev, key, prev);
    }
}

static
void flecs_reachable_node_replace(
    ecs_reachable_node_t *replace,
    ecs_reachable_node_t *with)
{
    if (!ecs_map_is_initialized(&replace->prev)) {
        return; /* Node must at least point to head */
    }

    /* Move edges from old to new node */
    ecs_map_iter_t it = ecs_map_iter(&replace->prev);
    ecs_map_key_t key;
    ecs_reachable_node_t *prev, *next;
    while ((prev = ecs_map_next_ptr(&it, ecs_reachable_node_t*, &key))) {
        next = ecs_map_get_ptr(&replace->next, ecs_reachable_node_t*, key);

        /* Connect prev->next with "with" and "with"->prev with prev */
        ecs_map_set_ptr(&prev->next, key, with);
        ecs_map_set_ptr(&with->prev, key, prev);

        /* Connect next->prev with "with" and "with"->next with next */
        ecs_map_set_ptr(&next->prev, key, with);
        ecs_map_set_ptr(&with->next, key, next);
    }
}

static
void flecs_reachable_node_replace_edge(
    ecs_reachable_node_t *replace,
    ecs_reachable_node_t *with,
    ecs_entity_t edge)
{
    if (!ecs_map_is_initialized(&with->prev)) {
        ecs_map_init(&with->prev, ecs_reachable_node_t*, 1);
    }
    if (!ecs_map_is_initialized(&with->next)) {
        ecs_map_init(&with->next, ecs_reachable_node_t*, 1);
    }

    /* Get values for previous edges */
    ecs_reachable_node_t *prev = ecs_map_get_ptr(
        &replace->prev, ecs_reachable_node_t*, edge);
    ecs_reachable_node_t *next = ecs_map_get_ptr(
        &replace->next, ecs_reachable_node_t*, edge);

    /* Set new edges, remove from old node */
    ecs_map_set_ptr(&with->prev, edge, prev);
    ecs_map_remove(&replace->prev, edge);
    if (next) {
        ecs_map_set_ptr(&with->next, edge, next);
        ecs_map_remove(&replace->next, edge);
    }
}

static
ecs_reachable_list_t* flecs_reachable_list_new(
    ecs_entity_t r,
    ecs_entity_t e)
{
    ecs_reachable_list_t *result = ecs_os_calloc_t(ecs_reachable_list_t);
    flecs_reachable_node_init(&result->first, ecs_pair(r, EcsWildcard), e);
    return result;
}

static
ecs_reachable_node_t* flecs_reachable_build_from_parents(
    ecs_world_t *world,
    ecs_reachable_list_t *rlist,
    ecs_table_t *table,
    const ecs_table_record_t *tr,
    ecs_entity_t e)
{
    ecs_reachable_node_t *prev = &rlist->first, *result = NULL;

    if (tr) {
        printf("  - initialize from parents for %s\n",
            ecs_get_fullpath(world, e));

        ecs_id_t *ids = table->type.array;
        int32_t i = tr->column, end = i + tr->count;
        for (; i < end; i ++) {
            ecs_entity_t t = ECS_PAIR_SECOND(ids[i]);
            ecs_table_record_t *t_tr = &table->records[i];
            ecs_id_record_t *rt_idr = (ecs_id_record_t*)t_tr->hdr.cache;
            ecs_reachable_list_t *t_rlist = rt_idr->reachable;
            if (!t_rlist) {
                printf("    - no cache for parent %s\n",
                    ecs_id_str(world, rt_idr->id));
                continue;
            }

            ecs_reachable_node_t *cur = &t_rlist->first;
            while ((cur = flecs_reachable_node_next(cur, t))) {
                if (!result) {
                    result = cur;
                }

                printf("    - connect %s from parent for %s\n",
                    ecs_id_str(world, cur->id),
                    ecs_get_fullpath(world, e));

                flecs_reachable_node_append(prev, cur, e);
                prev = cur;
            }
        }
    }

    return result;
}

static
void flecs_reachable_build(
    ecs_world_t *world,
    ecs_id_record_t *re_idr,
    ecs_table_t *table,
    const ecs_table_record_t *tr,
    ecs_entity_t r,
    ecs_entity_t e)
{
    printf("\n - build %s for %s\n", 
        ecs_get_fullpath(world, e),
        ecs_get_fullpath(world, r));

    ecs_reachable_list_t *rlist = re_idr->reachable;
    if (!rlist) {
        if (!table) {
            return;
        }
        rlist = flecs_reachable_list_new(r, e);
    }

    re_idr->reachable = rlist;

    ecs_reachable_node_t *cur = flecs_reachable_node_next(&rlist->first, e);
    if (!cur) {
        /* If this is the first node, initialize with inherited nodes */
        cur = flecs_reachable_build_from_parents(world, rlist, table, tr, e);
    }

    ecs_id_t *ids = table->type.array;
    int32_t i, count = table->type.count;
    ecs_reachable_node_t *prev = NULL;

    if (!cur) {
        /* Empty list, initialize with own components */
        cur = &rlist->first;
        for (i = 0; i < count; i ++) {
            ecs_reachable_node_t *node = flecs_reachable_node_new(ids[i], e);
            flecs_reachable_node_append(cur, node, e);
            printf("  - add node %s for %s\n",
                ecs_id_str(world, ids[i]),
                ecs_get_fullpath(world, e));
            cur = node;
        }
    } else {
        for (i = 0; i < count;) {
            ecs_id_t table_id = ids[i];

            if (cur) {
                ecs_id_t cache_id = cur->id;
                printf("  x cache = %s\n", ecs_id_str(world, cache_id));

                if (table_id == cache_id) {
                    if (cur->src == e) {
                        /* Nothing to be done, node for own component already
                         * exists */
                    } else {
                        printf("  - override %s from %s\n", 
                            ecs_id_str(world, table_id),
                            ecs_get_fullpath(world, cur->src));

                        /* A parent has the same id, which means this is an
                         * override. Replace edges for e with new node */
                        ecs_reachable_node_t *node = 
                            flecs_reachable_node_new(table_id, e);
                        flecs_reachable_node_replace_edge(cur, node, e);

                        /* Mark node as override of base component */
                        node->base = cur;
                    }
                } else
                if (table_id < cache_id) {
                    printf("  - insert %s\n", ecs_id_str(world, table_id));

                    /* Entity has id that is not in the cache, insert it */
                    ecs_reachable_node_t *node = 
                        flecs_reachable_node_new(table_id, e);

                    /* Table id is lower than cache id, so append it to the node
                     * before the current one */
                    flecs_reachable_node_append(prev, node, e);
                } else {
                    if (cur->src == e) {
                        /* Cache has node for id that entity no longer has,
                         * remove it */
                        if (cur->base) {
                            printf("  - replace %s with base from %s\n", 
                                ecs_id_str(world, cache_id),
                                ecs_get_fullpath(world, cur->src));

                            /* If node is an override, replace it with the
                             * node from the parent */
                            flecs_reachable_node_replace(cur, cur->base);
                        } else {
                            printf("  - remove %s\n", 
                                ecs_id_str(world, cache_id));

                            /* Node is not an override, remove it */
                            flecs_reachable_node_remove(cur);
                        }
                    } else {
                        /* Inherited node from parent, nothing to be done */
                    }
                }

                if (table_id <= cache_id) {
                    i ++;
                }
                if (cache_id <= table_id) {
                    prev = cur;
                    cur = flecs_reachable_node_next(cur, e);
                }
            } else {
                printf("  - table id = %s\n", ecs_id_str(world, table_id));
                i ++;
            }
        }
    }
}

void flecs_reachable_validate_parents(
    ecs_world_t *world,
    ecs_id_record_t *idr,
    ecs_id_record_t *re_idr,
    ecs_entity_t r,
    ecs_entity_t e)
{
    ecs_assert(idr != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(re_idr != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(r != 0, ECS_INTERNAL_ERROR, NULL);
    ecs_assert(e != 0, ECS_INTERNAL_ERROR, NULL);

    if (idr->reachable_generation == re_idr->reachable_generation) {
        return; /* Entity is up to date for relation */
    }

    ecs_table_t *table = NULL;
    ecs_record_t *record = flecs_entities_get(world, e);
    if (record) {
        table = record->table;
    }

    const ecs_table_record_t *tr = NULL;
    if (table) {
        tr = flecs_id_record_get_table(idr, table);
        if (tr) {
            ecs_id_t *ids = table->type.array;
            int32_t i = tr->column, end = i + tr->count;
            for (; i < end; i ++) {
                ecs_entity_t t = ecs_pair_second(world, ids[i]);
                ecs_id_record_t *rt_idr = flecs_id_record_ensure(
                    world, ecs_pair(r, t));
                flecs_reachable_validate_parents(world, idr, rt_idr, r, t);
            }
        }
    }

    flecs_reachable_build(world, re_idr, table, tr, r, e);

    re_idr->reachable_generation = idr->reachable_generation;
}

void flecs_reachable_validate(
    ecs_world_t *world,
    ecs_id_t id)
{
    ecs_assert(ECS_IS_PAIR(id), ECS_INVALID_PARAMETER, NULL);
    ecs_assert(!ecs_id_is_wildcard(id), ECS_INVALID_PARAMETER, NULL);
    ecs_id_record_t *re_idr = flecs_id_record_ensure(world, id);
    ecs_assert(re_idr->flags & EcsIdAcyclic, ECS_INVALID_PARAMETER, NULL);

    flecs_reachable_validate_parents(world, re_idr->parent, re_idr,
        ECS_PAIR_FIRST(id), ecs_pair_second(world, id));
}
