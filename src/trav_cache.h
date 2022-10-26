/**
 * @file trav_cache.h
 * @brief Cache for speeding up relationship traversal.
 */

#ifndef FLECS_TRAV_CACHE_H
#define FLECS_TRAV_CACHE_H

typedef struct {
    ecs_table_t *table;
    int16_t trav_column; /* Column index. Only set if pair is not the first for trav */
    bool leaf; /* Table owns and inherits id (for Up queries without Self) */
} ecs_trav_elem_t;

typedef struct ecs_trav_down_t {
    ecs_vec_t elems;     /* vector<ecs_trav_elem_t> */
    int32_t current;
} ecs_trav_down_t;

typedef struct ecs_trav_down_cache_t {
    ecs_map_t with;      /* map <with, ecs_trav_down_t> */
    int32_t generation;
} ecs_trav_down_cache_t;

typedef struct ecs_trav_cache_t {
    // ecs_trav_up_cache_t up;
    ecs_trav_down_cache_t down;
} ecs_trav_cache_t;

const ecs_trav_down_t* flecs_trav_down(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_t with);

void flecs_trav_down_invalidate(
    ecs_world_t *world,
    ecs_id_record_t *idr);

void flecs_trav_down_invalidate_table(
    ecs_world_t *world,
    ecs_table_t *table);

bool flecs_trav_down_invalidate_range(
    ecs_world_t *world,
    ecs_table_t *table,
    int32_t offset,
    int32_t count);

void flecs_trav_init(
    ecs_trav_cache_t *trav);

void flecs_trav_fini(
    ecs_world_t *world,
    ecs_trav_cache_t *trav);

#endif
