/**
 * @file trav_cache.h
 * @brief Memoization cache for speeding up relationship traversal.
 */

#ifndef FLECS_TRAV_CACHE_H
#define FLECS_TRAV_CACHE_H

/** Cache for speeding up relationship traversal */
typedef struct {
    ecs_table_t *table;
    ecs_entity_t source;
    bool leaf; /* Table owns and inherits id (for Up queries without Self) */
} ecs_trav_elem_t;

typedef struct ecs_trav_down_t {
    ecs_vector_t *elems; /* vector<trav_down_elem_t> */
    uint32_t generation;
} ecs_trav_down_t;

typedef struct {
    const struct ecs_table_record_t *tr;
    ecs_entity_t source;
    uint32_t generation;
} ecs_trav_up_t;

typedef struct {
    ecs_map_t with; /* map<with, trav_down_t> */
} ecs_trav_down_for_t;

typedef struct {
    ecs_map_t with; /* map<with, trav_down_t> */
} ecs_trav_up_for_t;

typedef struct {
    int32_t cache_hit;
    int32_t cache_miss;
} ecs_trav_stats_t;

typedef struct {
    ecs_map_t entity_down; /* map<(trav, entity), trav_down_for_t> */
    ecs_map_t table_down;  /* map<(trav, table), trav_down_for_t> */
    ecs_map_t up; /* map<(trav, entity), trav_up_for_t> */
    ecs_vector_t *vector_stack; /* Recycle vectors */
    ecs_trav_stats_t entity_down_stats;
    ecs_trav_stats_t table_down_stats;
    ecs_trav_stats_t up_stats;
} ecs_trav_cache_t;

const ecs_trav_down_t* flecs_trav_entity_down(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_t with);

const ecs_trav_down_t* flecs_trav_entity_down_w_idr(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_entity_t entity,
    ecs_id_record_t *idr);

const ecs_trav_down_t* flecs_trav_table_down(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_table_t *table,
    ecs_id_t with);

const ecs_trav_up_t* flecs_trav_up(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_table_t *table,
    ecs_id_t with);

void flecs_trav_entity_modified(
    ecs_world_t *world,
    ecs_entity_t entity);

void flecs_trav_entity_clear(
    ecs_world_t *world,
    ecs_entity_t trav,
    ecs_entity_t entity);

void flecs_trav_fini(
    ecs_trav_cache_t *cache);

#endif
