/**
 * @file trav_cache.h
 * @brief Memoization cache for speeding up relationship traversal.
 */

#ifndef FLECS_TRAV_CACHE_H
#define FLECS_TRAV_CACHE_H

/* Id compressed to index */
typedef struct {
    int32_t value;
} ecs_trav_id_index_t;

/* Traversal cache key */
typedef struct {
    uint64_t id;  /* Entity or table id */
    uint32_t trav; /* Compressed trav id */
    uint32_t with; /* Compressed with id */
} ecs_trav_key_t;

typedef struct {
    const struct ecs_table_record_t *tr;
    ecs_entity_t source;
    ecs_id_t id;    /* found id (can be different for wildcard queries) */
    int32_t column; /* column for traversed relationship */
    uint32_t generation;
} ecs_trav_up_t;

typedef struct {
    ecs_vec_t with;
} ecs_trav_up_for_trav_t; /* vec<ecs_trav_up_t> (with = index) */

typedef struct {
    ecs_vec_t trav; /* vec<ecs_trav_down_for_trav_t> (trav = index) */
} ecs_trav_up_for_t;

/** Cache for speeding up relationship traversal */
typedef struct {
    ecs_table_t *table;
    ecs_entity_t source;
    bool leaf; /* Table owns and inherits id (for Up queries without Self) */
} ecs_trav_elem_t;

typedef struct ecs_trav_down_t {
    ecs_vec_t elems;     /* vector<trav_down_elem_t> */
    uint32_t generation;
} ecs_trav_down_t;

typedef struct {
    ecs_vec_t with;
} ecs_trav_down_for_trav_t; /* vec<ecs_trav_down_t> (with = index) */

typedef struct {
    ecs_vec_t trav; /* vec<ecs_trav_down_for_trav_t> (trav = index) */
} ecs_trav_down_for_t;

typedef struct {
    int32_t cache_hit;
    int32_t cache_miss;
} ecs_trav_stats_t;

typedef struct {
    ecs_allocator_t *allocator;
    ecs_map_params_t trav_up_for_params;
    ecs_map_params_t trav_up_params;

    ecs_sparse_t entity_down;
    ecs_sparse_t table_down;
    ecs_sparse_t up;

    /* Compress trav and with ids so we can use direct array indexing. Use 
     * separate sparse sets for regular ids and relationships so that 
     * relationship ids remain small, which saves space in the cache. */
    ecs_sparse_t trav_map;    /* sparse set, relationship fits in 32 bits */
    ecs_sparse_t with_lo_map; /* sparse set for low ids */
    ecs_map_t with_hi_map;    /* map for high ids */

    ecs_trav_stats_t entity_down_stats;
    ecs_trav_stats_t table_down_stats;
    ecs_trav_stats_t up_stats;
} ecs_trav_cache_t;

void flecs_trav_init(
    ecs_allocator_t *allocator,
    ecs_trav_cache_t *cache);

void flecs_trav_fini(
    ecs_trav_cache_t *cache);

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
    const ecs_table_t *table,
    ecs_id_t with);

const ecs_trav_up_t* flecs_trav_up(
    ecs_world_t *world,
    ecs_entity_t trav,
    const ecs_table_t *table,
    ecs_id_t with);

const ecs_trav_up_t* flecs_trav_up_w_idr(
    ecs_world_t *world,
    ecs_entity_t trav,
    const ecs_table_t *table,
    ecs_id_record_t *idr);

void flecs_trav_entity_modified(
    ecs_world_t *world,
    ecs_entity_t entity);

void flecs_trav_entity_clear(
    ecs_world_t *world,
    ecs_entity_t entity);

void flecs_trav_table_clear(
    ecs_world_t *world,
    ecs_table_t *table);

#endif
