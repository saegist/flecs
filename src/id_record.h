/**
 * @file id_record.h
 * @brief Index for looking up tables by (component) id.
 */

#ifndef FLECS_ID_RECORD_H
#define FLECS_ID_RECORD_H

/* Payload for id cache */
typedef struct ecs_table_record_t {
    ecs_table_cache_hdr_t hdr;  /* Table cache header */
    int32_t column;             /* First column where id occurs in table */
    int32_t count;              /* Number of times id occurs in table */
} ecs_table_record_t;

/* Linked list of id records */
typedef struct ecs_id_record_elem_t {
    struct ecs_id_record_t *prev, *next;
} ecs_id_record_elem_t;

typedef struct ecs_reachable_ids_t {
    ecs_table_t *table;
    ecs_entity_t *sources;
    int32_t counter;
} ecs_reachable_ids_t;

/* Payload for id index which contains all datastructures for an id. */
struct ecs_id_record_t {
    /* Cache with all tables that contain the id. Must be first member. */
    ecs_table_cache_t cache; /* table_cache<ecs_table_record_t> */

    /* Flags for id (see api_defines.h) */
    ecs_flags32_t flags;

    /* Name lookup index (currently only used for ChildOf pairs) */
    ecs_hashmap_t *name_index;

    ecs_map_t *reachable; /* map<table_id, reachable_ids_t> */
    int32_t reachable_counter;

    /* Cached pointer to type info for id, if id contains data. */
    const ecs_type_info_t *type_info;

    /* Id of record */
    ecs_id_t id;

    /* Lists for all id records that match a pair wildcard. The wildcard id
     * record is at the head of the list. */
    ecs_id_record_elem_t first;   /* (R, *) */
    ecs_id_record_elem_t second;  /* (*, O) */
    ecs_id_record_elem_t acyclic; /* (*, O) with only acyclic relations */

    /* List of invalidated elements for updating the reachable id cache. The
     * id record for (*, *) contains the head of the list. */
    ecs_id_record_elem_t reachable_changed;
};

/* Get id record for id */
ecs_id_record_t* flecs_get_id_record(
    const ecs_world_t *world,
    ecs_id_t id);

/* Ensure id record for id */
ecs_id_record_t* flecs_ensure_id_record(
    ecs_world_t *world,
    ecs_id_t id);

/* Clear all tables in id record. */
void flecs_clear_id_record(
    ecs_world_t *world,
    ecs_id_t id,
    ecs_id_record_t *idr);

/* Remove id record if empty. If id record only contains empty tables, they will
 * be cleaned up. Id record removal may be deleted if one or more empty tables
 * are referenced as storage table by other tables. */
void flecs_remove_id_record(
    ecs_world_t *world,
    ecs_id_t id,
    ecs_id_record_t *idr);

/* Register table for id record */
void flecs_register_for_id_record(
    ecs_world_t *world,
    ecs_id_t id,
    const ecs_table_t *table,
    ecs_table_record_t *tr);

/* Set (component) type info for id record */
bool flecs_set_type_info_for_id_record(
    ecs_world_t *world,
    ecs_id_record_t *idr,
    const ecs_type_info_t *ti);

/* Ensure id record has name index */
ecs_hashmap_t* flecs_ensure_id_name_index(
    ecs_world_t *world,
    ecs_id_t id);

/* Get name index for id record */
ecs_hashmap_t* flecs_get_id_name_index(
    const ecs_world_t *world,
    ecs_id_t id);

/* Find table record for id */
ecs_table_record_t* flecs_get_table_record(
    const ecs_world_t *world,
    const ecs_table_t *table,
    ecs_id_t id);

/* Find table record for id record */
const ecs_table_record_t* flecs_id_record_table(
    ecs_id_record_t *idr,
    ecs_table_t *table);

/* Return table iterator for id */
ecs_id_record_t* flecs_table_iter(
    ecs_world_t *world,
    ecs_id_t id,
    ecs_table_cache_iter_t *out);

/* Return empty table iterator for id */
ecs_id_record_t* flecs_empty_table_iter(
    ecs_world_t *world,
    ecs_id_t id,
    ecs_table_cache_iter_t *out);

/* Invalidate reachable id cache for e */
void flecs_id_reachable_invalidate(
    ecs_world_t *world,
    ecs_entity_t e);

/* Revalidate reachable id caches */
void flecs_id_reachable_revalidate(
    ecs_world_t *world);

/* Get reachable ids for specific entity/relation */
const ecs_reachable_ids_t* flecs_get_reachable(
    ecs_world_t *world,
    ecs_entity_t relation,
    ecs_table_t *table);

/* Cleanup all reachable id caches */
void flecs_fini_id_reachable(
    ecs_world_t *world);

/* Cleanup all id records in world */
void flecs_fini_id_records(
    ecs_world_t *world);

#endif
