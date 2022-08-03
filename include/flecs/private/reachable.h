/**
 * @file reachable.h
 */

#ifndef FLECS_REACHABLE_H
#define FLECS_REACHABLE_H

#include "api_defines.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Node in reachable list */
typedef struct ecs_reachable_node_t {
    ecs_id_t id;
    ecs_entity_t src;
    ecs_map_t prev; /* map<entity, node> */
    ecs_map_t next; /* map<entity, node> */
    struct ecs_reachable_node_t *base;
} ecs_reachable_node_t;

/** List with reachable ids for (R, T) */
typedef struct ecs_reachable_list_t {
    ecs_reachable_node_t first;
    uint64_t table_id;
} ecs_reachable_list_t;

FLECS_API
void flecs_reachable_invalidate(
    ecs_world_t *world,
    ecs_entity_t entity);

FLECS_API
void flecs_reachable_validate(
    ecs_world_t *world,
    ecs_id_t id);

#ifdef __cplusplus
}
#endif

#endif
