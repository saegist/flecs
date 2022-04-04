#include "private_api.h"


static
void emit_propagate(
    ecs_world_t *world,
    ecs_iter_t *it,
    ecs_observable_t *observable,
    ecs_entity_t entity,
    ecs_entity_t event,
    ecs_entity_t relation,
    ecs_ids_t *ids);

static
void emit_propagate_id_record(
    ecs_world_t *world,
    ecs_iter_t *it,
    ecs_observable_t *observable,
    ecs_entity_t event,
    ecs_ids_t *ids,
    ecs_id_record_t *idr)
{
    if (!ecs_table_cache_count(&idr->cache)) {
        return;
    }

    ecs_table_cache_iter_t idt;
    if (flecs_table_cache_iter(&idr->cache, &idt)) {
        ecs_table_record_t *first = (ecs_table_record_t*)idt.next;
        ecs_entity_t rel = ECS_PAIR_FIRST(first->id);

        if (ecs_is_valid(world, rel) && !ecs_has_id(world, rel, EcsAcyclic)) {
            return;
        }

        const ecs_table_record_t *tr;
        while ((tr = flecs_table_cache_next(&idt, ecs_table_record_t))) {
            ecs_table_t *table = tr->hdr.table;
            int32_t e, entity_count = ecs_table_count(table);
            it->table = table;
            it->type = table->type;
            it->other_table = NULL;
            it->offset = 0;
            it->count = entity_count;

            /* Treat as new event as this could trigger observers again for
             * different tables. */
            world->event_id ++;

            flecs_set_triggers_notify(it, observable, ids, event,
                ecs_pair(rel, EcsWildcard));

            ecs_entity_t *entities = ecs_vector_first(
                table->storage.entities, ecs_entity_t);
            ecs_record_t **records = ecs_vector_first(
                table->storage.record_ptrs, ecs_record_t*);

            for (e = 0; e < entity_count; e ++) {
                uint32_t flags = ECS_RECORD_TO_ROW_FLAGS(records[e]->row);
                if (flags & ECS_FLAG_OBSERVED_ACYCLIC) {
                    ecs_entity_t relation = rel;
                    if (relation == EcsIsA) {
                        /* IsA pairs propagate all relations */
                        relation = 0;
                    }

                    emit_propagate(world, it, observable, entities[e], event, 
                        relation, ids);
                }
            }
        }
    }
}

static
void emit_propagate(
    ecs_world_t *world,
    ecs_iter_t *it,
    ecs_observable_t *observable,
    ecs_entity_t entity,
    ecs_entity_t event,
    ecs_entity_t relation,
    ecs_ids_t *ids)
{
    ecs_force_aperiodic(world);

    ecs_trace("%s: propagate event from %s", 
        ecs_get_name(world, event),
        ecs_get_name(world, entity));

    if (!relation) {
        /* If no relation is provided, iterate all relations for entity */
        // ecs_id_t pair = ecs_pair(EcsWildcard, entity);
        // ecs_id_record_t *widr = flecs_get_id_record(world, pair);
        // if (!widr) {
        //     return;
        // }

        // ecs_id_record_t *idr = widr;
        // while ((idr = idr->second.next)) {
        //     // emit_propagate_id_record(world, it, observable, event, ids, idr);
        // }
    } else {
        // ecs_id_t pair = ecs_pair(relation, entity);
        // ecs_id_record_t *idr = flecs_get_id_record(world, pair);
        // if (!idr) {
        //     return;
        // }

        // emit_propagate_id_record(world, it, observable, event, ids, idr);
    }
}
