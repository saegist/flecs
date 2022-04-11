#include "private_api.h"
#include <stddef.h>

static
int32_t count_events(
    const ecs_entity_t *events) 
{
    int32_t i;

    for (i = 0; i < ECS_TRIGGER_DESC_EVENT_COUNT_MAX; i ++) {
        if (!events[i]) {
            break;
        }
    }

    return i;
}

static
ecs_entity_t get_actual_event(
    ecs_trigger_t *trigger, 
    ecs_entity_t event)
{
    /* If operator is Not, reverse the event */
    if (trigger->term.oper == EcsNot) {
        if (event == EcsOnAdd) {
            event = EcsOnRemove;
        } else if (event == EcsOnRemove) {
            event = EcsOnAdd;
        }
    }

    return event;
}

static
void unregister_event_trigger(
    ecs_event_record_t *evt,
    ecs_id_t id)
{
    if (ecs_map_remove(&evt->event_ids, id) == 0) {
        ecs_map_fini(&evt->event_ids);
    }
}

static
ecs_event_id_record_t* ensure_event_id_record(
    ecs_world_t *world,
    ecs_map_t *map,
    ecs_id_t id)
{
    ecs_event_id_record_t **idt = ecs_map_ensure(
        map, ecs_event_id_record_t*, id);
    if (!idt[0]) {
        ecs_event_id_record_t *ptr = ecs_os_calloc_t(ecs_event_id_record_t);
        ptr->idr = flecs_ensure_id_record(world, id);
        idt[0] = ptr;
    }

    return idt[0];
}

static
void inc_trigger_count(
    ecs_world_t *world,
    ecs_entity_t event,
    ecs_event_record_t *evt,
    ecs_id_t id,
    int32_t value)
{
    ecs_event_id_record_t *idt = ensure_event_id_record
        (world, &evt->event_ids, id);
    ecs_assert(idt != NULL, ECS_INTERNAL_ERROR, NULL);
    
    int32_t result = idt->trigger_count += value;
    if (result == 1) {
        /* Notify framework that there are triggers for the event/id. This 
         * allows parts of the code to skip event evaluation early */
        flecs_notify_tables(world, id, &(ecs_table_event_t){
            .kind = EcsTableTriggersForId,
            .event = event
        });
    } else if (result == 0) {
        /* Ditto, but the reverse */
        flecs_notify_tables(world, id, &(ecs_table_event_t){
            .kind = EcsTableNoTriggersForId,
            .event = event
        });

        /* Remove admin for id for event */
        if (!ecs_map_is_initialized(&idt->triggers)) {
            unregister_event_trigger(evt, id);
            ecs_os_free(idt);
        }
    }
}

static
ecs_map_t* ensure_triggers_for_relation(
    ecs_event_id_record_t *idt,
    ecs_entity_t relation)
{
    if (!ecs_map_is_initialized(&idt->superset)) {
        ecs_map_init(&idt->superset, ecs_map_t, 1);
    }

    return ecs_map_ensure(&idt->superset, ecs_map_t, relation);
}

static
ecs_map_t* get_triggers_for_relation(
    const ecs_event_id_record_t *idt,
    ecs_entity_t relation)
{
    if (!ecs_map_is_initialized(&idt->superset)) {
        return NULL;
    }

    ecs_map_t *triggers = ecs_map_get(&idt->superset, ecs_map_t, relation);
    if (!triggers) {
        return NULL;
    }

    return triggers;
}

static
void register_trigger_for_id(
    ecs_world_t *world,
    ecs_observable_t *observable,
    ecs_trigger_t *trigger,
    ecs_id_t id,
    ecs_entity_t relation)
{
    ecs_sparse_t *events = observable->events;
    ecs_assert(events != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_id_t term_id = trigger->term.id;

    int i;
    for (i = 0; i < trigger->event_count; i ++) {
        ecs_entity_t event = get_actual_event(trigger, trigger->events[i]);

        /* Get triggers for event */
        ecs_event_record_t *evt = flecs_sparse_ensure(
            events, ecs_event_record_t, event);
        ecs_assert(evt != NULL, ECS_INTERNAL_ERROR, NULL);

        if (ecs_id_is_wildcard(term_id)) {
            evt->wildcard_count ++;
        }

        if (!ecs_map_is_initialized(&evt->event_ids)) {
            ecs_map_init(&evt->event_ids, ecs_event_id_record_t*, 1);
        }

        /* Get triggers for (component) id for event */
        ecs_event_id_record_t *idt = ensure_event_id_record(
            world, &evt->event_ids, id);
        ecs_assert(idt != NULL, ECS_INTERNAL_ERROR, NULL);

        ecs_map_t *triggers;
        if (!relation) {
            triggers = &idt->triggers;
        } else {
            triggers = ensure_triggers_for_relation(idt, relation);
        }

        if (!ecs_map_is_initialized(triggers)) {
            ecs_map_init(triggers, ecs_trigger_t*, 1);
        }

        ecs_map_ensure(triggers, ecs_trigger_t*, trigger->id)[0] = trigger;

        inc_trigger_count(world, event, evt, term_id, 1);
        if (term_id != id) {
            inc_trigger_count(world, event, evt, id, 1);
        }

        if (relation) {
            inc_trigger_count(world, event, evt, 
                ecs_pair(relation, EcsWildcard), 1);
        }

        if (ecs_should_log_3()) {
            char *idstr = ecs_id_str(world, id);
            if (!relation) {
                ecs_dbg_3("trigger registered for id %s", idstr);
            } else {
                ecs_dbg_3("trigger registered for id %s, "
                    "relation %s", idstr, ecs_get_name(world, relation));
            }
            ecs_os_free(idstr);
        }
    }
}

static
void register_trigger(
    ecs_world_t *world,
    ecs_observable_t *observable,
    ecs_trigger_t *trigger)
{
    ecs_term_t *term = &trigger->term;
    ecs_id_t id = term->id;
    ecs_flags32_t mask = term->subj.set.mask;

    if (mask & EcsSelf) {
        if (term->subj.entity == EcsThis) {
            register_trigger_for_id(world, observable, trigger, id, 0);
        }
    }

    if (ECS_HAS_ROLE(id, SWITCH)) {
        ecs_entity_t sw = id & ECS_COMPONENT_MASK;
        ecs_id_t sw_case = ecs_case(sw, EcsWildcard);
        register_trigger_for_id(world, observable, trigger, sw_case, 0);
    }

    if (ECS_HAS_ROLE(id, CASE)) {
        ecs_entity_t sw = ECS_PAIR_FIRST(id);
        register_trigger_for_id(world, observable, trigger, ECS_SWITCH | sw, 0);
    }

    if (mask & EcsSuperSet) {
        ecs_entity_t relation = term->subj.set.relation;
        ecs_assert(relation != 0, ECS_INTERNAL_ERROR, NULL);
        register_trigger_for_id(world, observable, trigger, id, relation);
    }
}

static
void unregister_trigger_for_id(
    ecs_world_t *world,
    ecs_observable_t *observable,
    ecs_trigger_t *trigger,
    ecs_id_t id,
    ecs_entity_t relation)
{
    ecs_sparse_t *events = observable->events;
    ecs_assert(events != NULL, ECS_INTERNAL_ERROR, NULL);
    ecs_id_t term_id = trigger->term.id;

    int i;
    for (i = 0; i < trigger->event_count; i ++) {
        ecs_entity_t event = get_actual_event(trigger, trigger->events[i]);

        /* Get triggers for event */
        ecs_event_record_t *evt = flecs_sparse_get(
            events, ecs_event_record_t, event);
        ecs_assert(evt != NULL, ECS_INTERNAL_ERROR, NULL);

        if (ecs_id_is_wildcard(term_id)) {
            evt->wildcard_count --;
        }

        /* Get triggers for (component) id */
        ecs_event_id_record_t *idt = ecs_map_get_ptr(
            &evt->event_ids, ecs_event_id_record_t*, id);
        ecs_assert(idt != NULL, ECS_INTERNAL_ERROR, NULL);
        
        ecs_map_t *triggers;
        if (!relation) {
            triggers = &idt->triggers;
        } else {
            triggers = get_triggers_for_relation(idt, relation);
        }

        if (ecs_map_remove(triggers, trigger->id) == 0) {
            ecs_map_fini(triggers);
        }

        inc_trigger_count(world, event, evt, term_id, -1);

        if (relation) {
            inc_trigger_count(world, event, evt,
                ecs_pair(relation, EcsWildcard), -1);
        }

        if (id != term_id) {
            /* Id is different from term_id in case of a set trigger. If they're
             * the same, inc_trigger_count could already have done cleanup */
            if (!ecs_map_is_initialized(&idt->triggers) && 
                !idt->trigger_count) 
            {
                unregister_event_trigger(evt, id);
            }

            inc_trigger_count(world, event, evt, id, -1);
        }
    }
}

static
void unregister_trigger(
    ecs_world_t *world,
    ecs_observable_t *observable,
    ecs_trigger_t *trigger)
{    
    ecs_term_t *term = &trigger->term;
    ecs_id_t id = term->id;
    ecs_flags32_t mask = term->subj.set.mask;

    if (term->subj.set.mask & EcsSelf) {
        if (term->subj.entity == EcsThis) {
            unregister_trigger_for_id(world, observable, trigger, term->id, 0);
        }
    }

    if (ECS_HAS_ROLE(term->id, SWITCH)) {
        ecs_entity_t sw = term->id & ECS_COMPONENT_MASK;
        ecs_id_t sw_case = ecs_case(sw, EcsWildcard);
        unregister_trigger_for_id(world, observable, trigger, sw_case, 0);
    }

    if (ECS_HAS_ROLE(term->id, CASE)) {
        ecs_entity_t sw = ECS_PAIR_FIRST(term->id);
        unregister_trigger_for_id(world, observable, trigger, ECS_SWITCH | sw, 0);
    }

    if (mask & EcsSuperSet) {
        ecs_entity_t relation = term->subj.set.relation;
        ecs_assert(relation != 0, ECS_INTERNAL_ERROR, NULL);
        unregister_trigger_for_id(world, observable, trigger, id, relation);
    }
}

static
ecs_map_t* get_triggers_for_event(
    const ecs_observable_t *observable,
    ecs_entity_t event)
{
    ecs_check(observable != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_assert(event != 0, ECS_INTERNAL_ERROR, NULL);

    ecs_sparse_t *events = observable->events;
    ecs_assert(events != NULL, ECS_INTERNAL_ERROR, NULL);

    const ecs_event_record_t *evt = flecs_sparse_get(
        events, ecs_event_record_t, event);
    
    if (evt) {
        return (ecs_map_t*)&evt->event_ids;
    }

error:
    return NULL;
}

static
ecs_event_id_record_t* get_triggers_for_id(
    const ecs_map_t *evt,
    ecs_id_t id)
{
    return ecs_map_get_ptr(evt, ecs_event_id_record_t*, id);
}

bool flecs_check_triggers_for_event(
    const ecs_poly_t *object,
    ecs_id_t id,
    ecs_entity_t event)
{    
    ecs_observable_t *observable = ecs_get_observable(object);
    const ecs_map_t *evt = get_triggers_for_event(observable, event);
    if (!evt) {
        return false;
    }

    ecs_event_id_record_t *edr = get_triggers_for_id(evt, id);
    if (edr) {
        return edr->trigger_count != 0;
    } else {
        return false;
    }
}

static
bool ignore_trigger(
    ecs_world_t *world,
    ecs_trigger_t *t,
    ecs_table_t *table)
{
    int32_t *last_event_id = t->last_event_id;
    if (last_event_id && last_event_id[0] == world->event_id) {
        return true;
    }

    if (!table) {
        return false;
    }

    if (!t->match_prefab && (table->flags & EcsTableIsPrefab)) {
        return true;
    }
    if (!t->match_disabled && (table->flags & EcsTableIsDisabled)) {
        return true;
    }
    
    return false;
}

static
void notify_triggers(
    ecs_world_t *world,
    ecs_iter_t *it,
    const ecs_map_t *triggers)
{
    ecs_assert(triggers != NULL, ECS_INTERNAL_ERROR, NULL);

    ecs_map_iter_t mit = ecs_map_iter(triggers);
    ecs_trigger_t *t;
    while ((t = ecs_map_next_ptr(&mit, ecs_trigger_t*, NULL))) {
        if (ignore_trigger(world, t, it->table)) {
            continue;
        }

        ECS_BIT_COND(it->flags, EcsIterIsFilter, 
            t->term.inout == EcsInOutFilter);

        it->system = t->entity;
        it->self = t->self;
        it->ctx = t->ctx;
        it->binding_ctx = t->binding_ctx;
        it->term_index = t->term.index;
        it->terms = &t->term;
        t->callback(it);
    }
}

static
void trigger_yield_existing(
    ecs_world_t *world,
    ecs_trigger_t *trigger)
{
    ecs_iter_action_t callback = trigger->callback;

    /* If yield existing is enabled, trigger for each thing that matches
     * the event, if the event is iterable. */
    int i, count = trigger->event_count;
    for (i = 0; i < count; i ++) {
        ecs_entity_t evt = trigger->events[i];
        const EcsIterable *iterable = ecs_get(world, evt, EcsIterable);
        if (!iterable) {
            continue;
        }

        ecs_iter_t it;
        iterable->init(world, world, &it, &trigger->term);
        it.system = trigger->entity;
        it.ctx = trigger->ctx;
        it.binding_ctx = trigger->binding_ctx;
        it.event = evt;

        ecs_iter_next_action_t next = it.next;
        ecs_assert(next != NULL, ECS_INTERNAL_ERROR, NULL);
        while (next(&it)) {
            it.event_id = it.ids[0];
            callback(&it);
        }
    }
}

static
void propagate_emit(
    ecs_world_t *world,
    ecs_iter_t *it,
    const ecs_event_id_record_t *eidr,
    ecs_entity_t entity,
    const ecs_id_record_t *id_idr,
    ecs_entity_t relation);

static
void propagate_emit_id_record(
    ecs_world_t *world,
    ecs_iter_t *it,
    const ecs_id_record_t *idr,
    const ecs_event_id_record_t *eidr,
    ecs_entity_t entity,
    const ecs_id_record_t *id_idr,
    ecs_entity_t relation)
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

        ecs_map_t *triggers = get_triggers_for_relation(eidr, rel);
        if (!triggers && (rel != EcsIsA)) {
            return;
        }

        const ecs_table_record_t *tr;
        while ((tr = flecs_table_cache_next(&idt, ecs_table_record_t))) {
            ecs_table_t *table = tr->hdr.table;
            if (id_idr && ecs_table_cache_get(&id_idr->cache, table) != NULL) {
                /* Propagation stops for tables that own the id/component */
                continue;
            }

            ecs_entity_t *entities = ecs_vector_first(
                table->storage.entities, ecs_entity_t);
            ecs_record_t **records = ecs_vector_first(
                table->storage.record_ptrs, ecs_record_t*);

            int32_t ent, entity_count = ecs_table_count(table);
            it->table = table;
            it->type = table->type;
            it->other_table = NULL;
            it->offset = 0;
            it->count = entity_count;
            it->entities = entities;

            if (triggers) {
                /* Treat as new event as this could trigger observers again for
                * different tables. */
                world->event_id ++;
                notify_triggers(world, it, triggers);
            }

            for (ent = 0; ent < entity_count; ent ++) {
                ecs_record_t *r = records[ent];
                uint32_t flags = ECS_RECORD_TO_ROW_FLAGS(r->row);
                if (flags & ECS_FLAG_OBSERVED_ACYCLIC) {
                    if (rel == EcsIsA) {
                        rel = 0;
                    }
                    propagate_emit(world, it, eidr, entities[ent], id_idr, rel);
                }
            }
        }
    }
}

static
void propagate_emit(
    ecs_world_t *world,
    ecs_iter_t *it,
    const ecs_event_id_record_t *eidr,
    ecs_entity_t entity,
    const ecs_id_record_t *id_idr,
    ecs_entity_t relation)
{
    ecs_force_aperiodic(world);
    ecs_log_push();

    if (!relation) {
        /* If no relation is provided, iterate all relations for entity */
        ecs_id_t pair = ecs_pair(EcsWildcard, entity);
        ecs_id_record_t *widr = flecs_get_id_record(world, pair);
        if (!widr) {
            return;
        }

        ecs_id_record_t *idr = widr;
        while ((idr = idr->second.next)) {
            propagate_emit_id_record(
                world, it, idr, eidr, entity, id_idr, relation);
        }
    } else {
        ecs_id_t pair = ecs_pair(relation, entity);
        ecs_id_record_t *idr = flecs_get_id_record(world, pair);
        if (idr) {
            propagate_emit_id_record(
                world, it, idr, eidr, entity, id_idr, relation);
        }
    }

    ecs_log_pop();
}

static
void emit_event(
    ecs_world_t *world,
    ecs_iter_t *it,
    const ecs_map_t *evt,
    ecs_id_t *id_array,
    int32_t id_count,
    ecs_table_t *table,
    ecs_column_t *columns,
    int32_t row,
    int32_t count,
    bool table_event,
    bool notify_self)
{
    int32_t i, ent;
    for (i = 0; i < id_count; i ++) {
        ecs_id_t id = id_array[i];

        ecs_id_record_t *id_idr = NULL;

        const ecs_event_id_record_t *eidr = get_triggers_for_id(evt, id);
        if (eidr) {
            id_idr = eidr->idr;

            /* Event id record should have ensured id record */
            ecs_assert(id_idr != NULL, ECS_INTERNAL_ERROR, NULL);
        } else {
            id_idr = flecs_get_id_record(world, id);
        }

        if (!id_idr) {
            /* If there's no id record there can't be anything to notify for */
            return;
        }

        if (!table_event) {
            const ecs_type_info_t *ti = id_idr->type_info;
            if (ti) {
                ecs_table_record_t *tr = ecs_table_cache_get(
                    &id_idr->cache, table);
                ecs_assert(tr != NULL, ECS_INTERNAL_ERROR, NULL);

                int32_t column = tr->column;
                column = table->storage_map[column];
                if (column != -1) {
                    ecs_size_t size = ti->size;
                    it->ptrs[0] = ecs_vector_get_t(columns[column].data,
                        size, ti->alignment, row);
                    it->sizes[0] = size;
                }
            }
        }

        it->ids[0] = id;
        it->event_id = id;

        /* Populate array with wildcard ids to evaluate */
        ecs_id_t wids[4];
        ecs_id_t role = id & ECS_ROLE_MASK;
        int32_t wid_cur = 1;
        if (role == ECS_PAIR || role == ECS_CASE) {
            ecs_entity_t r = ECS_PAIR_FIRST(id);
            ecs_entity_t o = ECS_PAIR_SECOND(id);
            wids[0] = role | ecs_entity_t_comb(o, EcsWildcard);
            wids[1] = role | ecs_entity_t_comb(EcsWildcard, r);
            wids[2] = role | ecs_entity_t_comb(EcsWildcard, EcsWildcard);
            wid_cur = 3;

            if (notify_self && ecs_is_valid(world, r) && 
                ecs_has_id(world, r, EcsAcyclic)) 
            {
                wids[3] = 0;
                wid_cur ++;
            }
        } else {
            wids[0] = EcsWildcard;
        }

        do {
            if (eidr) {
                notify_triggers(world, it, &eidr->triggers);

                if (count && !table_event) {
                    ecs_record_t **rptrs = ecs_vector_get(
                        table->storage.record_ptrs, ecs_record_t*, row);

                    /* Propagate event to entities */
                    for (ent = 0; ent < count; ent ++) {
                        ecs_record_t *r = rptrs[ent];
                        if (!r) {
                            goto next;
                        }

                        uint32_t flags = ECS_RECORD_TO_ROW_FLAGS(r->row);
                        if (flags & ECS_FLAG_OBSERVED_ACYCLIC) {
                            ecs_entity_t src = it->entities[ent];
                            it->subjects[0] = src;
                            propagate_emit(world, it, eidr, src, id_idr, 0);
                        }
                    }
                }
            }

        next:
            if (-- wid_cur < 0) {
                break;
            }

            ecs_id_t wid = wids[wid_cur];
            if (wid) {
                eidr = get_triggers_for_id(evt, wid);
            } else if (!table_event) {
                ecs_assert(ECS_HAS_ROLE(id, PAIR), ECS_INTERNAL_ERROR, NULL);
                ecs_entity_t obj = ecs_pair_second(world, id);
                ecs_record_t *r = ecs_eis_get(world, obj);

                if (!r) {
                    break;
                }

                ecs_table_t *obj_table = r->table;
                if (!obj_table) {
                    continue;
                }
            }
        } while (true);
    }
}

void flecs_emit(
    ecs_world_t *world,
    ecs_world_t *stage,
    ecs_event_desc_t *desc)
{
    ecs_poly_assert(world, ecs_world_t);
    ecs_check(desc != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_check(desc->event != 0, ECS_INVALID_PARAMETER, NULL);
    ecs_check(desc->event != EcsWildcard, ECS_INVALID_PARAMETER, NULL);
    ecs_check(desc->ids != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_check(desc->ids->count != 0, ECS_INVALID_PARAMETER, NULL);
    ecs_check(desc->table != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_check(desc->observable != NULL, ECS_INVALID_PARAMETER, NULL);

    ecs_ids_t *ids = desc->ids;
    ecs_entity_t event = desc->event;
    ecs_table_t *table = desc->table;
    int32_t row = desc->offset;
    int32_t count = desc->count;
    ecs_entity_t *entities = NULL;
    ecs_column_t *columns = NULL;
    bool table_event = desc->table_event;

    if (!table_event) {
        entities = ecs_vector_get(table->storage.entities, ecs_entity_t, row);
        columns = table->storage.columns;

        if (!count) {
            count = ecs_table_count(table) - row;
        }
    }

    ecs_iter_t it = {
        .world = stage,
        .real_world = world,
        .table = table,
        .type = table->type,
        .entities = entities,
        .term_count = 1,
        .other_table = desc->other_table,
        .offset = row,
        .count = count,
        .param = (void*)desc->param
    };

    flecs_iter_init(&it, flecs_iter_cache_all);

    ECS_BIT_SET(it.flags, EcsIterIsValid);
    ECS_BIT_COND(it.flags, EcsIterTableOnly, table_event);

    world->event_id ++;

    ecs_observable_t *observable = ecs_get_observable(desc->observable);
    ecs_check(observable != NULL, ECS_INVALID_PARAMETER, NULL);

    ecs_entity_t events[2] = {event, EcsWildcard};
    int32_t e;

    /* Iterate events (event + Wildcard) */
    for (e = 0; e < 2; e ++) {
        ecs_entity_t event = events[e];
        const ecs_map_t *evt = get_triggers_for_event(observable, event);
        if (!evt) {
            continue;
        }

        it.event = event;

        emit_event(world, &it, evt, ids->array, ids->count, 
            table, columns, row, count, table_event, true);
    }
    
error:
    return;
}

void ecs_emit(
    ecs_world_t *stage,
    ecs_event_desc_t *desc)
{
    ecs_world_t *world = (ecs_world_t*)ecs_get_world(stage);
    flecs_emit(world, stage, desc);
}

ecs_entity_t ecs_trigger_init(
    ecs_world_t *world,
    const ecs_trigger_desc_t *desc)
{
    char *name = NULL;
    
    ecs_poly_assert(world, ecs_world_t);
    ecs_check(!world->is_readonly, ECS_INVALID_OPERATION, NULL);
    ecs_check(desc != NULL, ECS_INVALID_PARAMETER, NULL);
    ecs_check(desc->_canary == 0, ECS_INVALID_PARAMETER, NULL);
    ecs_check(!world->is_fini, ECS_INVALID_OPERATION, NULL);

    const char *expr = desc->expr;
    ecs_trigger_t *trigger = NULL;
    
    ecs_observable_t *observable = desc->observable;
    if (!observable) {
        observable = ecs_get_observable(world);
    }

    /* If entity is provided, create it */
    ecs_entity_t existing = desc->entity.entity;
    ecs_entity_t entity = ecs_entity_init(world, &desc->entity);
    if (!existing && !desc->entity.name) {
        ecs_add_pair(world, entity, EcsChildOf, EcsFlecsHidden);
    }

    bool added = false;
    EcsTrigger *comp = ecs_get_mut(world, entity, EcsTrigger, &added);
    if (added) {
        ecs_check(desc->callback != NULL, ECS_INVALID_PARAMETER, NULL);
        
        /* Something went wrong with the construction of the entity */
        ecs_check(entity != 0, ECS_INVALID_PARAMETER, NULL);
        name = ecs_get_fullpath(world, entity);

        if (name) {
            ecs_trace("#[green]trigger#[reset] %s created", name);
        }

        ecs_term_t term;
        if (expr) {
    #ifdef FLECS_PARSER
            const char *ptr = ecs_parse_term(world, name, expr, expr, &term);
            if (!ptr) {
                goto error;
            }

            if (!ecs_term_is_initialized(&term)) {
                ecs_parser_error(
                    name, expr, 0, "invalid empty trigger expression");
                goto error;
            }

            if (ptr[0]) {
                ecs_parser_error(name, expr, 0, 
                    "too many terms in trigger expression (expected 1)");
                goto error;
            }
    #else
            ecs_abort(ECS_UNSUPPORTED, "parser addon is not available");
    #endif
        } else {
            term = ecs_term_copy(&desc->term);
        }

        if (ecs_term_finalize(world, name, &term)) {
            ecs_term_fini(&term);
            goto error;
        }

        ecs_log_push();

        trigger = flecs_sparse_add(world->triggers, ecs_trigger_t);
        trigger->id = flecs_sparse_last_id(world->triggers);

        trigger->term = ecs_term_move(&term);
        trigger->callback = desc->callback;
        trigger->ctx = desc->ctx;
        trigger->binding_ctx = desc->binding_ctx;
        trigger->ctx_free = desc->ctx_free;
        trigger->binding_ctx_free = desc->binding_ctx_free;
        trigger->event_count = count_events(desc->events);
        ecs_os_memcpy(trigger->events, desc->events, 
            trigger->event_count * ECS_SIZEOF(ecs_entity_t));
        trigger->entity = entity;
        trigger->self = desc->self;
        trigger->observable = observable;
        trigger->match_prefab = desc->match_prefab;
        trigger->match_disabled = desc->match_disabled;
        trigger->instanced = desc->instanced;
        trigger->last_event_id = desc->last_event_id;

        if (trigger->term.id == EcsPrefab) {
            trigger->match_prefab = true;
        }
        if (trigger->term.id == EcsDisabled) {
            trigger->match_disabled = true;
        }

        comp->trigger = trigger;

        /* Trigger must have at least one event */
        ecs_check(trigger->event_count != 0, ECS_INVALID_PARAMETER, NULL);

        register_trigger(world, observable, trigger);

        ecs_term_fini(&term);

        if (desc->yield_existing) {
            trigger_yield_existing(world, trigger);
        }

        ecs_log_pop();
    } else {
        ecs_assert(comp->trigger != NULL, ECS_INTERNAL_ERROR, NULL);

        /* If existing entity handle was provided, override existing params */
        if (existing) {
            if (desc->callback) {
                ((ecs_trigger_t*)comp->trigger)->callback = desc->callback;
            }
            if (desc->ctx) {
                ((ecs_trigger_t*)comp->trigger)->ctx = desc->ctx;
            }
            if (desc->binding_ctx) {
                ((ecs_trigger_t*)comp->trigger)->binding_ctx = desc->binding_ctx;
            }
        }
    }

    ecs_os_free(name);
    return entity;
error:
    ecs_os_free(name);
    ecs_delete(world, entity);
    return 0;
}

void* ecs_get_trigger_ctx(
    const ecs_world_t *world,
    ecs_entity_t trigger)
{
    const EcsTrigger *t = ecs_get(world, trigger, EcsTrigger);
    if (t) {
        return t->trigger->ctx;
    } else {
        return NULL;
    }     
}

void* ecs_get_trigger_binding_ctx(
    const ecs_world_t *world,
    ecs_entity_t trigger)
{
    const EcsTrigger *t = ecs_get(world, trigger, EcsTrigger);
    if (t) {
        return t->trigger->binding_ctx;
    } else {
        return NULL;
    }      
}

void flecs_trigger_fini(
    ecs_world_t *world,
    ecs_trigger_t *trigger)
{    
    unregister_trigger(world, trigger->observable, trigger);
    ecs_term_fini(&trigger->term);

    if (trigger->ctx_free) {
        trigger->ctx_free(trigger->ctx);
    }

    if (trigger->binding_ctx_free) {
        trigger->binding_ctx_free(trigger->binding_ctx);
    }

    flecs_sparse_remove(world->triggers, trigger->id);
}

void flecs_observable_init(
    ecs_observable_t *observable)
{
    observable->events = ecs_sparse_new(ecs_event_record_t);
}

void flecs_observable_fini(
    ecs_observable_t *observable)
{
    ecs_sparse_t *triggers = observable->events;
    int32_t i, count = flecs_sparse_count(triggers);

    for (i = 0; i < count; i ++) {
        ecs_event_record_t *et = 
            ecs_sparse_get_dense(triggers, ecs_event_record_t, i);
        ecs_assert(et != NULL, ECS_INTERNAL_ERROR, NULL);

        ecs_map_iter_t it = ecs_map_iter(&et->event_ids);
        ecs_event_id_record_t *idt;
        while ((idt = ecs_map_next(&it, ecs_event_id_record_t, NULL))) {
            ecs_map_fini(&idt->triggers);
        }
        ecs_map_fini(&et->event_ids);
    }

    flecs_sparse_free(observable->events);
}
