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
void inc_relation_trigger_count(
    ecs_world_t *world,
    ecs_observable_t *observable,
    ecs_entity_t event,
    ecs_id_t id,
    int32_t value)
{
    ecs_sparse_t *events = observable->events;

    if (event == EcsOnSet) {
        event = EcsOnAdd;
    } else if (event == EcsUnSet) {
        event = EcsOnRemove;
    } else if (event != EcsOnAdd && event != EcsOnRemove) {
        return;
    }

    ecs_event_record_t *evt = flecs_sparse_ensure(
        events, ecs_event_record_t, event);

    inc_trigger_count(world, event, evt, id, value);
}

static
ecs_map_t* ensure_trigger_set(
    ecs_event_id_record_t *idt,
    ecs_map_t *trigger_set,
    ecs_entity_t index)
{
    if (!ecs_map_is_initialized(trigger_set)) {
        ecs_map_init(trigger_set, ecs_map_t, 1);
    }

    return ecs_map_ensure(trigger_set, ecs_map_t, index);
}

static
ecs_map_t* get_trigger_set(
    const ecs_event_id_record_t *idt,
    const ecs_map_t *trigger_set,
    ecs_entity_t index)
{
    if (!ecs_map_is_initialized(trigger_set)) {
        return NULL;
    }

    ecs_map_t *triggers = ecs_map_get(trigger_set, ecs_map_t, index);
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
        ecs_entity_t subj = trigger->term.subj.entity;
        if (subj == EcsThis) {
            if (!relation) {
                triggers = &idt->triggers;
            } else {
                triggers = ensure_trigger_set(idt, &idt->superset, relation);
            }
        } else {
            if (!relation) {
                triggers = ensure_trigger_set(idt, &idt->entity, subj);
            } else {
                triggers = ensure_trigger_set(idt, &idt->superset_entity, subj);
            }
            flecs_add_flag(world, subj, EcsEntityObserved);
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
            inc_relation_trigger_count(world, observable, event, 
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
        register_trigger_for_id(world, observable, trigger, id, 0);
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
        ecs_entity_t subj = trigger->term.subj.entity;
        if (subj == EcsThis) {
            if (!relation) {
                triggers = &idt->triggers;
            } else {
                triggers = get_trigger_set(idt, &idt->superset, relation);
            }
        } else {
            if (!relation) {
                triggers = get_trigger_set(idt, &idt->entity, subj);
            } else {
                triggers = get_trigger_set(idt, &idt->superset_entity, subj);
            }
        }

        if (ecs_map_remove(triggers, trigger->id) == 0) {
            ecs_map_fini(triggers);
        }

        inc_trigger_count(world, event, evt, term_id, -1);

        if (relation) {
            inc_relation_trigger_count(world, observable, event,
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
    const ecs_map_t *triggers,
    ecs_entity_t relation_filter,
    bool has_override)
{
    ecs_assert(triggers != NULL, ECS_INTERNAL_ERROR, NULL);

    ecs_map_iter_t mit = ecs_map_iter(triggers);
    ecs_trigger_t *t;

    while ((t = ecs_map_next_ptr(&mit, ecs_trigger_t*, NULL))) {
        if (ignore_trigger(world, t, it->table)) {
            continue;
        }

        ecs_term_t *term = &t->term;

        if (relation_filter) {
            /* Used for entity triggers. Since the map for an entity contains
             * superset triggers for all relations, skip the ones for relations
             * that don't match the specified filter. */
            if (term->subj.set.relation != relation_filter) {
                continue;
            }
        }

        /* If id is from a different source than the current table and trigger
         * has Self flag, only propagate if table doesn't own the id. */
        ecs_flags32_t set_mask = term->subj.set.mask;
        if (has_override && (set_mask & EcsSelf)) {
            continue;
        }

        ECS_BIT_COND(it->flags, EcsIterIsFilter, 
            t->term.inout == EcsInOutFilter);

        it->system = t->entity;
        it->self = t->self;
        it->ctx = t->ctx;
        it->binding_ctx = t->binding_ctx;
        it->term_index = t->term.index;
        it->terms = term;

        int32_t count = it->count;
        if (t->instanced || (count == 1) || !it->subjects[0] || !it->ptrs[0]) {
            t->callback(it);
        } else {
            ecs_entity_t *entities = it->entities;
            int32_t i, count = it->count;
            it->count = 1;
            for (i = 0; i < count; i ++) {
                it->entities = &entities[i];
                t->callback(it);
            }
            it->entities = entities;
            it->count = count;
        }
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
void notify_entity_triggers(
    ecs_world_t *world,
    ecs_iter_t *it,
    const ecs_event_id_record_t *eidr,
    ecs_entity_t relation,
    bool has_override)
{
    /* If entity triggers exist for event/id, check if the entities
     * triggered for are being observed */
    const ecs_map_t *te;
    if (!relation) {
        te = &eidr->entity;
    } else {
        te = &eidr->superset_entity;
    }

    if (ecs_map_is_initialized(te) && ecs_map_count(te)) {
        ecs_entity_t *entities = it->entities;
        int32_t e_i, e_count = it->count;
        for (e_i = 0; e_i < e_count; e_i ++) {
            ecs_entity_t zero = 0;
            ecs_entity_t subj = it->entities[e_i];
            ecs_record_t *record = ecs_eis_get(world, subj);

            if (record && record->row & EcsEntityObserved) {
                ecs_map_t *triggers = ecs_map_get(te, ecs_map_t, subj);

                if (triggers) {
                    it->entities = &zero;
                    if (!relation) {
                        it->subjects[0] = subj;
                    }
                    it->count = 1;
                    notify_triggers(
                        world, it, triggers, relation, has_override);
                }
                it->entities = entities;
                it->subjects[0] = 0;
                it->count = e_count;
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
    ecs_entity_t relation);

static
void propagate_emit_id_record(
    ecs_world_t *world,
    ecs_iter_t *it,
    const ecs_id_record_t *idr,
    const ecs_event_id_record_t *eidr,
    const ecs_id_record_t *id_idr)
{
    if (!ecs_table_cache_count(&idr->cache)) {
        return;
    }

    ecs_entity_t rel = ECS_PAIR_FIRST(idr->id);
    ecs_id_t id = id_idr->id;
    if (ecs_id_is_pair(id)) {
        if (ECS_PAIR_FIRST(id) == rel) {
            /* If the relation of the id is the same as the relation along which
             * we propagate, stop. */
            return;
        }
    }

    ecs_table_cache_iter_t idt;
    if (flecs_table_cache_iter(&idr->cache, &idt)) {
        bool has_entity_triggers = ecs_map_is_initialized(&eidr->superset_entity);
        ecs_map_t *triggers = get_trigger_set(eidr, &eidr->superset, rel);
        if (!triggers && (rel != EcsIsA) && !has_entity_triggers) {
            return;
        }

        ecs_table_t *prev_table = it->table;
        ecs_table_t *prev_other_table = it->other_table;
        int32_t prev_offset = it->offset;
        int32_t prev_count = it->count;
        ecs_entity_t *prev_entities = it->entities;

        const ecs_table_record_t *tr;
        while ((tr = flecs_table_cache_next(&idt, ecs_table_record_t))) {
            ecs_table_t *table = tr->hdr.table;
            if (id_idr && ecs_table_cache_get(&id_idr->cache, table) != NULL) {
                /* Propagation stops for tables that own the id/component */
                continue;
            }

            ecs_entity_t *entities = ecs_vector_first(
                table->data.entities, ecs_entity_t);
            ecs_record_t **records = ecs_vector_first(
                table->data.record_ptrs, ecs_record_t*);

            int32_t ent, entity_count = ecs_table_count(table);
            it->table = table;
            it->other_table = NULL;
            it->offset = 0;
            it->count = entity_count;
            it->entities = entities;

            if (triggers) {
                /* Treat as new event as this could trigger observers again for
                 * different tables. */
                world->event_id ++;
                notify_triggers(world, it, triggers, 0, false);
            }

            if (has_entity_triggers) {
                notify_entity_triggers(world, it, eidr, rel, false);
            }

            for (ent = 0; ent < entity_count; ent ++) {
                ecs_record_t *r = records[ent];
                uint32_t flags = ECS_RECORD_TO_ROW_FLAGS(r->row);
                if (flags & EcsEntityObservedAcyclic) {
                    if (rel == EcsIsA) {
                        rel = 0;
                    }
                    propagate_emit(world, it, eidr, entities[ent], id_idr, rel);
                }
            }
        }

        it->table = prev_table;
        it->other_table = prev_other_table;
        it->offset = prev_offset;
        it->count = prev_count;
        it->entities = prev_entities;
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
    ecs_run_aperiodic(world, EcsAperiodicEmptyTableEvents);

    if (!relation) {
        /* If no relation is provided, iterate acyclic relations for entity */
        ecs_id_t pair = ecs_pair(EcsWildcard, entity);
        ecs_id_record_t *widr = flecs_get_id_record(world, pair);
        if (!widr) {
            return;
        }

        ecs_id_record_t *idr = widr;
        while ((idr = idr->acyclic.next)) {
            propagate_emit_id_record(world, it, idr, eidr, id_idr);
        }

    } else {
        ecs_id_t pair = ecs_pair(relation, entity);
        ecs_id_record_t *idr = flecs_get_id_record(world, pair);
        if (idr) {
            propagate_emit_id_record(world, it, idr, eidr, id_idr);
        }
    }
}

static
void emit_event(
    ecs_world_t *world,
    ecs_observable_t *observable,
    ecs_iter_t *it,
    const ecs_map_t *evt,
    ecs_entity_t relation,
    ecs_entity_t source,
    ecs_id_t *id_array,
    int32_t id_count,
    int32_t row,
    int32_t count,
    bool notify_self,
    bool wildcard_event);

static
void emit_reachable_ids(
    ecs_world_t *world,
    ecs_observable_t *observable,
    ecs_iter_t *it,
    const ecs_map_t *evt,
    ecs_entity_t r,
    const ecs_reachable_ids_t *reachable,
    int32_t row,
    int32_t count,
    bool notify_self)
{
    ecs_table_t *rtable = reachable->table;
    ecs_entity_t *rsrc = reachable->sources;
    
    if (!rtable) {
        return;
    }

    ecs_entity_t event = it->event;

    /* Emit UnSet events for reachable components of removed relationship */
    if (event == EcsOnRemove && r == EcsIsA) {
        const ecs_map_t *evt_unset = get_triggers_for_event(observable, EcsUnSet);
        if (evt) {
            it->event = EcsUnSet;
            emit_reachable_ids(world, observable, it, evt_unset, r,
                reachable, row, count, notify_self);
            it->event = event;
        }
    }

    int32_t ri, rcount = ecs_vector_count(rtable->type);
    ecs_id_t *rids = ecs_vector_first(
        rtable->type, ecs_id_t);
    for (ri = 0; ri < rcount; ri ++) {
        ecs_id_t rid = rids[ri];
        ecs_entity_t src = rsrc[ri];

        it->subjects[0] = src;

        world->event_id ++;
        emit_event(world, observable, it, evt, r, src, &rid, 1,
            row, count, notify_self, false);
    }

    it->subjects[0] = 0;

    /* Emit OnSet events for reachable components of added relationship */
    if (event == EcsOnAdd && r == EcsIsA) {
        const ecs_map_t *evt_onset = get_triggers_for_event(observable, EcsOnSet);
        if (evt) {
            it->event = EcsOnSet;
            emit_reachable_ids(world, observable, it, evt_onset, r,
                reachable, row, count, notify_self);
            it->event = event;
        }
    }
}

static
void copy_from_base(
    ecs_world_t *world,
    ecs_id_record_t *id_idr,
    ecs_entity_t base,
    ecs_iter_t *it)
{
    ecs_id_t id = id_idr->id;
    const ecs_type_info_t *ti = id_idr->type_info;

    /* Component is reachable, copy from base */
    const void *base_ptr = ecs_get_id(world, base, id);
    ecs_assert(base_ptr != NULL, ECS_INTERNAL_ERROR, NULL);

    ecs_size_t size = ti->size;
    ecs_copy_t copy = ti->lifecycle.copy;
    int32_t i, count = it->count;
    void *ptr = it->ptrs[0];

    if (copy) {
        for (i = 0; i < count; i ++) {
            copy(ptr, base_ptr, 1, ti);
            ptr = ECS_OFFSET(ptr, size);
        }
    } else {
        for (i = 0; i < count; i ++) {
            ecs_os_memcpy(ptr, base_ptr, size);
            ptr = ECS_OFFSET(ptr, size);
        }
    }
}

static
void emit_event(
    ecs_world_t *world,
    ecs_observable_t *observable,
    ecs_iter_t *it,
    const ecs_map_t *evt,
    ecs_entity_t relation,
    ecs_entity_t source,
    ecs_id_t *id_array,
    int32_t id_count,
    int32_t row,
    int32_t count,
    bool notify_self,
    bool wildcard_event)
{
    ecs_table_t *table = it->table;
    ecs_flags32_t it_flags = it->flags;
    ecs_flags32_t table_flags = table->flags;
    bool table_event = it_flags & EcsIterTableOnly;
    bool is_set = it_flags & EcsIterIsSet;
    ecs_entity_t event = it->event;

    int32_t i, ent;
    for (i = 0; i < id_count; i ++) {
        ecs_id_t id = id_array[i];
        ecs_id_record_t *id_idr = NULL;
        const ecs_type_info_t *ti = NULL;
        bool has_override = false;
        ecs_entity_t override_base = 0;
        const ecs_reachable_ids_t *isa_reachable = NULL;

        const ecs_event_id_record_t *eidr = get_triggers_for_id(evt, id);
        if (eidr) {
            id_idr = eidr->idr;

            /* Event id record should have ensured id record */
            ecs_assert(id_idr != NULL, ECS_INTERNAL_ERROR, NULL);
        } else {
            id_idr = flecs_get_id_record(world, id);
        }

        if (!id_idr) {
            if (ECS_HAS_ROLE(id, CASE)) {
                id_idr = flecs_get_id_record(world, 
                    ecs_case(ECS_PAIR_FIRST(id), EcsWildcard));
                if (!id_idr) {
                    return;
                }
            } else {
                /* If there's no id record there can't be anything to notify for */
                return;
            }
        }

        /* Check if id is component, fetch pointer & size */
        if (!table_event && (ti = id_idr->type_info)) {
            ecs_table_t *src_table;
            int32_t src_row;

            /* Get data from This or source */
            if (source) {
                ecs_record_t *r = ecs_eis_get(world, source);
                src_table = r->table;
                src_row = ECS_RECORD_TO_ROW(r->row);
            } else {
                src_table = table;
                src_row = row;
            }

            ecs_column_t *src_columns = src_table->data.columns;

            /* Can't notify for id that we don't have */
            ecs_table_record_t *tr = ecs_table_cache_get(
                &id_idr->cache, src_table);
            ecs_assert(tr != NULL, ECS_INTERNAL_ERROR, NULL);

            int32_t column = tr->column;
            column = src_table->storage_map[column];
            if (column != -1) {
                ecs_size_t size = ti->size;
                it->columns[0] = column + 1;
                if (source) {
                    it->columns[0] = 0;
                }
                it->ptrs[0] = ecs_vector_get_t(src_columns[column].data,
                    size, ti->alignment, src_row);
                it->sizes[0] = size;
            }
        }

        if (source != 0) {
            ecs_table_t *dt = it->table;
            if (it_flags & EcsIterOtherTableIsDst) {
                dt = it->other_table;
            }

            if (dt && ecs_table_cache_get(&id_idr->cache, dt)) {
                has_override = true;
            }
        }

        it->ids[0] = id;
        it->event_id = id;

        if (!wildcard_event && !source && ti != NULL) {
            bool base_has_id = false;

            /* If component is added to table with IsA relationship, check if
             * it's an override. Skip if it's a set operation, as the set will
             * write the component value. */
            if (event == EcsOnAdd && table_flags & EcsTableHasIsA && !is_set) {
                isa_reachable = flecs_get_reachable(world, EcsIsA, table);
                ecs_table_t *reachable_table;
                if (isa_reachable && (reachable_table = isa_reachable->table)) {
                    ecs_table_record_t *tr = ecs_table_cache_get(&id_idr->cache,
                        reachable_table);
                    if (tr) {
                        override_base = isa_reachable->sources[tr->column];
                        copy_from_base(world, id_idr, override_base, it);
                    }
                }
            }

            /* Run UnSet for removed components */
            if (event == EcsOnRemove) {
                const ecs_map_t *evt_unset = get_triggers_for_event(
                    observable, EcsUnSet);
                if (evt_unset) {
                    /* If table has base and base has id, no UnSet is needed */
                    if (table_flags & EcsTableHasIsA) {
                        isa_reachable = flecs_get_reachable(
                            world, EcsIsA, table);
                        if (isa_reachable && isa_reachable->table) {
                            if (ecs_table_cache_get(&id_idr->cache, isa_reachable->table)) {
                                base_has_id = true;
                            }
                        }
                    }

                    if (!base_has_id) {
                        it->event = EcsUnSet;
                        emit_event(world, observable, it, evt_unset, 0, 0,
                            &id, 1, row, count, notify_self, false);
                        it->event = EcsOnRemove;
                    }
                }
            }
        }

        /* Populate array with wildcard ids to evaluate */
        ecs_id_t wids[5];
        ecs_id_t role = id & ECS_ROLE_MASK;
        int32_t wid_cur = 1;
        ecs_entity_t r = 0;
        if (role == ECS_PAIR || role == ECS_CASE) {
            r = ECS_PAIR_FIRST(id);
            ecs_entity_t o = ECS_PAIR_SECOND(id);
            wids[0] = role | ecs_entity_t_comb(o, EcsWildcard);
            wids[1] = role | ecs_entity_t_comb(EcsWildcard, r);
            wids[2] = role | ecs_entity_t_comb(EcsWildcard, EcsWildcard);
            wid_cur = 3;
        } else {
            wids[0] = EcsWildcard;
        }

        /* Always evaluate for EcsAny */
        wids[wid_cur] = EcsAny;
        wid_cur ++;

        /* Notify for id and derived wildcard ids */
        ecs_id_t wid;
        do {
            if (eidr) {
                const ecs_map_t *triggers;
                if (!relation) {
                    triggers = &eidr->triggers;
                } else {
                    triggers = get_trigger_set(eidr, &eidr->superset, relation);
                }

                if (triggers) {
                    notify_triggers(world, it, triggers, 0, has_override);
                }

                /* If entity triggers exist for event/id, check if the entities
                 * triggered for are being observed */
                notify_entity_triggers(world, it, eidr, 0, has_override);

                if (count && !table_event) {
                    ecs_record_t **rptrs = ecs_vector_get(
                        table->data.record_ptrs, ecs_record_t*, row);

                    /* Propagate event downwards */
                    for (ent = 0; ent < count; ent ++) {
                        ecs_record_t *record = rptrs[ent];
                        if (!record) {
                            goto next;
                        }

                        /* Only if entity used as acyclic relationship target */
                        uint32_t flags = ECS_RECORD_TO_ROW_FLAGS(record->row);
                        if (flags & EcsEntityObservedAcyclic) {
                            ecs_entity_t src = it->entities[ent];
                            if (!it->subjects[0]) {
                                it->subjects[0] = src;
                            }

                            propagate_emit(world, it, eidr, src, id_idr, 0);
                        }
                    }
                }
            }

        next:
            if (-- wid_cur < 0) {
                break;
            }

            wid = wids[wid_cur];
            ecs_assert(wid != 0, ECS_INTERNAL_ERROR, NULL);
            eidr = get_triggers_for_id(evt, wid);
        } while (true);

        if (wildcard_event) {
            continue;
        }

        /* If id is a pair of an acyclic relation and event is OnAdd / OnRemove,
         * generate events for reachable ids from parent(s) */
        if (table_event || (event == EcsOnAdd || event == EcsOnRemove)) {
            if (r != 0 && ECS_PAIR_SECOND(id) != EcsWildcard) {
                const ecs_reachable_ids_t *reachable = flecs_get_reachable(
                    world, r, table);

                if (reachable) {
                    emit_reachable_ids(world, observable, it, evt, r,
                        reachable, row, count, notify_self);
                }
            }
        }

        if (!source) {
            /* Generate OnSet events for new overrides and re-exposed components */
            if (ti && table_flags & EcsTableHasIsA) {
                if (event == EcsOnAdd && override_base && !is_set) {
                    /* When a component is overridden. usually an OnSet event 
                     * should not be emitted as the value of the override is the
                     * same as the underlying component.
                     * The only exception is when an IsA relationship is added
                     * for a base with an auto-override. In this scenario, the
                     * entity did not have have the component yet, so an OnSet
                     * event should be emitted for the new value. */

                    /* We already know this is an override, so isa_reachable
                     * should have been set. */
                    ecs_assert(isa_reachable != NULL, ECS_INTERNAL_ERROR, NULL);

                    /* We also already know that the id is in the reachable
                     * table, or it wouldn't be an override. What we still need
                     * to check is whether the source of the reachable component
                     * was added in this operation, as that will tell us whether
                     * a new component value was introduced. 
                     *
                     * If the previous table already had an IsA relation to the
                     * source, the component value was already there and no 
                     * OnSet would be needed. */
                    ecs_id_record_t *isa_idr = flecs_get_id_record(world,
                        ecs_pair(EcsIsA, override_base));
                    ecs_assert(isa_idr != NULL, ECS_INTERNAL_ERROR, NULL);

                    ecs_table_t *other_table = it->other_table;
                    if (!other_table || !ecs_table_cache_get(
                        &isa_idr->cache, other_table))
                    {
                        const ecs_map_t *evt_onset = get_triggers_for_event(
                            observable, EcsOnSet);

                        /* The previous table did not have an IsA relationship
                         * with the source, which means this is a new value. 
                         * Emit the OnSet event. */
                        it->event = EcsOnSet;
                        emit_event(world, observable, it, evt_onset, 0, 0,
                            &id, 1, row, count, notify_self, false);
                        it->event = EcsOnAdd;
                    }
                }

                /* If component is removed and table has IsA relationship, 
                 * generate OnSet event if component from base got re-exposed */
                if (event == EcsOnRemove) {
                    if (!isa_reachable) {
                        isa_reachable = flecs_get_reachable(
                            world, EcsIsA, table);
                    }

                    if (isa_reachable) {
                        const ecs_table_record_t *tr;
                        if ((tr = flecs_id_record_table(id_idr, isa_reachable->table))) {
                            /* Id has been exposed, throw OnSet for it */
                            const ecs_map_t *evt_onset = get_triggers_for_event(
                                observable, EcsOnSet);
                            ecs_entity_t src = isa_reachable->sources[tr->column];

                            it->event = EcsOnSet;
                            it->subjects[0] = src;
                            emit_event(world, observable, it, evt_onset, 0, src,
                                &id, 1, row, count, notify_self, false);
                            it->event = EcsOnRemove;
                            it->subjects[0] = 0;
                        }
                    }
                }
            }

            /* If IsA relation got added, instantiate for base */
            if (it->event == EcsOnAdd && r == EcsIsA) {
                ecs_entity_t base = ECS_PAIR_SECOND(id);
                ecs_check(
                    !ecs_has_id(world, ecs_get_alive(world, base), EcsFinal),
                    ECS_CONSTRAINT_VIOLATED, NULL);
                flecs_instantiate(
                    world, ECS_PAIR_SECOND(id), table, row, count);
            }
        }
    }

error:
    return;
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

    if (!(desc->flags & EcsIterTableOnly)) {
        entities = ecs_vector_first(table->data.entities, ecs_entity_t);
        entities = ECS_OFFSET(entities, row * ECS_SIZEOF(ecs_entity_t));
        if (!count) {
            count = ecs_table_count(table) - row;
        }
    }

    ecs_iter_t it = {
        .world = stage,
        .real_world = world,
        .table = table,
        .entities = entities,
        .term_count = 1,
        .other_table = desc->other_table,
        .offset = row,
        .count = count,
        .param = (void*)desc->param,
        .event = event
    };

    flecs_iter_init(&it, flecs_iter_cache_all);

    it.flags |= desc->flags | EcsIterIsValid;

    world->event_id ++;

    ecs_observable_t *observable = ecs_get_observable(desc->observable);
    ecs_check(observable != NULL, ECS_INVALID_PARAMETER, NULL);

    ecs_entity_t events[2] = {event, EcsWildcard};
    int32_t i;

    /* Iterate events (event + Wildcard) */
    for (i = 0; i < 2; i ++) {
        ecs_entity_t e = events[i];
        const ecs_map_t *evt = get_triggers_for_event(observable, e);
        if (!evt) {
            continue;
        }

        emit_event(world, observable, &it, evt, 0, 0, ids->array, ids->count, 
            row, count, true, i == 1);
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
        ecs_event_id_record_t **idt_ptr;
        while ((idt_ptr = ecs_map_next(&it, ecs_event_id_record_t*, NULL))) {
            ecs_event_id_record_t *idt = *idt_ptr;
            ecs_map_fini(&idt->triggers);
            ecs_map_fini(&idt->superset);
            ecs_map_fini(&idt->entity);
            ecs_map_fini(&idt->superset_entity);
        }
        ecs_map_fini(&et->event_ids);
    }

    flecs_sparse_free(observable->events);
}
