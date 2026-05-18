# Partial Reconfiguration Rules

Partial MIG reconfiguration is represented as a concrete slot patch, not only as
an abstract template transition.

## Rule

A source physical layout can be partially reconfigured into a target physical
layout when:

- at least one non-void source slot is preserved exactly as
  `(slotStart, slotEnd, profile)`;
- every target create slot is fully covered by the union of source delete slots;
- preserve slots do not overlap delete slots;
- live execution may delete only slots that are idle or safely drained;
- preserved slots keep their MIG UUIDs and may keep running pods.

If no slot is preserved, the transition is a full reconfiguration.

## Machine Source

The executable rule lives in:

```text
migrant_core/partial_reconfig.py
```

The action planner calls `build_partial_reconfig_plan(src_gpu, tgt_gpu)` on the
already materialized physical layouts. The function returns:

```text
deleteSlots
createSlots
preserveSlots
deleteSpec
createSpec
preserveSpec
```

The `*Spec` fields are the strings consumed by `fast-mig-node-agent patch-slots`.

## Human Table

The following table is the template-level partial-reconfiguration rule for the
current A100 7-slice template catalog.

This table answers:

```text
Can there exist at least one physical source layout and one physical target
layout where a local slot patch can preserve at least one MIG instance?
```

It does not choose the exact runtime slots. The planner must still compare the
actual observed source slots against the materialized target slots and emit a
concrete patch.

| Source template | Partial-reconfigurable target templates |
| --- | --- |
| `7` | - |
| `4+3` | `4+2+1`, `4+1+1+1`, `2+2+3`, `3+2+1+1`, `3+1+1+1+1` |
| `4+2+1` | `4+3`, `4+1+1+1`, `2+2+2+1`, `2+2+1+1+1`, `2+1+1+1+1+1`, `1+1+1+1+1+1+1` |
| `4+1+1+1` | `4+3`, `4+2+1`, `3+2+1`, `3+1+1+1`, `2+2+2+1`, `2+2+1+1+1`, `2+1+1+1+1+1`, `1+1+1+1+1+1+1` |
| `3+3` | `3+2+1`, `3+1+1+1` |
| `3+2+1` | `3+3`, `3+1+1+1` |
| `3+1+1+1` | `3+3`, `3+2+1` |
| `2+2+3` | `4+3`, `3+2+1+1`, `3+1+1+1+1`, `2+2+2+1`, `2+2+1+1+1`, `2+1+1+1+1+1` |
| `3+2+1+1` | `4+3`, `3+1+1+1`, `2+2+3`, `3+1+1+1+1`, `2+2+2+1`, `2+2+1+1+1`, `2+1+1+1+1+1`, `1+1+1+1+1+1+1` |
| `3+1+1+1+1` | `4+3`, `3+1+1+1`, `2+2+3`, `3+2+1+1`, `2+2+1+1+1`, `2+1+1+1+1+1`, `1+1+1+1+1+1+1` |
| `2+2+2+1` | `4+2+1`, `4+1+1+1`, `2+2+3`, `3+2+1+1`, `2+2+1+1+1`, `2+1+1+1+1+1`, `1+1+1+1+1+1+1` |
| `2+2+1+1+1` | `4+2+1`, `4+1+1+1`, `3+1+1+1`, `2+2+3`, `3+2+1+1`, `3+1+1+1+1`, `2+2+2+1`, `2+1+1+1+1+1`, `1+1+1+1+1+1+1` |
| `2+1+1+1+1+1` | `4+2+1`, `4+1+1+1`, `3+2+1`, `3+1+1+1`, `2+2+3`, `3+2+1+1`, `3+1+1+1+1`, `2+2+2+1`, `2+2+1+1+1`, `1+1+1+1+1+1+1` |
| `1+1+1+1+1+1+1` | `4+2+1`, `4+1+1+1`, `3+2+1`, `3+1+1+1`, `3+2+1+1`, `3+1+1+1+1`, `2+2+2+1`, `2+2+1+1+1`, `2+1+1+1+1+1` |

`2+2+3` is the canonical name used in the code for the same multiset often
written as `3+2+2`.

Generate the abstract template table with:

```bash
python3 k8s-extension-prototype/tools/list_partial_reconfig_rules.py
```

Generate JSON for scripts or paper tables with:

```bash
python3 k8s-extension-prototype/tools/list_partial_reconfig_rules.py --json
```

The table is generated from the same rule as the planner. It is not a separate
hand-maintained policy.
