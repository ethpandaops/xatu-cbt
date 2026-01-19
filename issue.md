# Storage Expiry Model Bug: Stale prev_base_state

## Problem

The `int_storage_slot_state_with_expiry` and `int_contract_storage_state_with_expiry` models used a stale `prev_base_state` value for expiry-only blocks, causing incorrect `active_slots` calculations (visible as "jaggies" in daily aggregations).

## Root Cause

When processing a batch of blocks, the old model:
1. Computed `prev_base_state` as a single value per address from before `bounds.start`
2. Used this same value for ALL expiry-only blocks in the batch

This is incorrect when base activity within the batch changes `active_slots` - expiry-only blocks after that change should use the updated value, not the stale pre-batch value.

## Fix

Replace `prev_base_state` CTE with ASOF LEFT JOIN to find the correct base state at each specific block.

---

## Test Cases

### Slot-Level Model (`int_storage_slot_state_with_expiry`)

**Test Address:** `0x06450dee7fd2fb8e39061434babcfc05599a6fb8`
**Batch Range:** `22206000 - 22206100`
**Policy:** `12m`

#### Reproduce Bug (OLD model logic)

```sql
-- OLD: Single prev_base_state for all expiry-only blocks
WITH
expiry_only AS (
    SELECT e.block_number, '0x06450dee7fd2fb8e39061434babcfc05599a6fb8' as address
    FROM (
        SELECT DISTINCT block_number
        FROM mainnet.int_storage_slot_expiry_12m FINAL
        WHERE address = '0x06450dee7fd2fb8e39061434babcfc05599a6fb8'
          AND block_number BETWEEN 22206000 AND 22206100
    ) e
    LEFT JOIN (
        SELECT block_number
        FROM mainnet.int_storage_slot_state FINAL
        WHERE address = '0x06450dee7fd2fb8e39061434babcfc05599a6fb8'
          AND block_number BETWEEN 22206000 AND 22206100
    ) b ON e.block_number = b.block_number
    WHERE b.block_number = 0
),
prev_base_state AS (
    SELECT
        argMax(active_slots, (block_number, updated_date_time)) as prev_base_active_slots
    FROM mainnet.int_storage_slot_state
    WHERE address = '0x06450dee7fd2fb8e39061434babcfc05599a6fb8'
      AND block_number < 22206000
)
SELECT
    e.block_number,
    'OLD' as model,
    p.prev_base_active_slots as base_active_slots
FROM expiry_only e
CROSS JOIN prev_base_state p
ORDER BY e.block_number
```

**Expected (buggy) result:** All rows have `base_active_slots = 65478565`

#### Verify Fix (NEW model logic)

```sql
-- NEW: ASOF join finds correct base state per block
WITH
expiry_only AS (
    SELECT e.block_number, '0x06450dee7fd2fb8e39061434babcfc05599a6fb8' as address
    FROM (
        SELECT DISTINCT block_number
        FROM mainnet.int_storage_slot_expiry_12m FINAL
        WHERE address = '0x06450dee7fd2fb8e39061434babcfc05599a6fb8'
          AND block_number BETWEEN 22206000 AND 22206100
    ) e
    LEFT JOIN (
        SELECT block_number
        FROM mainnet.int_storage_slot_state FINAL
        WHERE address = '0x06450dee7fd2fb8e39061434babcfc05599a6fb8'
          AND block_number BETWEEN 22206000 AND 22206100
    ) b ON e.block_number = b.block_number
    WHERE b.block_number = 0
)
SELECT
    e.block_number,
    'NEW' as model,
    s.block_number as base_from_block,
    s.active_slots as base_active_slots
FROM expiry_only e
ASOF LEFT JOIN (
    SELECT block_number, address, active_slots
    FROM mainnet.int_storage_slot_state FINAL
    WHERE address = '0x06450dee7fd2fb8e39061434babcfc05599a6fb8'
) s ON e.address = s.address AND e.block_number >= s.block_number
ORDER BY e.block_number
```

**Expected (fixed) result:** Each row has correct `base_active_slots` ranging from 65,486,895 to 65,504,706

#### Expected Differences

| Block | OLD (buggy) | NEW (fixed) | Difference |
|-------|-------------|-------------|------------|
| 22206032 | 65,478,565 | 65,486,895 | +8,330 |
| 22206053 | 65,478,565 | 65,493,090 | +14,525 |
| 22206071 | 65,478,565 | 65,498,010 | +19,445 |
| 22206076 | 65,478,565 | 65,499,650 | +21,085 |
| 22206087 | 65,478,565 | 65,502,431 | +23,866 |
| 22206097 | 65,478,565 | 65,504,706 | +26,141 |

---

### Contract-Level Model (`int_contract_storage_state_with_expiry`)

**Test Address:** `0xd6b0a6eaeabb0c750776b024f2ad2123dba567fa`
**Batch Range:** `21966116 - 22291075`
**Policy:** `1m`

#### Block History

| Block | Type | active_slots |
|-------|------|--------------|
| 21,966,216 | Base activity | 9 |
| 22,290,975 | Expiry only | - |

#### Reproduce Bug (OLD model logic)

```sql
-- OLD: prev_base_state from before batch
SELECT
    argMax(active_slots, (block_number, updated_date_time)) as prev_base_active_slots,
    argMax(block_number, (block_number, updated_date_time)) as from_block
FROM mainnet.int_storage_slot_state_by_address
WHERE address = '0xd6b0a6eaeabb0c750776b024f2ad2123dba567fa'
  AND block_number < 21966116
```

**Expected (buggy) result:** `prev_base_active_slots = 0` (no prior activity)

#### Verify Fix (NEW model logic)

```sql
-- NEW: ASOF join finds correct base state
SELECT
    22290975 as expiry_block,
    s.block_number as base_from_block,
    s.active_slots as base_active_slots
FROM (SELECT '0xd6b0a6eaeabb0c750776b024f2ad2123dba567fa' as address, 22290975 as block_number) e
ASOF LEFT JOIN (
    SELECT block_number, address, active_slots
    FROM mainnet.int_storage_slot_state_by_address FINAL
    WHERE address = '0xd6b0a6eaeabb0c750776b024f2ad2123dba567fa'
) s ON e.address = s.address AND e.block_number >= s.block_number
```

**Expected (fixed) result:** `base_active_slots = 9` (from block 21,966,216)

#### Expected Difference

| Model | base_active_slots | Source |
|-------|-------------------|--------|
| OLD (buggy) | 0 | prev_base_state (before batch) |
| NEW (fixed) | 9 | ASOF join (block 21,966,216) |

---

## Affected Tables (Require Full Backfill)

### Slot-Level Chain
- `int_storage_slot_state_with_expiry` ← CHANGED
- `int_storage_slot_state_with_expiry_by_address`
- `int_storage_slot_state_with_expiry_by_block`
- `fct_storage_slot_state_with_expiry_by_address_daily`
- `fct_storage_slot_state_with_expiry_by_address_hourly`
- `fct_storage_slot_state_with_expiry_by_block_daily`
- `fct_storage_slot_state_with_expiry_by_block_hourly`
- `fct_storage_slot_top_100_by_slots`
- `fct_storage_slot_top_100_by_bytes`

### Contract-Level Chain
- `int_contract_storage_state_with_expiry` ← CHANGED
- `int_contract_storage_state_with_expiry_by_address`
- `int_contract_storage_state_with_expiry_by_block`
- `fct_contract_storage_state_with_expiry_by_address_daily`
- `fct_contract_storage_state_with_expiry_by_address_hourly`
- `fct_contract_storage_state_with_expiry_by_block_daily`
- `fct_contract_storage_state_with_expiry_by_block_hourly`

**Total: 16 tables**
