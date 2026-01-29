# app/logic/queries.py
# Version 5.3 — 2026-01-16
# - Added detailed timing logging to identify performance bottlenecks
# - Removed LastEntryDate from get_bulk_operations() for performance
# - Added get_operation_last_entries() for on-demand loading
#
# SQL queries for The Queue
# Uses OpCode-based work cell filtering
# Material status: star (full demand), check (job ok), partial, none
# ResourceID from ResourceTimeUsed (actual scheduled resource)
# CapabilityID from JobOpDtl (scheduling option)

import json
import os
import time
from sqlalchemy import text
from decimal import Decimal
from app.config import get_engine

# Timing flag - set to True to enable detailed query timing
TIMING_ENABLED = True

def log_timing(label, elapsed):
    """Log timing if enabled."""
    if TIMING_ENABLED:
        print(f"[TIMING] {label}: {elapsed:.3f}s")

# Load work cell configuration
def load_workcells():
    """Load work cell configuration from JSON file."""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'config', 'workcells.json'
    )
    with open(config_path, 'r') as f:
        return json.load(f)['workcells']

WORKCELLS = load_workcells()


def sql_query(query, params=None):
    """Execute SQL query and return list of dicts with Decimal conversion."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        cols = result.keys()
        rows = [dict(zip(cols, row)) for row in result.fetchall()]
        # Convert Decimal to float
        for row in rows:
            for k, v in row.items():
                if isinstance(v, Decimal):
                    row[k] = float(v)
        return rows


def get_workcells():
    """Return list of work cells for the home page."""
    return [
        {'id': key, 'name': val['name']}
        for key, val in WORKCELLS.items()
    ]


def get_workcell_ops(workcell_id):
    """Get the operation codes for a work cell."""
    if workcell_id not in WORKCELLS:
        return []
    return WORKCELLS[workcell_id]['ops']


def get_workcell_config(workcell_id):
    """Get the full configuration for a work cell."""
    if workcell_id not in WORKCELLS:
        return None
    return WORKCELLS[workcell_id]


def get_materials_for_workcell(workcell_id):
    """
    Get all unique material part numbers for jobs in a workcell.
    Used for the material filter dropdown.
    Includes materials from backflush operations (same logic as detail panel).
    Returns list of dicts with PartNum and PartDescription.
    """
    ops = get_workcell_ops(workcell_id)
    if not ops:
        return []
    
    placeholders = ', '.join([f':op{i}' for i in range(len(ops))])
    params = {f'op{i}': op for i, op in enumerate(ops)}
    
    # This query finds materials linked to:
    # 1. The visible (quantity) operations in this workcell
    # 2. Any backflush operations that precede those quantity operations
    query = f"""
        WITH VisibleOps AS (
            -- Get the visible (quantity) operations for this workcell
            SELECT jo.Company, jo.JobNum, jo.AssemblySeq, jo.OprSeq
            FROM Erp.JobOper jo
            INNER JOIN Erp.JobHead jh ON jo.Company = jh.Company AND jo.JobNum = jh.JobNum
            WHERE jh.JobComplete = 0
              AND jh.JobReleased = 1
              AND jo.OpCode IN ({placeholders})
              AND jo.OpComplete = 0
              AND jo.LaborEntryMethod != 'B'
        ),
        BackflushOps AS (
            -- For each visible op, find preceding backflush ops (excluding PAINT)
            SELECT jo.Company, jo.JobNum, jo.AssemblySeq, jo.OprSeq
            FROM Erp.JobOper jo
            INNER JOIN VisibleOps vo 
                ON jo.Company = vo.Company 
                AND jo.JobNum = vo.JobNum 
                AND jo.AssemblySeq = vo.AssemblySeq
            WHERE jo.LaborEntryMethod = 'B'
              AND jo.OpCode != 'PAINT'
              AND jo.OprSeq < vo.OprSeq
              AND jo.OprSeq > ISNULL(
                  (SELECT MAX(jo2.OprSeq) 
                   FROM Erp.JobOper jo2 
                   WHERE jo2.Company = vo.Company 
                     AND jo2.JobNum = vo.JobNum 
                     AND jo2.AssemblySeq = vo.AssemblySeq
                     AND jo2.OprSeq < vo.OprSeq 
                     AND jo2.LaborEntryMethod != 'B'), 0)
        ),
        AllRelevantOps AS (
            SELECT Company, JobNum, AssemblySeq, OprSeq FROM VisibleOps
            UNION
            SELECT Company, JobNum, AssemblySeq, OprSeq FROM BackflushOps
        )
        SELECT DISTINCT jm.PartNum, 
               REPLACE(p.PartDescription, ' - die billet ungrooved', '') AS PartDescription
        FROM Erp.JobMtl jm
        INNER JOIN AllRelevantOps aro 
            ON jm.Company = aro.Company 
            AND jm.JobNum = aro.JobNum 
            AND jm.AssemblySeq = aro.AssemblySeq
            AND jm.RelatedOperation = aro.OprSeq
        LEFT JOIN Erp.Part p
            ON jm.Company = p.Company AND jm.PartNum = p.PartNum
        WHERE jm.RequiredQty > 0
        ORDER BY PartDescription, jm.PartNum
    """
    
    return sql_query(query, params)


def get_bulk_operations(job_nums):
    """
    Get all operations for a list of jobs in one query.
    Returns dict keyed by JobNum.
    Uses ResourceTimeUsed for actual scheduled ResourceID.
    Uses JobOpDtl for CapabilityID (scheduling option).
    
    For labor entry, we need ResourceGrpID and JCDept with fallback chain:
    1. JobOpDtl.ResourceGrpID
    2. Resource table lookup from JobOpDtl.ResourceID
    3. Resource table lookup from ResourceTimeUsed.ResourceID
    
    NOTE: LastEntryDate removed for performance - LaborDtl lookup was causing
    234K reads. Now loaded on-demand via get_operation_last_entries().
    """
    if not job_nums:
        return {}
    
    placeholders = ', '.join([f':j{i}' for i in range(len(job_nums))])
    params = {f'j{i}': jn for i, jn in enumerate(job_nums)}
    
    query = f"""
        SELECT jo.JobNum, jo.OprSeq, jo.OpCode, jo.OpDesc, jo.QtyCompleted, 
               CAST(jo.OpComplete AS INT) AS OpComplete, jo.ProdStandard, jo.AssemblySeq,
               -- For LABOR ENTRY: ResourceGrpID with fallback chain
               COALESCE(
                   NULLIF(jod.ResourceGrpID, ''), 
                   res_from_jod.ResourceGrpID,
                   res_from_rtu.ResourceGrpID
               ) AS ResourceGrpID, 
               -- For LABOR ENTRY: ResourceID with fallback chain
               COALESCE(
                   NULLIF(jod.ResourceID, ''), 
                   r.ResourceID,
                   rtu.ResourceID
               ) AS ResourceID,
               -- For LABOR ENTRY: JCDept from ResourceGroup with fallback chain
               COALESCE(
                   rg_from_jod_grp.JCDept, 
                   rg_from_jod_res.JCDept,
                   rg_from_rtu.JCDept
               ) AS JCDept,
               -- For DISPLAY/FILTER: Scheduled resource from ResourceTimeUsed
               rtu.ResourceID AS ScheduledResourceID,
               jod.CapabilityID
        FROM Erp.JobOper jo
        LEFT JOIN Erp.JobOpDtl jod ON jo.Company = jod.Company 
            AND jo.JobNum = jod.JobNum 
            AND jo.AssemblySeq = jod.AssemblySeq 
            AND jo.OprSeq = jod.OprSeq
        -- Get ResourceGroup directly from JobOpDtl.ResourceGrpID
        LEFT JOIN Erp.ResourceGroup rg_from_jod_grp ON jod.Company = rg_from_jod_grp.Company
            AND jod.ResourceGrpID = rg_from_jod_grp.ResourceGrpID
            AND jod.ResourceGrpID IS NOT NULL AND jod.ResourceGrpID != ''
        -- Get default Resource from ResourceGroup (first location resource)
        OUTER APPLY (
            SELECT TOP 1 ResourceID 
            FROM Erp.Resource 
            WHERE ResourceGrpID = jod.ResourceGrpID 
              AND Location = 1
        ) r
        -- If JobOpDtl has ResourceID but no ResourceGrpID, look up the group from Resource table
        LEFT JOIN Erp.Resource res_from_jod ON jod.Company = res_from_jod.Company
            AND jod.ResourceID = res_from_jod.ResourceID
            AND (jod.ResourceGrpID IS NULL OR jod.ResourceGrpID = '')
        LEFT JOIN Erp.ResourceGroup rg_from_jod_res ON res_from_jod.Company = rg_from_jod_res.Company
            AND res_from_jod.ResourceGrpID = rg_from_jod_res.ResourceGrpID
        -- Get scheduled resource from ResourceTimeUsed (for display/filtering AND as final fallback)
        OUTER APPLY (
            SELECT TOP 1 ResourceID
            FROM Erp.ResourceTimeUsed 
            WHERE Company = jo.Company 
              AND JobNum = jo.JobNum 
              AND AssemblySeq = jo.AssemblySeq 
              AND OprSeq = jo.OprSeq
        ) rtu
        -- Fallback: Get ResourceGrpID from Resource table using ResourceTimeUsed.ResourceID
        LEFT JOIN Erp.Resource res_from_rtu ON jo.Company = res_from_rtu.Company
            AND rtu.ResourceID = res_from_rtu.ResourceID
        LEFT JOIN Erp.ResourceGroup rg_from_rtu ON res_from_rtu.Company = rg_from_rtu.Company
            AND res_from_rtu.ResourceGrpID = rg_from_rtu.ResourceGrpID
        WHERE jo.JobNum IN ({placeholders})
          AND jo.LaborEntryMethod != 'B'
        ORDER BY jo.JobNum, jo.AssemblySeq DESC, jo.OprSeq ASC
    """
    
    rows = sql_query(query, params)
    
    # Group by JobNum
    result = {}
    for row in rows:
        jn = row['JobNum']
        if jn not in result:
            result[jn] = []
        result[jn].append(row)
    
    return result


def get_bulk_materials(job_nums, all_operations=None):
    """
    Get all materials with inventory for a list of jobs.

    Includes materials from preceding backflush operations up to the previous
    non-backflush (quantity) operation. Excludes materials from PAINT operations.

    Args:
        job_nums: List of job numbers to fetch materials for
        all_operations: Optional dict from get_bulk_operations() to avoid duplicate query

    Returns dict keyed by 'JobNum-AssemblySeq-OprSeq'.
    """
    if not job_nums:
        return {}

    placeholders = ', '.join([f':j{i}' for i in range(len(job_nums))])
    params = {f'j{i}': jn for i, jn in enumerate(job_nums)}

    # Step 1: Get operation info for backflush mapping
    # If we already have operations from get_bulk_operations, use that data
    t1 = time.time()
    if all_operations:
        # Flatten the operations dict and add LaborEntryMethod info
        # We need a separate lightweight query just for LaborEntryMethod since bulk_operations excludes backflush
        ops_query = f"""
            SELECT JobNum, AssemblySeq, OprSeq, OpCode, LaborEntryMethod
            FROM Erp.JobOper
            WHERE JobNum IN ({placeholders})
            ORDER BY JobNum, AssemblySeq, OprSeq
        """
        all_ops = sql_query(ops_query, params)
        log_timing(f"    4a. bulk_materials: ops query ({len(all_ops)} ops)", time.time() - t1)
    else:
        ops_query = f"""
            SELECT JobNum, AssemblySeq, OprSeq, OpCode, LaborEntryMethod
            FROM Erp.JobOper
            WHERE JobNum IN ({placeholders})
            ORDER BY JobNum, AssemblySeq, OprSeq
        """
        all_ops = sql_query(ops_query, params)
        log_timing(f"    4a. bulk_materials: ops query ({len(all_ops)} ops)", time.time() - t1)

    # Build a lookup for OpCode by (job, asm, opr) - used to filter PAINT in Python
    op_code_lookup = {(op['JobNum'], op['AssemblySeq'], op['OprSeq']): op['OpCode'] for op in all_ops}

    # Step 2: Build mapping of which operations' materials belong to which visible operation
    op_mapping = {}  # (job, asm, opr) -> list of (job, asm, opr) whose materials to include

    # Group operations by job and assembly
    ops_by_job_asm = {}
    for op in all_ops:
        key = (op['JobNum'], op['AssemblySeq'])
        if key not in ops_by_job_asm:
            ops_by_job_asm[key] = []
        ops_by_job_asm[key].append(op)

    # For each assembly, determine material ownership
    for (job_num, asm_seq), ops in ops_by_job_asm.items():
        pending_backflush = []

        for op in ops:
            opr_seq = op['OprSeq']
            is_backflush = op['LaborEntryMethod'] == 'B'
            is_paint = op['OpCode'] == 'PAINT'

            if is_backflush:
                if not is_paint:
                    pending_backflush.append((job_num, asm_seq, opr_seq))
            else:
                visible_key = (job_num, asm_seq, opr_seq)
                op_mapping[visible_key] = pending_backflush + [visible_key]
                pending_backflush = []

    # Step 3: Get materials - REMOVED JobOper join for performance
    # We filter PAINT operations in Python using op_code_lookup
    t2 = time.time()
    mtl_query = f"""
        SELECT jm.JobNum, jm.AssemblySeq, jm.RelatedOperation AS OprSeq,
               jm.MtlSeq, jm.PartNum, p.PartDescription, jm.RequiredQty,
               ISNULL(jm.IUM, p.IUM) AS ReqUOM, p.IUM AS OnHandUOM
        FROM Erp.JobMtl jm
        LEFT JOIN Erp.Part p ON jm.Company = p.Company AND jm.PartNum = p.PartNum
        WHERE jm.JobNum IN ({placeholders})
          AND jm.RequiredQty > 0
        ORDER BY jm.JobNum, jm.AssemblySeq, jm.RelatedOperation, jm.MtlSeq
    """

    materials_raw = sql_query(mtl_query, params)

    # Filter out PAINT operations in Python (much faster than SQL join)
    materials = []
    for m in materials_raw:
        op_key = (m['JobNum'], m['AssemblySeq'], m['OprSeq'])
        op_code = op_code_lookup.get(op_key, '')
        if op_code != 'PAINT':
            m['SourceOpCode'] = op_code
            materials.append(m)

    log_timing(f"    4b. bulk_materials: materials query ({len(materials_raw)} raw, {len(materials)} filtered)", time.time() - t2)

    if not materials:
        return {}
    
    # Step 4: Get unique part numbers and batch lookup inventory
    t3 = time.time()
    part_nums = list(set(m['PartNum'] for m in materials))

    inv_placeholders = ', '.join([f':p{i}' for i in range(len(part_nums))])
    inv_params = {f'p{i}': pn for i, pn in enumerate(part_nums)}

    inv_query = f"""
        SELECT PartNum, SUM(OnHandQty) AS OnHandQty, SUM(DemandQty) AS DemandQty
        FROM Erp.PartQty
        WHERE PartNum IN ({inv_placeholders})
        GROUP BY PartNum
    """

    inv_data = sql_query(inv_query, inv_params)
    inv_map = {row['PartNum']: row for row in inv_data}
    log_timing(f"    4c. bulk_materials: inventory query ({len(part_nums)} parts)", time.time() - t3)
    
    # Step 5: Group materials by their source operation
    mtl_by_source = {}
    for m in materials:
        inv = inv_map.get(m['PartNum'], {})
        m['OnHandQty'] = inv.get('OnHandQty', 0) or 0
        m['DemandQty'] = inv.get('DemandQty', 0) or 0
        
        source_key = (m['JobNum'], m['AssemblySeq'], m['OprSeq'])
        if source_key not in mtl_by_source:
            mtl_by_source[source_key] = []
        mtl_by_source[source_key].append(m)
    
    # Step 6: Build final result - assign materials to visible operations
    result = {}
    for visible_key, source_ops in op_mapping.items():
        job_num, asm_seq, opr_seq = visible_key
        result_key = f"{job_num}-{asm_seq}-{opr_seq}"
        result[result_key] = []
        
        for source_op in source_ops:
            if source_op in mtl_by_source:
                result[result_key].extend(mtl_by_source[source_op])
    
    return result


def get_jobs_with_details(workcell_id):
    """
    Get jobs for workcell. Operations and materials loaded on-demand via API.
    MtlStatus is calculated in the main query via CTEs.
    """
    t_total = time.time()

    # 1. Get main job list (includes MtlStatus from CTEs)
    t1 = time.time()
    jobs = get_jobs_for_workcell(workcell_id)
    log_timing(f"  1. get_jobs_for_workcell ({len(jobs)} jobs)", time.time() - t1)

    if not jobs:
        return []

    # 2. SKIP bulk operations - load on-demand via /api/job/<job>/<asm>/<opr>
    log_timing(f"  2. bulk_operations: SKIPPED (on-demand)", 0)

    # 3. SKIP bulk materials - load on-demand via /api/job/<job>/<asm>/<opr>
    # MtlStatus is already calculated in get_jobs_for_workcell() via CTEs
    log_timing(f"  3. bulk_materials: SKIPPED (on-demand)", 0)

    # 4. Set empty JSON for on-demand loading
    t4 = time.time()
    for job in jobs:
        job['OperationsJSON'] = '[]'  # Empty - loaded on-demand
        job['MaterialsJSON'] = '[]'   # Empty - loaded on-demand
    log_timing(f"  4. set empty JSON", time.time() - t4)

    log_timing(f"  TOTAL get_jobs_with_details", time.time() - t_total)
    return jobs


def get_jobs_using_material(workcell_id, material_partnum):
    """
    Get job keys (JobNum-AssemblySeq-OprSeq) that use a specific material.
    Used for filtering by material selection.
    Includes materials from backflush operations (same logic as detail panel).
    """
    ops = get_workcell_ops(workcell_id)
    if not ops:
        return []
    
    placeholders = ', '.join([f':op{i}' for i in range(len(ops))])
    params = {f'op{i}': op for i, op in enumerate(ops)}
    params['material'] = material_partnum
    
    query = f"""
        WITH VisibleOps AS (
            -- Get the visible (quantity) operations for this workcell
            SELECT jo.Company, jo.JobNum, jo.AssemblySeq, jo.OprSeq
            FROM Erp.JobOper jo
            INNER JOIN Erp.JobHead jh ON jo.Company = jh.Company AND jo.JobNum = jh.JobNum
            WHERE jh.JobComplete = 0
              AND jh.JobReleased = 1
              AND jo.OpCode IN ({placeholders})
              AND jo.OpComplete = 0
              AND jo.LaborEntryMethod != 'B'
        ),
        BackflushOps AS (
            -- For each visible op, find preceding backflush ops (excluding PAINT)
            SELECT jo.Company, jo.JobNum, jo.AssemblySeq, jo.OprSeq, vo.OprSeq AS OwnerOprSeq
            FROM Erp.JobOper jo
            INNER JOIN VisibleOps vo 
                ON jo.Company = vo.Company 
                AND jo.JobNum = vo.JobNum 
                AND jo.AssemblySeq = vo.AssemblySeq
            WHERE jo.LaborEntryMethod = 'B'
              AND jo.OpCode != 'PAINT'
              AND jo.OprSeq < vo.OprSeq
              AND jo.OprSeq > ISNULL(
                  (SELECT MAX(jo2.OprSeq) 
                   FROM Erp.JobOper jo2 
                   WHERE jo2.Company = vo.Company 
                     AND jo2.JobNum = vo.JobNum 
                     AND jo2.AssemblySeq = vo.AssemblySeq
                     AND jo2.OprSeq < vo.OprSeq 
                     AND jo2.LaborEntryMethod != 'B'), 0)
        ),
        AllRelevantOps AS (
            -- Visible ops own themselves
            SELECT Company, JobNum, AssemblySeq, OprSeq, OprSeq AS OwnerOprSeq FROM VisibleOps
            UNION
            -- Backflush ops map to their owner
            SELECT Company, JobNum, AssemblySeq, OprSeq, OwnerOprSeq FROM BackflushOps
        )
        SELECT DISTINCT 
            aro.JobNum + '-' + CAST(aro.AssemblySeq AS VARCHAR) + '-' + CAST(aro.OwnerOprSeq AS VARCHAR) AS JobKey
        FROM Erp.JobMtl jm
        INNER JOIN AllRelevantOps aro 
            ON jm.Company = aro.Company 
            AND jm.JobNum = aro.JobNum 
            AND jm.AssemblySeq = aro.AssemblySeq
            AND jm.RelatedOperation = aro.OprSeq
        WHERE jm.PartNum = :material
          AND jm.RequiredQty > 0
    """
    
    result = sql_query(query, params)
    return [row['JobKey'] for row in result]


def get_colors_for_workcell(workcell_id):
    """
    Get all unique finish colors for jobs in a workcell.
    Used for the color filter dropdown on POWDER.
    Returns list of dicts with FinishColor.
    """
    ops = get_workcell_ops(workcell_id)
    if not ops:
        return []
    
    placeholders = ', '.join([f':op{i}' for i in range(len(ops))])
    params = {f'op{i}': op for i, op in enumerate(ops)}
    
    query = f"""
        SELECT DISTINCT joud.FinishColor_c AS FinishColor
        FROM Erp.JobOper jo
        INNER JOIN Erp.JobHead jh ON jo.Company = jh.Company AND jo.JobNum = jh.JobNum
        LEFT JOIN Erp.JobOper_UD joud ON jo.SysRowID = joud.ForeignSysRowID
        WHERE jh.JobComplete = 0
          AND jh.JobReleased = 1
          AND jo.OpCode IN ({placeholders})
          AND jo.OpComplete = 0
          AND jo.LaborEntryMethod != 'B'
          AND joud.FinishColor_c IS NOT NULL
          AND joud.FinishColor_c != ''
        ORDER BY joud.FinishColor_c
    """
    
    return sql_query(query, params)


def get_jobs_using_color(workcell_id, color):
    """
    Get job keys (JobNum-AssemblySeq-OprSeq) that have a specific finish color.
    Used for filtering by color selection.
    """
    ops = get_workcell_ops(workcell_id)
    if not ops:
        return []
    
    placeholders = ', '.join([f':op{i}' for i in range(len(ops))])
    params = {f'op{i}': op for i, op in enumerate(ops)}
    params['color'] = color
    
    query = f"""
        SELECT DISTINCT 
            jo.JobNum + '-' + CAST(jo.AssemblySeq AS VARCHAR) + '-' + CAST(jo.OprSeq AS VARCHAR) AS JobKey
        FROM Erp.JobOper jo
        INNER JOIN Erp.JobHead jh ON jo.Company = jh.Company AND jo.JobNum = jh.JobNum
        LEFT JOIN Erp.JobOper_UD joud ON jo.SysRowID = joud.ForeignSysRowID
        WHERE jh.JobComplete = 0
          AND jh.JobReleased = 1
          AND jo.OpCode IN ({placeholders})
          AND jo.OpComplete = 0
          AND jo.LaborEntryMethod != 'B'
          AND joud.FinishColor_c = :color
    """
    
    result = sql_query(query, params)
    return [row['JobKey'] for row in result]


def get_resources_for_workcell(workcell_id):
    """
    Get all unique ResourceIDs for jobs in a workcell.
    Uses ResourceTimeUsed for actual scheduled resources.
    Used for the resource filter dropdown on Mill-Lathe.
    Returns list of dicts with ResourceID.
    """
    ops = get_workcell_ops(workcell_id)
    if not ops:
        return []
    
    placeholders = ', '.join([f':op{i}' for i in range(len(ops))])
    params = {f'op{i}': op for i, op in enumerate(ops)}
    
    query = f"""
        SELECT DISTINCT rtu.ResourceID
        FROM Erp.JobOper jo
        INNER JOIN Erp.JobHead jh ON jo.Company = jh.Company AND jo.JobNum = jh.JobNum
        INNER JOIN Erp.ResourceTimeUsed rtu ON jo.Company = rtu.Company 
            AND jo.JobNum = rtu.JobNum 
            AND jo.AssemblySeq = rtu.AssemblySeq 
            AND jo.OprSeq = rtu.OprSeq
        WHERE jh.JobComplete = 0
          AND jh.JobReleased = 1
          AND jo.OpCode IN ({placeholders})
          AND jo.OpComplete = 0
          AND jo.LaborEntryMethod != 'B'
          AND rtu.ResourceID IS NOT NULL
          AND rtu.ResourceID != ''
        ORDER BY rtu.ResourceID
    """
    
    return sql_query(query, params)


def get_capabilities_for_workcell(workcell_id):
    """
    Get all unique CapabilityIDs for jobs in a workcell.
    Used for the capability filter dropdown on Mill-Lathe.
    Returns list of dicts with CapabilityID.
    """
    ops = get_workcell_ops(workcell_id)
    if not ops:
        return []
    
    placeholders = ', '.join([f':op{i}' for i in range(len(ops))])
    params = {f'op{i}': op for i, op in enumerate(ops)}
    
    query = f"""
        SELECT DISTINCT jod.CapabilityID
        FROM Erp.JobOper jo
        INNER JOIN Erp.JobHead jh ON jo.Company = jh.Company AND jo.JobNum = jh.JobNum
        LEFT JOIN Erp.JobOpDtl jod ON jo.Company = jod.Company 
            AND jo.JobNum = jod.JobNum 
            AND jo.AssemblySeq = jod.AssemblySeq 
            AND jo.OprSeq = jod.OprSeq
        WHERE jh.JobComplete = 0
          AND jh.JobReleased = 1
          AND jo.OpCode IN ({placeholders})
          AND jo.OpComplete = 0
          AND jo.LaborEntryMethod != 'B'
          AND jod.CapabilityID IS NOT NULL
          AND jod.CapabilityID != ''
        ORDER BY jod.CapabilityID
    """
    
    return sql_query(query, params)


def get_jobs_using_resource(workcell_id, resource_id):
    """
    Get job keys (JobNum-AssemblySeq-OprSeq) that use a specific ResourceID.
    Uses ResourceTimeUsed for actual scheduled resources.
    Used for filtering by resource selection.
    """
    ops = get_workcell_ops(workcell_id)
    if not ops:
        return []
    
    placeholders = ', '.join([f':op{i}' for i in range(len(ops))])
    params = {f'op{i}': op for i, op in enumerate(ops)}
    params['resource'] = resource_id
    
    query = f"""
        SELECT DISTINCT 
            jo.JobNum + '-' + CAST(jo.AssemblySeq AS VARCHAR) + '-' + CAST(jo.OprSeq AS VARCHAR) AS JobKey
        FROM Erp.JobOper jo
        INNER JOIN Erp.JobHead jh ON jo.Company = jh.Company AND jo.JobNum = jh.JobNum
        INNER JOIN Erp.ResourceTimeUsed rtu ON jo.Company = rtu.Company 
            AND jo.JobNum = rtu.JobNum 
            AND jo.AssemblySeq = rtu.AssemblySeq 
            AND jo.OprSeq = rtu.OprSeq
        WHERE jh.JobComplete = 0
          AND jh.JobReleased = 1
          AND jo.OpCode IN ({placeholders})
          AND jo.OpComplete = 0
          AND jo.LaborEntryMethod != 'B'
          AND rtu.ResourceID = :resource
    """
    
    result = sql_query(query, params)
    return [row['JobKey'] for row in result]


def get_jobs_using_capability(workcell_id, capability_id):
    """
    Get job keys (JobNum-AssemblySeq-OprSeq) that use a specific CapabilityID.
    Used for filtering by capability selection.
    """
    ops = get_workcell_ops(workcell_id)
    if not ops:
        return []
    
    placeholders = ', '.join([f':op{i}' for i in range(len(ops))])
    params = {f'op{i}': op for i, op in enumerate(ops)}
    params['capability'] = capability_id
    
    query = f"""
        SELECT DISTINCT 
            jo.JobNum + '-' + CAST(jo.AssemblySeq AS VARCHAR) + '-' + CAST(jo.OprSeq AS VARCHAR) AS JobKey
        FROM Erp.JobOper jo
        INNER JOIN Erp.JobHead jh ON jo.Company = jh.Company AND jo.JobNum = jh.JobNum
        LEFT JOIN Erp.JobOpDtl jod ON jo.Company = jod.Company 
            AND jo.JobNum = jod.JobNum 
            AND jo.AssemblySeq = jod.AssemblySeq 
            AND jo.OprSeq = jod.OprSeq
        WHERE jh.JobComplete = 0
          AND jh.JobReleased = 1
          AND jo.OpCode IN ({placeholders})
          AND jo.OpComplete = 0
          AND jo.LaborEntryMethod != 'B'
          AND jod.CapabilityID = :capability
    """
    
    result = sql_query(query, params)
    return [row['JobKey'] for row in result]


def get_last_checkin(part_num, op_code=None):
    """
    Get the last labor check-in for a part number at a specific operation.
    Matches against JobAsmbl.PartNum so it works for both header parts and sub-assemblies.
    """
    if op_code:
        query = """
            SELECT TOP 1
                ld.EmployeeNum,
                e.Name AS EmployeeName,
                ld.LaborQty,
                CONVERT(VARCHAR(10), ld.ClockInDate, 23) AS ClockInDate,
                ld.ClockInTime,
                ld.JobNum,
                jo.OpCode
            FROM Erp.LaborDtl ld
            INNER JOIN Erp.JobAsmbl ja ON ld.Company = ja.Company 
                AND ld.JobNum = ja.JobNum 
                AND ld.AssemblySeq = ja.AssemblySeq
            INNER JOIN Erp.JobOper jo ON ld.Company = jo.Company AND ld.JobNum = jo.JobNum 
                AND ld.AssemblySeq = jo.AssemblySeq AND ld.OprSeq = jo.OprSeq
            LEFT JOIN Erp.EmpBasic e ON ld.Company = e.Company AND ld.EmployeeNum = e.EmpID
            WHERE ja.PartNum = :part_num
              AND jo.OpCode = :op_code
              AND ld.LaborQty > 0
            ORDER BY ld.ClockInDate DESC, ld.ClockInTime DESC
        """
        result = sql_query(query, {'part_num': part_num, 'op_code': op_code})
    else:
        query = """
            SELECT TOP 1
                ld.EmployeeNum,
                e.Name AS EmployeeName,
                ld.LaborQty,
                CONVERT(VARCHAR(10), ld.ClockInDate, 23) AS ClockInDate,
                ld.ClockInTime,
                ld.JobNum
            FROM Erp.LaborDtl ld
            INNER JOIN Erp.JobAsmbl ja ON ld.Company = ja.Company 
                AND ld.JobNum = ja.JobNum 
                AND ld.AssemblySeq = ja.AssemblySeq
            LEFT JOIN Erp.EmpBasic e ON ld.Company = e.Company AND ld.EmployeeNum = e.EmpID
            WHERE ja.PartNum = :part_num
              AND ld.LaborQty > 0
            ORDER BY ld.ClockInDate DESC, ld.ClockInTime DESC
        """
        result = sql_query(query, {'part_num': part_num})
    return result[0] if result else None


def get_jobs_for_workcell(workcell_id):
    """
    Get jobs ready for a specific work cell.
    
    Filtering rules:
    1. Job not complete, released
    2. Operation uses work cell's op codes, not complete, not backflush
    3. If NOT first op on assembly: prior op must have QtyCompleted > 0
    4. If AssemblySeq = 0: ALL sub-assemblies (AssemblySeq > 0) must be complete
    
    Material status logic (based on OnHand vs RequiredQty vs DemandQty):
    - 'star' = OnHand >= total DemandQty (rock star - covers all shop demand)
    - 'check' = OnHand >= this job's RequiredQty but < DemandQty (job ok)
    - 'partial' = OnHand > 0 but < RequiredQty (can do some)
    - 'none' = OnHand = 0 or no materials needed
    """
    ops = get_workcell_ops(workcell_id)
    if not ops:
        return []
    
    # Build the IN clause for op codes
    placeholders = ', '.join([f':op{i}' for i in range(len(ops))])
    params = {f'op{i}': op for i, op in enumerate(ops)}
    
    query = f"""
        WITH PriorOpQty AS (
            SELECT 
                jo.Company,
                jo.JobNum,
                jo.AssemblySeq,
                jo.OprSeq,
                -- Get qty completed from prior non-backflush operation (within same assembly)
                ISNULL(
                    (SELECT TOP 1 jo_prior.QtyCompleted
                     FROM Erp.JobOper jo_prior
                     WHERE jo_prior.Company = jo.Company
                       AND jo_prior.JobNum = jo.JobNum
                       AND jo_prior.AssemblySeq = jo.AssemblySeq
                       AND jo_prior.OprSeq < jo.OprSeq
                       AND jo_prior.LaborEntryMethod != 'B'
                     ORDER BY jo_prior.OprSeq DESC),
                    0
                ) AS QtyFromPrior,
                -- Is this the first non-backflush operation on this assembly?
                CASE WHEN NOT EXISTS (
                    SELECT 1 FROM Erp.JobOper jo_prior
                    WHERE jo_prior.Company = jo.Company
                      AND jo_prior.JobNum = jo.JobNum
                      AND jo_prior.AssemblySeq = jo.AssemblySeq
                      AND jo_prior.OprSeq < jo.OprSeq
                      AND jo_prior.LaborEntryMethod != 'B'
                ) THEN 1 ELSE 0 END AS IsFirstOp
            FROM Erp.JobOper jo
            WHERE jo.LaborEntryMethod != 'B'
        ),
        SubAsmComplete AS (
            -- Check if all sub-assemblies have qty completed on their last non-backflush operation
            SELECT 
                jo.Company,
                jo.JobNum,
                -- 1 if all sub-assembly last non-BF ops have QtyCompleted > 0, 0 otherwise
                CASE WHEN NOT EXISTS (
                    SELECT 1 
                    FROM (
                        -- Get the last non-backflush operation for each sub-assembly
                        SELECT Company, JobNum, AssemblySeq, MAX(OprSeq) AS LastOprSeq
                        FROM Erp.JobOper
                        WHERE AssemblySeq > 0
                          AND LaborEntryMethod != 'B'
                        GROUP BY Company, JobNum, AssemblySeq
                    ) last_ops
                    INNER JOIN Erp.JobOper jo_last 
                        ON last_ops.Company = jo_last.Company
                        AND last_ops.JobNum = jo_last.JobNum
                        AND last_ops.AssemblySeq = jo_last.AssemblySeq
                        AND last_ops.LastOprSeq = jo_last.OprSeq
                    WHERE last_ops.Company = jo.Company
                      AND last_ops.JobNum = jo.JobNum
                      AND jo_last.QtyCompleted = 0
                ) THEN 1 ELSE 0 END AS AllSubAsmsReady
            FROM Erp.JobOper jo
            GROUP BY jo.Company, jo.JobNum
        ),
        -- Map each operation to its "owner" (the next non-backflush operation)
        -- Backflush ops get assigned to the next quantity op; quantity ops own themselves
        OpOwnership AS (
            SELECT 
                jo.Company,
                jo.JobNum,
                jo.AssemblySeq,
                jo.OprSeq,
                jo.OpCode,
                jo.LaborEntryMethod,
                -- Find the next non-backflush operation (the owner)
                CASE 
                    WHEN jo.LaborEntryMethod != 'B' THEN jo.OprSeq  -- Qty ops own themselves
                    ELSE (
                        SELECT TOP 1 jo_next.OprSeq
                        FROM Erp.JobOper jo_next
                        WHERE jo_next.Company = jo.Company
                          AND jo_next.JobNum = jo.JobNum
                          AND jo_next.AssemblySeq = jo.AssemblySeq
                          AND jo_next.OprSeq > jo.OprSeq
                          AND jo_next.LaborEntryMethod != 'B'
                        ORDER BY jo_next.OprSeq ASC
                    )
                END AS OwnerOprSeq
            FROM Erp.JobOper jo
        ),
        -- Aggregate materials by owner operation, excluding PAINT
        MaterialAgg AS (
            SELECT 
                jm.Company,
                jm.JobNum,
                jm.AssemblySeq,
                oo.OwnerOprSeq AS RelatedOperation,
                COUNT(*) AS TotalMtls,
                SUM(CASE 
                    WHEN ISNULL(pq.OnHandQty, 0) >= ISNULL(pq.DemandQty, jm.RequiredQty) THEN 1 
                    ELSE 0 
                END) AS StarMtls,
                SUM(CASE 
                    WHEN ISNULL(pq.OnHandQty, 0) >= jm.RequiredQty 
                         AND ISNULL(pq.OnHandQty, 0) < ISNULL(pq.DemandQty, jm.RequiredQty + 1) THEN 1 
                    ELSE 0 
                END) AS CheckMtls,
                SUM(CASE 
                    WHEN ISNULL(pq.OnHandQty, 0) > 0 
                         AND ISNULL(pq.OnHandQty, 0) < jm.RequiredQty THEN 1 
                    ELSE 0 
                END) AS PartialMtls,
                SUM(CASE 
                    WHEN ISNULL(pq.OnHandQty, 0) = 0 THEN 1 
                    ELSE 0 
                END) AS NoneMtls
            FROM Erp.JobMtl jm
            INNER JOIN OpOwnership oo 
                ON jm.Company = oo.Company 
                AND jm.JobNum = oo.JobNum 
                AND jm.AssemblySeq = oo.AssemblySeq
                AND jm.RelatedOperation = oo.OprSeq
            LEFT JOIN (
                SELECT Company, PartNum, 
                       SUM(OnHandQty) AS OnHandQty,
                       SUM(DemandQty) AS DemandQty
                FROM Erp.PartQty
                GROUP BY Company, PartNum
            ) pq ON jm.Company = pq.Company AND jm.PartNum = pq.PartNum
            WHERE jm.RequiredQty > 0
              AND oo.OpCode != 'PAINT'  -- Exclude PAINT operation materials
              AND oo.OwnerOprSeq IS NOT NULL  -- Only include if there's an owner
            GROUP BY jm.Company, jm.JobNum, jm.AssemblySeq, oo.OwnerOprSeq
        )
        SELECT 
            jh.JobNum,
            CASE WHEN jo.AssemblySeq > 0 THEN ja.PartNum ELSE jh.PartNum END AS PartNum,
            CASE WHEN jo.AssemblySeq > 0 THEN pa.PartDescription ELSE p.PartDescription END AS PartDescription,
            CASE WHEN jo.AssemblySeq > 0 THEN ja.RequiredQty ELSE jh.ProdQty END AS ProdQty,
            jh.SchedCode AS Priority,
            jo.OprSeq,
            jo.OpCode,
            jo.OpDesc,
            jo.AssemblySeq,
            jo.QtyCompleted AS QtyCompletedThisOp,
            jh.ProdQty - jo.QtyCompleted AS QtyLeft,
            jo.EstProdHours AS OpHours,
            jo.ProdStandard AS CycleTime,
            jo.CommentText AS Notes,
            joud.FinalLocation_c AS NextLocation,
            joud.Finish_c AS Material,
            joud.FinishColor_c AS FinishColor,
            joud.PrepTime_c AS PrepTime,
            joud.MachineLoadTime_c AS MachLoad,
            joud.MachineRunTime_c AS MachRun,
            joud.MachineUnloadTime_c AS MachUnload,
            joud.MachProgramNum_c AS MachProgram,
            poq.QtyFromPrior,
            poq.IsFirstOp,
            jh.ReqDueDate,
            jh.StartDate,
            jh.DueDate,
            DATEDIFF(day, GETDATE(), jh.ReqDueDate) AS DaysUntilDue,
            CASE 
                WHEN ma.TotalMtls IS NULL OR ma.TotalMtls = 0 THEN 'none'
                WHEN ma.NoneMtls > 0 THEN 'missing'
                WHEN ma.PartialMtls > 0 THEN 'partial'
                WHEN ma.CheckMtls > 0 THEN 'check'
                WHEN ma.StarMtls = ma.TotalMtls THEN 'star'
                ELSE 'none'
            END AS MtlStatus,
            ISNULL(ma.TotalMtls, 0) AS TotalMtls,
            xfr.XFileName AS PdfPath,
            -- Resource from ResourceTimeUsed (actual scheduled resource)
            rtu.ResourceID,
            -- Capability from JobOpDtl (scheduling option)
            jod.CapabilityID,
            -- Part on-hand quantity
            ISNULL(poh.OnHandQty, 0) AS PartOnHand
        FROM Erp.JobHead jh
        INNER JOIN Erp.JobOper jo 
            ON jh.Company = jo.Company AND jh.JobNum = jo.JobNum
        LEFT JOIN Erp.JobOper_UD joud
            ON jo.SysRowID = joud.ForeignSysRowID
        -- Get actual scheduled resource from ResourceTimeUsed
        OUTER APPLY (
            SELECT TOP 1 ResourceID 
            FROM Erp.ResourceTimeUsed 
            WHERE Company = jo.Company 
              AND JobNum = jo.JobNum 
              AND AssemblySeq = jo.AssemblySeq 
              AND OprSeq = jo.OprSeq
        ) rtu
        -- Get CapabilityID from JobOpDtl (scheduling option)
        OUTER APPLY (
            SELECT TOP 1 CapabilityID 
            FROM Erp.JobOpDtl 
            WHERE Company = jo.Company 
              AND JobNum = jo.JobNum 
              AND AssemblySeq = jo.AssemblySeq 
              AND OprSeq = jo.OprSeq
        ) jod
        INNER JOIN PriorOpQty poq
            ON jo.Company = poq.Company 
            AND jo.JobNum = poq.JobNum 
            AND jo.AssemblySeq = poq.AssemblySeq
            AND jo.OprSeq = poq.OprSeq
        INNER JOIN SubAsmComplete sac
            ON jo.Company = sac.Company
            AND jo.JobNum = sac.JobNum
        LEFT JOIN Erp.Part p 
            ON jh.Company = p.Company AND jh.PartNum = p.PartNum
        LEFT JOIN Erp.JobAsmbl ja
            ON jo.Company = ja.Company AND jo.JobNum = ja.JobNum AND jo.AssemblySeq = ja.AssemblySeq
        LEFT JOIN Erp.Part pa
            ON ja.Company = pa.Company AND ja.PartNum = pa.PartNum
        LEFT JOIN MaterialAgg ma
            ON jo.Company = ma.Company
            AND jo.JobNum = ma.JobNum
            AND jo.AssemblySeq = ma.AssemblySeq
            AND jo.OprSeq = ma.RelatedOperation
        LEFT JOIN Ice.XFileAttch xfa
            ON ja.Company = xfa.Company
            AND ja.PartNum = xfa.Key1
            AND ja.RevisionNum = xfa.Key2
            AND xfa.RelatedToFile = 'PartRev'
        LEFT JOIN Ice.XFileRef xfr
            ON xfa.Company = xfr.Company
            AND xfa.XFileRefNum = xfr.XFileRefNum
        -- Get on-hand qty for the part (use JobAsmbl part for sub-assemblies, JobHead part for asm 0)
        OUTER APPLY (
            SELECT SUM(OnHandQty) AS OnHandQty
            FROM Erp.PartQty
            WHERE PartNum = CASE WHEN jo.AssemblySeq > 0 THEN ja.PartNum ELSE jh.PartNum END
        ) poh
        WHERE jh.JobComplete = 0
          AND jh.JobReleased = 1
          AND jo.OpCode IN ({placeholders})
          AND jo.OpComplete = 0
          AND jo.LaborEntryMethod != 'B'
          -- Rule 3: First op on assembly OR prior op has qty completed OR SS/FF scheduling
          AND (poq.IsFirstOp = 1 OR poq.QtyFromPrior > 0 OR jo.SchedRelation IN ('SS', 'FF'))
          -- Rule 4: If ASM0, all sub-assemblies must have last op with qty completed
          AND (jo.AssemblySeq > 0 OR sac.AllSubAsmsReady = 1)
        ORDER BY 
            jh.StartDate ASC,
            jh.JobNum ASC,
            jo.AssemblySeq ASC,
            jo.OprSeq ASC
    """
    
    return sql_query(query, params)


def get_billet_summary():
    """
    Get billet demand summary for the Burn dashboard.
    
    Aggregates materials from Die workcell jobs (DIEGRIND, DRILLDIE, TURNDIE)
    grouped by PartNum, showing:
    - OnHand: Current inventory
    - LateNeed: Sum of RequiredQty where job StartDate <= today
    - FutureNeed: Sum of RequiredQty where job StartDate > today  
    - TotalDemand: Total demand from PartQty
    """
    query = """
        WITH DieMaterials AS (
            -- Get materials from Die workcell operations
            SELECT 
                jm.PartNum,
                jm.RequiredQty,
                jh.StartDate
            FROM Erp.JobMtl jm
            INNER JOIN Erp.JobOper jo ON jm.Company = jo.Company 
                AND jm.JobNum = jo.JobNum 
                AND jm.AssemblySeq = jo.AssemblySeq
                AND jm.RelatedOperation = jo.OprSeq
            INNER JOIN Erp.JobHead jh ON jo.Company = jh.Company 
                AND jo.JobNum = jh.JobNum
            WHERE jh.JobComplete = 0
              AND jh.JobReleased = 1
              AND jo.OpComplete = 0
              AND jo.OpCode IN ('DIEGRIND', 'DRILLDIE', 'TURNDIE')
              AND jm.RequiredQty > 0
        ),
        MaterialSummary AS (
            -- Aggregate by PartNum
            SELECT 
                PartNum,
                SUM(CASE WHEN StartDate <= CAST(GETDATE() AS DATE) THEN RequiredQty ELSE 0 END) AS LateNeed,
                SUM(CASE WHEN StartDate > CAST(GETDATE() AS DATE) THEN RequiredQty ELSE 0 END) AS FutureNeed
            FROM DieMaterials
            GROUP BY PartNum
        )
        SELECT 
            ms.PartNum,
            REPLACE(p.PartDescription, ' - die billet ungrooved', '') AS PartDescription,
            ISNULL(pq.OnHandQty, 0) AS OnHand,
            ms.LateNeed,
            ms.FutureNeed,
            ISNULL(pq.TotalDemand, 0) AS TotalDemand
        FROM MaterialSummary ms
        LEFT JOIN Erp.Part p ON ms.PartNum = p.PartNum
        LEFT JOIN (
            SELECT PartNum, SUM(OnHandQty) AS OnHandQty, SUM(DemandQty) AS TotalDemand
            FROM Erp.PartQty
            GROUP BY PartNum
        ) pq ON ms.PartNum = pq.PartNum
        ORDER BY p.PartDescription, ms.PartNum
    """
    
    return sql_query(query)


def search_parts(search_term):
    """
    Search for parts by part number or description.
    Returns top 20 matches for Kanban search.
    Only returns non-obsolete, stocked parts.
    """
    query = """
        SELECT TOP 20
            p.PartNum,
            p.PartDescription,
            ISNULL(pq.OnHandQty, 0) AS OnHand
        FROM Erp.Part p
        LEFT JOIN (
            SELECT PartNum, SUM(OnHandQty) AS OnHandQty
            FROM Erp.PartQty
            GROUP BY PartNum
        ) pq ON p.PartNum = pq.PartNum
        WHERE (p.PartNum LIKE :search_pattern
               OR p.PartDescription LIKE :search_pattern)
          AND p.InActive = 0
          AND p.TypeCode = 'M'
        ORDER BY 
            CASE WHEN p.PartNum LIKE :exact_pattern THEN 0 ELSE 1 END,
            p.PartNum
    """
    
    return sql_query(query, {
        'search_pattern': f'%{search_term}%',
        'exact_pattern': f'{search_term}%'
    })


def get_job_operations(job_num):
    """
    Get all non-backflush operations for a job, ordered by assembly then op seq.
    Includes ResourceGrpID, ResourceID, JCDept, CapabilityID for labor entry.
    """
    query = """
        SELECT
            jo.AssemblySeq,
            jo.OprSeq,
            jo.OpCode,
            jo.OpDesc,
            jo.QtyCompleted,
            jo.OpComplete,
            jo.ProdStandard,
            -- For LABOR ENTRY: ResourceGrpID with fallback chain
            COALESCE(
                NULLIF(jod.ResourceGrpID, ''),
                res_from_jod.ResourceGrpID,
                res_from_rtu.ResourceGrpID
            ) AS ResourceGrpID,
            -- For LABOR ENTRY: ResourceID with fallback chain
            COALESCE(
                NULLIF(jod.ResourceID, ''),
                r.ResourceID,
                rtu.ResourceID
            ) AS ResourceID,
            -- For LABOR ENTRY: JCDept from ResourceGroup with fallback chain
            COALESCE(
                rg_from_jod_grp.JCDept,
                rg_from_jod_res.JCDept,
                rg_from_rtu.JCDept
            ) AS JCDept,
            jod.CapabilityID
        FROM Erp.JobOper jo
        LEFT JOIN Erp.JobOpDtl jod ON jo.Company = jod.Company
            AND jo.JobNum = jod.JobNum
            AND jo.AssemblySeq = jod.AssemblySeq
            AND jo.OprSeq = jod.OprSeq
        -- Get ResourceGroup directly from JobOpDtl.ResourceGrpID
        LEFT JOIN Erp.ResourceGroup rg_from_jod_grp ON jod.Company = rg_from_jod_grp.Company
            AND jod.ResourceGrpID = rg_from_jod_grp.ResourceGrpID
            AND jod.ResourceGrpID IS NOT NULL AND jod.ResourceGrpID != ''
        -- Get default Resource from ResourceGroup (first location resource)
        OUTER APPLY (
            SELECT TOP 1 ResourceID
            FROM Erp.Resource
            WHERE ResourceGrpID = jod.ResourceGrpID
              AND Location = 1
        ) r
        -- If JobOpDtl has ResourceID but no ResourceGrpID, look up the group from Resource table
        LEFT JOIN Erp.Resource res_from_jod ON jod.Company = res_from_jod.Company
            AND jod.ResourceID = res_from_jod.ResourceID
            AND (jod.ResourceGrpID IS NULL OR jod.ResourceGrpID = '')
        LEFT JOIN Erp.ResourceGroup rg_from_jod_res ON res_from_jod.Company = rg_from_jod_res.Company
            AND res_from_jod.ResourceGrpID = rg_from_jod_res.ResourceGrpID
        -- Get scheduled resource from ResourceTimeUsed (for display/filtering AND as final fallback)
        OUTER APPLY (
            SELECT TOP 1 ResourceID
            FROM Erp.ResourceTimeUsed
            WHERE Company = jo.Company
              AND JobNum = jo.JobNum
              AND AssemblySeq = jo.AssemblySeq
              AND OprSeq = jo.OprSeq
        ) rtu
        -- Fallback: Get ResourceGrpID from Resource table using ResourceTimeUsed.ResourceID
        LEFT JOIN Erp.Resource res_from_rtu ON jo.Company = res_from_rtu.Company
            AND rtu.ResourceID = res_from_rtu.ResourceID
        LEFT JOIN Erp.ResourceGroup rg_from_rtu ON res_from_rtu.Company = rg_from_rtu.Company
            AND res_from_rtu.ResourceGrpID = rg_from_rtu.ResourceGrpID
        WHERE jo.JobNum = :job_num
          AND jo.LaborEntryMethod != 'B'
        ORDER BY jo.AssemblySeq DESC, jo.OprSeq ASC
    """
    return sql_query(query, {'job_num': job_num})


def get_employee(emp_id):
    """Look up an employee by ID. Returns dict with EmpID and Name, or None."""
    query = """
        SELECT EmpID, Name
        FROM Erp.EmpBasic
        WHERE EmpID = :emp_id
          AND EmpStatus = 'A'
    """
    result = sql_query(query, {'emp_id': emp_id})
    return result[0] if result else None


def get_job_header(job_num):
    """Get job header info."""
    query = """
        SELECT 
            jh.JobNum,
            jh.PartNum,
            p.PartDescription,
            jh.ProdQty,
            jh.StartDate,
            jh.ReqDueDate,
            jh.DueDate
        FROM Erp.JobHead jh
        LEFT JOIN Erp.Part p ON jh.Company = p.Company AND jh.PartNum = p.PartNum
        WHERE jh.JobNum = :job_num
    """
    result = sql_query(query, {'job_num': job_num})
    return result[0] if result else None


def get_job_materials(job_num, assembly_seq, opr_seq):
    """
    Get material details for a specific job operation.
    Uses two queries for better performance - materials first, then batch inventory lookup.
    """
    # First get the materials (fast)
    mtl_query = """
        SELECT 
            jm.MtlSeq,
            jm.PartNum,
            p.PartDescription,
            jm.RequiredQty,
            ISNULL(jm.IUM, p.IUM) AS ReqUOM,
            p.IUM AS OnHandUOM
        FROM Erp.JobMtl jm
        LEFT JOIN Erp.Part p ON jm.Company = p.Company AND jm.PartNum = p.PartNum
        WHERE jm.JobNum = :job_num
          AND jm.AssemblySeq = :assembly_seq
          AND jm.RelatedOperation = :opr_seq
          AND jm.RequiredQty > 0
        ORDER BY jm.MtlSeq
    """
    
    materials = sql_query(mtl_query, {
        'job_num': job_num,
        'assembly_seq': assembly_seq,
        'opr_seq': opr_seq
    })
    
    if not materials:
        return []
    
    # Get all unique part numbers
    part_nums = list(set(m['PartNum'] for m in materials))
    
    # Batch lookup inventory for all parts at once
    placeholders = ', '.join([f':p{i}' for i in range(len(part_nums))])
    params = {f'p{i}': pn for i, pn in enumerate(part_nums)}
    
    inv_query = f"""
        SELECT PartNum, 
               SUM(OnHandQty) AS OnHandQty,
               SUM(DemandQty) AS DemandQty
        FROM Erp.PartQty
        WHERE PartNum IN ({placeholders})
        GROUP BY PartNum
    """
    
    inv_data = sql_query(inv_query, params)
    inv_map = {row['PartNum']: row for row in inv_data}
    
    # Merge inventory into materials and calculate status
    for m in materials:
        inv = inv_map.get(m['PartNum'], {})
        on_hand = inv.get('OnHandQty', 0) or 0
        demand = inv.get('DemandQty', 0) or m['RequiredQty']
        required = m['RequiredQty'] or 0
        
        m['OnHandQty'] = on_hand
        m['DemandQty'] = demand
        m['DemandUOM'] = m['OnHandUOM']
        
        if on_hand >= demand:
            m['Status'] = 'star'
        elif on_hand >= required:
            m['Status'] = 'check'
        elif on_hand > 0:
            m['Status'] = 'partial'
        else:
            m['Status'] = 'missing'
        
        m['QtyShort'] = max(0, required - on_hand)
    
    return materials


def get_active_labor_details(job_nums_with_ops):
    """
    Get job details for active labor records.
    
    Args:
        job_nums_with_ops: list of dicts with JobNum, AssemblySeq, OprSeq
    
    Returns:
        dict mapping 'JobNum-AsmSeq-OprSeq' to job details including material status
    """
    if not job_nums_with_ops:
        return {}
    
    # Build query to get job/part details
    conditions = []
    params = {}
    for i, rec in enumerate(job_nums_with_ops):
        conditions.append(f"(jh.JobNum = :job{i} AND jo.AssemblySeq = :asm{i} AND jo.OprSeq = :opr{i})")
        params[f'job{i}'] = rec['JobNum']
        params[f'asm{i}'] = rec['AssemblySeq']
        params[f'opr{i}'] = rec['OprSeq']
    
    where_clause = ' OR '.join(conditions)
    
    query = f"""
        SELECT jh.JobNum,
               -- Use JobAsmbl part for sub-assemblies, JobHead part for asm 0
               CASE WHEN jo.AssemblySeq > 0 THEN ja.PartNum ELSE jh.PartNum END AS PartNum,
               CASE WHEN jo.AssemblySeq > 0 THEN pa.PartDescription ELSE p.PartDescription END AS PartDescription,
               CASE WHEN jo.AssemblySeq > 0 THEN ja.RequiredQty ELSE jh.ProdQty END AS ProdQty,
               jo.AssemblySeq, jo.OprSeq, jo.OpCode, jo.QtyCompleted,
               -- Get qty completed from prior non-backflush operation
               (SELECT TOP 1 jo_prior.QtyCompleted
                FROM Erp.JobOper jo_prior
                WHERE jo_prior.JobNum = jo.JobNum
                  AND jo_prior.AssemblySeq = jo.AssemblySeq
                  AND jo_prior.OprSeq < jo.OprSeq
                  AND jo_prior.LaborEntryMethod != 'B'
                ORDER BY jo_prior.OprSeq DESC) AS QtyFromPrior,
               -- Is this the first non-backflush operation?
               CASE WHEN NOT EXISTS (
                   SELECT 1 FROM Erp.JobOper jo_prior
                   WHERE jo_prior.JobNum = jo.JobNum
                     AND jo_prior.AssemblySeq = jo.AssemblySeq
                     AND jo_prior.OprSeq < jo.OprSeq
                     AND jo_prior.LaborEntryMethod != 'B'
               ) THEN 1 ELSE 0 END AS IsFirstOp
        FROM Erp.JobHead jh
        JOIN Erp.JobOper jo ON jh.Company = jo.Company AND jh.JobNum = jo.JobNum
        LEFT JOIN Erp.Part p ON jh.Company = p.Company AND jh.PartNum = p.PartNum
        LEFT JOIN Erp.JobAsmbl ja ON jo.Company = ja.Company AND jo.JobNum = ja.JobNum AND jo.AssemblySeq = ja.AssemblySeq
        LEFT JOIN Erp.Part pa ON ja.Company = pa.Company AND ja.PartNum = pa.PartNum
        WHERE ({where_clause})
    """
    
    results = sql_query(query, params)
    
    # Get material info for these operations using bulk materials lookup
    job_nums = list(set(rec['JobNum'] for rec in job_nums_with_ops))
    all_materials = get_bulk_materials(job_nums)
    
    # Build map of jobkey -> details
    details_map = {}
    for row in results:
        key = f"{row['JobNum']}-{row['AssemblySeq']}-{row['OprSeq']}"
        prod_qty = row['ProdQty'] or 0
        qty_completed = row['QtyCompleted'] or 0
        qty_left = prod_qty - qty_completed
        is_first_op = row['IsFirstOp'] == 1
        
        # Available is dash (None) for first op, otherwise QtyFromPrior - QtyCompleted
        if is_first_op:
            qty_available = None
        else:
            qty_from_prior = row['QtyFromPrior'] or 0
            qty_available = max(0, qty_from_prior - qty_completed)
        
        # Calculate material status and max producible qty
        materials = all_materials.get(key, [])
        mtl_status = 'none'
        max_mtl_qty = None  # None means no material constraint
        
        if materials:
            has_missing = False
            has_partial = False
            has_check = False
            all_star = True
            min_producible = float('inf')
            
            for m in materials:
                on_hand = m.get('OnHandQty', 0) or 0
                required = m.get('RequiredQty', 0) or 0
                demand = m.get('DemandQty', 0) or required
                
                # Calculate status for this material
                if on_hand == 0:
                    has_missing = True
                    all_star = False
                elif on_hand < required:
                    has_partial = True
                    all_star = False
                elif on_hand < demand:
                    has_check = True
                    all_star = False
                
                # Calculate max producible from this material
                # max_units = on_hand / (required / prod_qty) = on_hand * prod_qty / required
                if required > 0 and prod_qty > 0:
                    per_unit_need = required / prod_qty
                    if per_unit_need > 0:
                        can_make = on_hand / per_unit_need
                        min_producible = min(min_producible, can_make)
            
            # Set material status (priority: missing > partial > check > star)
            if has_missing:
                mtl_status = 'missing'
            elif has_partial:
                mtl_status = 'partial'
            elif has_check:
                mtl_status = 'check'
            elif all_star:
                mtl_status = 'star'
            
            # Set max producible (floor to int)
            if min_producible != float('inf'):
                max_mtl_qty = int(min_producible)
        
        details_map[key] = {
            'PartNum': row['PartNum'],
            'PartDescription': row['PartDescription'],
            'OpCode': row['OpCode'],
            'ProdQty': int(prod_qty),
            'QtyCompleted': int(qty_completed),
            'QtyLeft': int(qty_left),
            'QtyAvailable': int(qty_available) if qty_available is not None else None,
            'IsFirstOp': is_first_op,
            'MtlStatus': mtl_status,
            'MaxMtlQty': max_mtl_qty
        }
    
    return details_map


def get_last_kanban_receipt(part_num):
    """
    Get the last inventory receipt for a part number.
    Used to prevent double-entry on Kanban receipts.
    Looks for MFG-STK transactions (manufactured to stock).
    Gets employee from the associated LaborDtl record.
    """
    query = """
        SELECT TOP 1
            pt.TranQty,
            CONVERT(VARCHAR(10), pt.TranDate, 23) AS TranDate,
            ld.EmployeeNum,
            e.Name AS EmployeeName,
            pt.TranNum,
            pt.TranType
        FROM Erp.PartTran pt
        LEFT JOIN Erp.LaborDtl ld ON pt.Company = ld.Company 
            AND pt.JobNum = ld.JobNum
            AND ld.LaborQty > 0
        LEFT JOIN Erp.EmpBasic e ON ld.Company = e.Company AND ld.EmployeeNum = e.EmpID
        WHERE pt.PartNum = :part_num
          AND pt.TranType = 'MFG-STK'
          AND pt.TranQty > 0
        ORDER BY pt.TranDate DESC, pt.TranNum DESC
    """
    result = sql_query(query, {'part_num': part_num})
    return result[0] if result else None


def get_insert_summary():
    """
    Get Gen 3 insert demand summary for the Inserts dashboard.
    
    Shows insert parts with:
    - OnHand: Current inventory
    - InProd: Inserts needed for die set jobs in process (Asm 1 Op 10 complete)
    - TotalNeed: Overall demand from PartQty.DemandQty
    - Shortage calculations for both in-prod and total
    
    Die sets need 2 inserts each, matched by CommercialSize1.
    """
    query = """
        WITH InProdNeed AS (
            -- Die set jobs in process (Assembly 1 Op 10 complete)
            -- Each die set needs 2 inserts, matched by CommercialSize1
            SELECT 
                p.CommercialSize1,
                SUM((jh.ProdQty - jh.QtyCompleted) * 2) AS InProdQty
            FROM Erp.JobHead jh
            INNER JOIN Erp.Part p ON jh.Company = p.Company AND jh.PartNum = p.PartNum
            INNER JOIN Erp.JobOper jo ON jh.Company = jo.Company AND jh.JobNum = jo.JobNum
            WHERE jh.JobClosed = 0
              AND jh.JobComplete = 0
              AND jh.PartDescription LIKE '%die set%'
              AND p.CommercialSize1 > '0'
              AND jo.AssemblySeq = 1
              AND jo.OprSeq = 10
              AND jo.OpComplete = 1
            GROUP BY p.CommercialSize1
        ),
        InsertParts AS (
            -- Gen 3 insert parts with inventory
            SELECT 
                p.PartNum,
                p.PartDescription,
                p.CommercialSize1,
                ISNULL(pq.OnHandQty, 0) AS OnHand,
                ISNULL(pq.DemandQty, 0) AS TotalNeed
            FROM Erp.Part p
            LEFT JOIN (
                SELECT PartNum, SUM(OnHandQty) AS OnHandQty, SUM(DemandQty) AS DemandQty
                FROM Erp.PartQty
                GROUP BY PartNum
            ) pq ON p.PartNum = pq.PartNum
            WHERE p.PartDescription LIKE '%Gen 3%'
              AND p.CommercialSize1 > '0'
        )
        SELECT 
            ip.PartNum,
            ip.PartDescription,
            ip.OnHand,
            ISNULL(ipn.InProdQty, 0) AS InProd,
            ip.TotalNeed
        FROM InsertParts ip
        LEFT JOIN InProdNeed ipn ON ip.CommercialSize1 = ipn.CommercialSize1
        WHERE ip.TotalNeed > 0 OR ISNULL(ipn.InProdQty, 0) > 0
        ORDER BY ip.PartDescription
    """
    
    return sql_query(query)


def get_all_workcell_counts():
    """
    Get job counts for all workcells in one efficient query.
    Returns dict mapping workcell_id to job count.
    """
    # Collect all op codes from all workcells
    all_ops = set()
    op_to_workcell = {}  # Map each op code to its workcell(s)
    
    for wc_id, wc_config in WORKCELLS.items():
        # Skip dashboard-type workcells (they don't have standard job queues)
        if wc_config.get('dashboard_type'):
            continue
        for op in wc_config.get('ops', []):
            all_ops.add(op)
            if op not in op_to_workcell:
                op_to_workcell[op] = []
            op_to_workcell[op].append(wc_id)
    
    if not all_ops:
        return {}
    
    # Build query to count jobs by OpCode
    placeholders = ', '.join([f':op{i}' for i in range(len(all_ops))])
    params = {f'op{i}': op for i, op in enumerate(all_ops)}
    
    query = f"""
        SELECT jo.OpCode, COUNT(*) AS JobCount
        FROM Erp.JobHead jh
        INNER JOIN Erp.JobOper jo ON jh.Company = jo.Company AND jh.JobNum = jo.JobNum
        WHERE jh.JobComplete = 0
          AND jh.JobReleased = 1
          AND jo.OpCode IN ({placeholders})
          AND jo.OpComplete = 0
          AND jo.LaborEntryMethod != 'B'
        GROUP BY jo.OpCode
    """
    
    rows = sql_query(query, params)
    
    # Build counts by workcell
    counts = {wc_id: 0 for wc_id in WORKCELLS.keys()}
    
    for row in rows:
        op_code = row['OpCode']
        count = row['JobCount']
        # Add this count to each workcell that uses this op code
        for wc_id in op_to_workcell.get(op_code, []):
            counts[wc_id] += count
    
    return counts


def get_active_worker_count():
    """
    Get count of workers currently working on jobs.
    Returns int count of distinct employees with active LaborDtl records.
    """
    query = """
        SELECT COUNT(DISTINCT lh.EmployeeNum) AS ActiveCount
        FROM Erp.LaborDtl ld
        INNER JOIN Erp.LaborHed lh ON ld.Company = lh.Company AND ld.LaborHedSeq = lh.LaborHedSeq
        WHERE ld.ActiveTrans = 1
    """
    
    result = sql_query(query)
    return result[0]['ActiveCount'] if result else 0


def get_active_job_count():
    """
    Get count of jobs currently being worked on.
    Returns int count of distinct jobs with active LaborDtl records.
    """
    query = """
        SELECT COUNT(DISTINCT JobNum) AS ActiveCount
        FROM Erp.LaborDtl
        WHERE ActiveTrans = 1
    """
    
    result = sql_query(query)
    return result[0]['ActiveCount'] if result else 0


def get_operation_last_entries(job_num):
    """
    Get last entry dates for all non-backflush operations on a job.
    Loaded on-demand when row is expanded (removed from bulk query for performance).
    
    Returns dict mapping 'AssemblySeq-OprSeq' to last entry date string (YYYY-MM-DD).
    """
    query = """
        SELECT 
            jo.AssemblySeq,
            jo.OprSeq,
            CONVERT(VARCHAR(10), 
                (SELECT MAX(ld.ClockInDate) 
                 FROM Erp.LaborDtl ld 
                 WHERE ld.JobNum = jo.JobNum 
                   AND ld.AssemblySeq = jo.AssemblySeq
                   AND ld.OprSeq = jo.OprSeq 
                   AND ld.LaborQty > 0), 23) AS LastEntryDate
        FROM Erp.JobOper jo
        WHERE jo.JobNum = :job_num
          AND jo.LaborEntryMethod != 'B'
    """
    
    rows = sql_query(query, {'job_num': job_num})
    
    # Build dict keyed by 'AssemblySeq-OprSeq'
    result = {}
    for row in rows:
        key = f"{row['AssemblySeq']}-{row['OprSeq']}"
        result[key] = row['LastEntryDate']  # May be None if no labor entry yet

    return result


# ============================================================================
# PRODUCTION ACTIVITY REPORT
# ============================================================================

def get_all_employees():
    """Get all active employees for dropdown."""
    query = """
        SELECT EmpID, Name
        FROM Erp.EmpBasic
        WHERE EmpStatus = 'A'
        ORDER BY EmpID
    """
    return sql_query(query)


def get_activity_report(emp_id, start_date, end_date, op_codes=None):
    """
    Get production activity report for an employee or all employees.

    Returns all labor entries (including indirect) with job/part details.

    Args:
        emp_id: Employee ID (string) or 'all' for all employees
        start_date: Start date (string 'YYYY-MM-DD')
        end_date: End date (string 'YYYY-MM-DD')
        op_codes: Optional list of operation codes to filter

    Returns:
        List of dicts with labor entry details
    """
    params = {
        'start_date': start_date,
        'end_date': end_date
    }

    # Build employee filter clause
    emp_filter = ""
    if emp_id and emp_id != 'all':
        emp_filter = "AND ld.EmployeeNum = :emp_id"
        params['emp_id'] = emp_id

    # Build operation filter clause
    op_filter = ""
    if op_codes and len(op_codes) > 0:
        placeholders = ', '.join([f':op{i}' for i in range(len(op_codes))])
        params.update({f'op{i}': op for i, op in enumerate(op_codes)})
        op_filter = f"AND jo.OpCode IN ({placeholders})"

    query = f"""
        SELECT TOP 1000
            CONVERT(VARCHAR(10), ld.ClockInDate, 23) AS ClockInDate,
            CASE
                WHEN ld.ClockInTime IS NOT NULL
                THEN FORMAT(DATEADD(MINUTE, ld.ClockInTime * 60, CONVERT(DATETIME, '00:00:00')), 'HH:mm')
                ELSE ''
            END AS ClockInTime,
            ld.EmployeeNum,
            e.Name AS EmployeeName,
            ld.JobNum,
            ld.AssemblySeq,
            ld.OprSeq,
            jo.OpCode,
            ISNULL(jo.OpDesc, '') AS OpDesc,
            CASE WHEN ld.AssemblySeq > 0 THEN ja.PartNum ELSE jh.PartNum END AS PartNum,
            CASE WHEN ld.AssemblySeq > 0 THEN pa.PartDescription ELSE p.PartDescription END AS PartDescription,
            ISNULL(ld.LaborQty, 0) AS LaborQty,
            ISNULL(ld.ScrapQty, 0) AS ScrapQty,
            ISNULL(ld.LaborHrs, 0) AS LaborHrs
        FROM Erp.LaborDtl ld
        INNER JOIN Erp.EmpBasic e
            ON ld.Company = e.Company AND ld.EmployeeNum = e.EmpID
        INNER JOIN Erp.JobHead jh
            ON ld.Company = jh.Company AND ld.JobNum = jh.JobNum
        INNER JOIN Erp.JobOper jo
            ON ld.Company = jo.Company
            AND ld.JobNum = jo.JobNum
            AND ld.AssemblySeq = jo.AssemblySeq
            AND ld.OprSeq = jo.OprSeq
        LEFT JOIN Erp.JobAsmbl ja
            ON ld.Company = ja.Company
            AND ld.JobNum = ja.JobNum
            AND ld.AssemblySeq = ja.AssemblySeq
        LEFT JOIN Erp.Part p
            ON jh.Company = p.Company AND jh.PartNum = p.PartNum
        LEFT JOIN Erp.Part pa
            ON ja.Company = pa.Company AND ja.PartNum = pa.PartNum
        WHERE ld.ClockInDate >= :start_date
          AND ld.ClockInDate <= :end_date
          {emp_filter}
          {op_filter}
        ORDER BY ld.ClockInDate DESC, ld.ClockInTime DESC
    """

    import sys
    print(f"[get_activity_report] Query params: {params}", file=sys.stderr, flush=True)
    print(f"[get_activity_report] Employee filter: {emp_filter}", file=sys.stderr, flush=True)
    print(f"[get_activity_report] Op filter: {op_filter}", file=sys.stderr, flush=True)

    result = sql_query(query, params)
    print(f"[get_activity_report] Returned {len(result)} rows", file=sys.stderr, flush=True)

    return result
