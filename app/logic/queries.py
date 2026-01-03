# app/logic/queries.py
# Version 5.0 — 2026-01-02
#
# SQL queries for The Queue
# Uses OpCode-based work cell filtering
# Material status: star (full demand), check (job ok), partial, none

import json
import os
from sqlalchemy import text
from decimal import Decimal
from app.config import get_engine

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
    """
    if not job_nums:
        return {}
    
    placeholders = ', '.join([f':j{i}' for i in range(len(job_nums))])
    params = {f'j{i}': jn for i, jn in enumerate(job_nums)}
    
    query = f"""
        SELECT jo.JobNum, jo.OprSeq, jo.OpCode, jo.OpDesc, jo.QtyCompleted, 
               CAST(jo.OpComplete AS INT) AS OpComplete, jo.ProdStandard, jo.AssemblySeq,
               CONVERT(VARCHAR(10), 
                   (SELECT MAX(ld.ClockInDate) 
                    FROM Erp.LaborDtl ld 
                    WHERE ld.JobNum = jo.JobNum 
                      AND ld.AssemblySeq = jo.AssemblySeq
                      AND ld.OprSeq = jo.OprSeq 
                      AND ld.LaborQty > 0), 23) AS LastEntryDate
        FROM Erp.JobOper jo
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


def get_bulk_materials(job_nums):
    """
    Get all materials with inventory for a list of jobs.
    
    Includes materials from preceding backflush operations up to the previous
    non-backflush (quantity) operation. Excludes materials from PAINT operations.
    
    Returns dict keyed by 'JobNum-AssemblySeq-OprSeq'.
    """
    if not job_nums:
        return {}
    
    placeholders = ', '.join([f':j{i}' for i in range(len(job_nums))])
    params = {f'j{i}': jn for i, jn in enumerate(job_nums)}
    
    # Step 1: Get all operations to determine backflush relationships
    ops_query = f"""
        SELECT JobNum, AssemblySeq, OprSeq, OpCode, LaborEntryMethod
        FROM Erp.JobOper
        WHERE JobNum IN ({placeholders})
        ORDER BY JobNum, AssemblySeq, OprSeq
    """
    all_ops = sql_query(ops_query, params)
    
    # Step 2: Build mapping of which operations' materials belong to which visible operation
    # For each (job, assembly), walk through ops in sequence
    # When we hit a non-backflush op, all preceding backflush ops (since last non-backflush) belong to it
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
        pending_backflush = []  # Backflush ops waiting to be assigned
        
        for op in ops:
            opr_seq = op['OprSeq']
            is_backflush = op['LaborEntryMethod'] == 'B'
            is_paint = op['OpCode'] == 'PAINT'
            
            if is_backflush:
                # Don't include PAINT operation materials
                if not is_paint:
                    pending_backflush.append((job_num, asm_seq, opr_seq))
            else:
                # Non-backflush (quantity) operation - assign pending backflush materials to it
                visible_key = (job_num, asm_seq, opr_seq)
                # Include this operation's own materials plus any pending backflush
                op_mapping[visible_key] = pending_backflush + [visible_key]
                pending_backflush = []  # Reset for next group
    
    # Step 3: Get all materials (excluding PAINT operations)
    mtl_query = f"""
        SELECT jm.JobNum, jm.AssemblySeq, jm.RelatedOperation AS OprSeq,
               jm.MtlSeq, jm.PartNum, p.PartDescription, jm.RequiredQty,
               ISNULL(jm.IUM, p.IUM) AS ReqUOM, p.IUM AS OnHandUOM,
               jo.OpCode AS SourceOpCode
        FROM Erp.JobMtl jm
        LEFT JOIN Erp.Part p ON jm.Company = p.Company AND jm.PartNum = p.PartNum
        INNER JOIN Erp.JobOper jo ON jm.Company = jo.Company 
            AND jm.JobNum = jo.JobNum 
            AND jm.AssemblySeq = jo.AssemblySeq 
            AND jm.RelatedOperation = jo.OprSeq
        WHERE jm.JobNum IN ({placeholders})
          AND jm.RequiredQty > 0
          AND jo.OpCode != 'PAINT'
        ORDER BY jm.JobNum, jm.AssemblySeq, jm.RelatedOperation, jm.MtlSeq
    """
    
    materials = sql_query(mtl_query, params)
    
    if not materials:
        return {}
    
    # Step 4: Get unique part numbers and batch lookup inventory
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
    Get jobs for workcell with operations and materials pre-loaded.
    Uses 3 queries instead of N subqueries for performance.
    """
    # 1. Get main job list (fast)
    jobs = get_jobs_for_workcell(workcell_id)
    if not jobs:
        return []
    
    # 2. Collect unique job numbers
    job_nums = list(set(j['JobNum'] for j in jobs))
    
    # 3. Bulk fetch operations and materials (just by job number)
    all_operations = get_bulk_operations(job_nums)
    all_materials = get_bulk_materials(job_nums)
    
    # 4. Merge into jobs as JSON strings
    for job in jobs:
        jn = job['JobNum']
        key = f"{jn}-{job['AssemblySeq']}-{job['OprSeq']}"
        
        ops = all_operations.get(jn, [])
        mtls = all_materials.get(key, [])
        
        job['OperationsJSON'] = json.dumps(ops) if ops else '[]'
        job['MaterialsJSON'] = json.dumps(mtls) if mtls else '[]'
    
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
            ISNULL(ma.TotalMtls, 0) AS TotalMtls
        FROM Erp.JobHead jh
        INNER JOIN Erp.JobOper jo 
            ON jh.Company = jo.Company AND jh.JobNum = jo.JobNum
        LEFT JOIN Erp.JobOper_UD joud
            ON jo.SysRowID = joud.ForeignSysRowID
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
        WHERE jh.JobComplete = 0
          AND jh.JobReleased = 1
          AND jo.OpCode IN ({placeholders})
          AND jo.OpComplete = 0
          AND jo.LaborEntryMethod != 'B'
          -- Rule 3: First op on assembly OR prior op has qty completed
          AND (poq.IsFirstOp = 1 OR poq.QtyFromPrior > 0)
          -- Rule 4: If ASM0, all sub-assemblies must have last op with qty completed
          AND (jo.AssemblySeq > 0 OR sac.AllSubAsmsReady = 1)
        ORDER BY 
            jh.StartDate ASC,
            jh.JobNum ASC,
            jo.AssemblySeq ASC,
            jo.OprSeq ASC
    """
    
    return sql_query(query, params)


def get_job_operations(job_num):
    """Get all non-backflush operations for a job, ordered by assembly then op seq."""
    query = """
        SELECT 
            jo.AssemblySeq,
            jo.OprSeq,
            jo.OpCode,
            jo.OpDesc,
            jo.QtyCompleted,
            jo.OpComplete,
            jo.ProdStandard
        FROM Erp.JobOper jo
        WHERE jo.JobNum = :job_num
          AND jo.LaborEntryMethod != 'B'
        ORDER BY jo.AssemblySeq DESC, jo.OprSeq ASC
    """
    return sql_query(query, {'job_num': job_num})


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
