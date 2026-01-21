# app/routes/views.py
# Version 5.2 — 2026-01-16
#
# Routes for The Queue web interface
# Added /api/job/<job_num>/last_entries for on-demand LastEntryDate loading

print("========== VIEWS.PY LOADED ==========")

import os
from flask import Blueprint, render_template, jsonify, request, send_file, abort
from app.logic.queries import (
    get_workcells, get_jobs_for_workcell, get_jobs_with_details, get_job_materials, 
    get_job_operations, get_job_header, get_workcell_config,
    get_last_checkin, get_materials_for_workcell, get_billet_summary, WORKCELLS,
    get_operation_last_entries
)
from app.config import translate_pdf_path

views = Blueprint('views', __name__)


@views.route('/')
def index():
    """Home page - show work cell buttons grouped by area."""
    all_workcells = {wc['id']: wc for wc in get_workcells()}
    
    # Define groups and their workcell IDs
    groups = [
        ('Laser / Press', ['LASER', 'PRESS']),
        ('Saw / Weld / Burn', ['SAW', 'WELD', 'BURN']),
        ('Machining', ['MILL-LATHE', 'DIES', 'FBS', 'INSERTS']),
        ('Powder', ['POWDER', 'BLACKENING']),
        ('Shipping', ['SHIPPING']),
        ('Assembly', ['ASM-M50', 'ASM-MAD', 'CRATING', 'WIRE']),
        ('Office', ['OFFICE']),
    ]
    
    # Build grouped workcells list
    grouped_workcells = []
    for group_name, wc_ids in groups:
        workcells_in_group = [all_workcells[wc_id] for wc_id in wc_ids if wc_id in all_workcells]
        if workcells_in_group:
            grouped_workcells.append({
                'name': group_name,
                'workcells': workcells_in_group
            })
    
    return render_template('index.html', groups=grouped_workcells)


@views.route('/queue/<workcell_id>')
def queue(workcell_id):
    """Queue page - show jobs ready for a work cell."""
    import time
    t_start = time.time()
    
    # Get work cell info
    if workcell_id not in WORKCELLS:
        return render_template('error.html', message=f"Work cell '{workcell_id}' not found"), 404
    
    workcell_config = get_workcell_config(workcell_id)
    workcell_name = workcell_config['name']
    
    # Check for dashboard type - serve different template
    dashboard_type = workcell_config.get('dashboard_type')
    
    if dashboard_type == 'inserts':
        # Inserts dashboard - show Gen 3 insert demand summary
        from app.logic.queries import get_insert_summary
        inserts = get_insert_summary()
        workcells = get_workcells()  # For the dropdown
        
        # Calculate shortage fields for each insert
        for i in inserts:
            on_hand = i.get('OnHand', 0) or 0
            in_prod = i.get('InProd', 0) or 0
            total_need = i.get('TotalNeed', 0) or 0
            
            # In-prod shortage
            i['ShortProd'] = max(0, in_prod - on_hand)
            i['SetsProd'] = int((i['ShortProd'] + 3) // 4) if i['ShortProd'] > 0 else 0
            
            # Total shortage
            i['ShortTotal'] = max(0, total_need - on_hand)
            i['SetsTotal'] = int((i['ShortTotal'] + 3) // 4) if i['ShortTotal'] > 0 else 0
        
        return render_template(
            'inserts.html',
            workcell_id=workcell_id,
            workcell_name=workcell_name,
            workcell_config=workcell_config,
            inserts=inserts,
            workcells=workcells
        )
    
    if dashboard_type == 'burn':
        # Burn dashboard - show billet summary
        billets = get_billet_summary()
        workcells = get_workcells()  # For the dropdown
        
        # Calculate shortage fields for each billet
        for b in billets:
            on_hand = b.get('OnHand', 0) or 0
            late_need = b.get('LateNeed', 0) or 0
            future_need = b.get('FutureNeed', 0) or 0
            
            # Total demand from die jobs
            b['TotalDemand'] = late_need + future_need
            
            # On-hand first covers late, remainder covers future
            remaining_after_late = max(0, on_hand - late_need)
            b['ShortLate'] = max(0, late_need - on_hand)
            b['ShortFuture'] = max(0, future_need - remaining_after_late)
            b['Shortage'] = b['ShortLate'] + b['ShortFuture']
        
        return render_template(
            'burn.html',
            workcell_id=workcell_id,
            workcell_name=workcell_name,
            workcell_config=workcell_config,
            billets=billets,
            workcells=workcells
        )
    
    # Standard queue view
    t1 = time.time()
    jobs = get_jobs_with_details(workcell_id)
    t2 = time.time()
    print(f"[TIMING] {workcell_id}: get_jobs_with_details took {t2-t1:.2f}s for {len(jobs)} jobs")
    
    workcells = get_workcells()  # For the dropdown
    
    # Get materials list for dropdown if this workcell uses material grouping
    # DISABLED for performance - need to optimize
    material_list = []
    # if workcell_config.get('group_by_material'):
    #     material_list = get_materials_for_workcell(workcell_id)
    
    t3 = time.time()
    response = render_template(
        'queue.html',
        workcell_id=workcell_id,
        workcell_name=workcell_name,
        workcell_config=workcell_config,
        jobs=jobs,
        workcells=workcells,
        material_list=material_list
    )
    t4 = time.time()
    print(f"[TIMING] {workcell_id}: render_template took {t4-t3:.2f}s")
    print(f"[TIMING] {workcell_id}: TOTAL server time {t4-t_start:.2f}s")
    return response


@views.route('/api/materials/<workcell_id>')
def api_materials(workcell_id):
    """API endpoint for material list - loaded async for performance."""
    if workcell_id not in WORKCELLS:
        return jsonify({'error': 'Work cell not found'}), 404
    
    materials = get_materials_for_workcell(workcell_id)
    return jsonify(materials)


@views.route('/api/jobs_by_material/<workcell_id>/<material_partnum>')
def api_jobs_by_material(workcell_id, material_partnum):
    """API endpoint to get job keys that use a specific material."""
    if workcell_id not in WORKCELLS:
        return jsonify({'error': 'Work cell not found'}), 404
    
    from app.logic.queries import get_jobs_using_material
    job_keys = get_jobs_using_material(workcell_id, material_partnum)
    return jsonify(job_keys)


@views.route('/api/colors/<workcell_id>')
def api_colors(workcell_id):
    """API endpoint for color list - loaded async for performance."""
    if workcell_id not in WORKCELLS:
        return jsonify({'error': 'Work cell not found'}), 404
    
    from app.logic.queries import get_colors_for_workcell
    colors = get_colors_for_workcell(workcell_id)
    return jsonify(colors)


@views.route('/api/jobs_by_color/<workcell_id>/<color>')
def api_jobs_by_color(workcell_id, color):
    """API endpoint to get job keys that have a specific finish color."""
    if workcell_id not in WORKCELLS:
        return jsonify({'error': 'Work cell not found'}), 404
    
    from app.logic.queries import get_jobs_using_color
    job_keys = get_jobs_using_color(workcell_id, color)
    return jsonify(job_keys)


@views.route('/api/resources/<workcell_id>')
def api_resources(workcell_id):
    """API endpoint for resource list - loaded async for Mill-Lathe."""
    if workcell_id not in WORKCELLS:
        return jsonify({'error': 'Work cell not found'}), 404
    
    from app.logic.queries import get_resources_for_workcell
    resources = get_resources_for_workcell(workcell_id)
    return jsonify(resources)


@views.route('/api/jobs_by_resource/<workcell_id>/<resource_id>')
def api_jobs_by_resource(workcell_id, resource_id):
    """API endpoint to get job keys that use a specific resource."""
    if workcell_id not in WORKCELLS:
        return jsonify({'error': 'Work cell not found'}), 404
    
    from app.logic.queries import get_jobs_using_resource
    job_keys = get_jobs_using_resource(workcell_id, resource_id)
    return jsonify(job_keys)


@views.route('/api/capabilities/<workcell_id>')
def api_capabilities(workcell_id):
    """API endpoint for capability list - loaded async for Mill-Lathe."""
    if workcell_id not in WORKCELLS:
        return jsonify({'error': 'Work cell not found'}), 404
    
    from app.logic.queries import get_capabilities_for_workcell
    capabilities = get_capabilities_for_workcell(workcell_id)
    return jsonify(capabilities)


@views.route('/api/jobs_by_capability/<workcell_id>/<capability_id>')
def api_jobs_by_capability(workcell_id, capability_id):
    """API endpoint to get job keys that use a specific capability."""
    if workcell_id not in WORKCELLS:
        return jsonify({'error': 'Work cell not found'}), 404
    
    from app.logic.queries import get_jobs_using_capability
    job_keys = get_jobs_using_capability(workcell_id, capability_id)
    return jsonify(job_keys)


@views.route('/api/queue/<workcell_id>')
def api_queue(workcell_id):
    """API endpoint for auto-refresh."""
    if workcell_id not in WORKCELLS:
        return jsonify({'error': 'Work cell not found'}), 404
    
    jobs = get_jobs_for_workcell(workcell_id)
    return jsonify(jobs)


@views.route('/api/job/<job_num>/<int:assembly_seq>/<int:opr_seq>')
def api_job_detail(job_num, assembly_seq, opr_seq):
    """API endpoint for job header, operations and materials - all in one call."""
    header = get_job_header(job_num)
    operations = get_job_operations(job_num)
    materials = get_job_materials(job_num, assembly_seq, opr_seq)
    return jsonify({
        'header': header,
        'operations': operations,
        'materials': materials
    })


@views.route('/api/job/<job_num>/last_entries')
def api_job_last_entries(job_num):
    """
    API endpoint for operation last entry dates - loaded on-demand when row expands.
    Returns dict mapping 'AssemblySeq-OprSeq' to last entry date string.
    Separated from bulk load for performance (LaborDtl lookup is expensive).
    """
    entries = get_operation_last_entries(job_num)
    return jsonify(entries)


@views.route('/api/employee/<emp_id>')
def api_employee(emp_id):
    """Validate employee ID and return name."""
    from app.logic.queries import get_employee
    employee = get_employee(emp_id)
    if employee:
        return jsonify(employee)
    return jsonify({'error': 'Employee not found'}), 404


@views.route('/api/test_epicor')
def api_test_epicor():
    """Test Epicor REST API connection."""
    import requests
    from requests.auth import HTTPBasicAuth
    from app.config import EPICOR_API_URL, EPICOR_API_KEY, EPICOR_USERNAME, EPICOR_PASSWORD
    
    # Debug: show what's loaded (masked)
    debug_info = {
        'api_url': EPICOR_API_URL,
        'api_key_len': len(EPICOR_API_KEY) if EPICOR_API_KEY else 0,
        'username': EPICOR_USERNAME,
        'password_len': len(EPICOR_PASSWORD) if EPICOR_PASSWORD else 0
    }
    
    if not EPICOR_API_URL or not EPICOR_API_KEY:
        return jsonify({'error': 'Epicor API not configured', 'debug': debug_info}), 500
    
    if not EPICOR_USERNAME or not EPICOR_PASSWORD:
        return jsonify({'error': 'Epicor credentials not configured', 'debug': debug_info}), 500
    
    try:
        # v1 format: /api/v1/Erp.BO.EmpBasicSvc/EmpBasics
        url = f"{EPICOR_API_URL}/Erp.BO.EmpBasicSvc/EmpBasics"
        headers = {
            'Accept': 'application/json',
            'x-api-key': EPICOR_API_KEY
        }
        
        # Basic Auth (Epicor user) + API Key
        auth = HTTPBasicAuth(EPICOR_USERNAME, EPICOR_PASSWORD)
        
        response = requests.get(url, headers=headers, auth=auth, timeout=10, verify=False)
        
        if response.ok:
            return jsonify({
                'status': 'connected',
                'debug': debug_info,
                'data': 'success'
            })
        else:
            return jsonify({
                'status': 'error',
                'code': response.status_code,
                'message': response.text[:500],
                'debug': debug_info
            }), response.status_code
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e), 'debug': debug_info}), 500


@views.route('/api/last_checkin/<part_num>')
@views.route('/api/last_checkin/<part_num>/<op_code>')
def api_last_checkin(part_num, op_code=None):
    """API endpoint for last labor check-in on a part number at an operation (for WELD)."""
    checkin = get_last_checkin(part_num, op_code)
    return jsonify(checkin)


@views.route('/api/pdf')
def api_pdf():
    """
    Serve a PDF file. Takes UNC path as query param, translates to local path.
    Usage: /api/pdf?path=\\\\server\\share\\file.pdf
    """
    unc_path = request.args.get('path')
    if not unc_path:
        abort(400, 'Missing path parameter')
    
    # Translate UNC path to local path
    local_path = translate_pdf_path(unc_path)
    
    if not local_path or not os.path.exists(local_path):
        abort(404, f'PDF not found: {local_path}')
    
    # Security check - ensure it's a PDF
    if not local_path.lower().endswith('.pdf'):
        abort(400, 'Only PDF files allowed')
    
    return send_file(local_path, mimetype='application/pdf')


# ============================================================================
# Labor Check-In Routes
# ============================================================================

@views.route('/api/labor/start', methods=['POST'])
def api_labor_start():
    """
    Start labor activity on a job operation.
    
    POST body: {
        "empId": "123",
        "jobNum": "JOB001",
        "asmSeq": 0,
        "oprSeq": 10
    }
    """
    from app.logic.epicor_api import start_activity
    
    data = request.get_json()
    print(f"[START] Received data: {data}")
    
    if not data:
        print("[START] No data provided")
        return jsonify({'success': False, 'error': 'No data provided'}), 400
    
    emp_id = data.get('empId')
    job_num = data.get('jobNum')
    asm_seq = data.get('asmSeq', 0)
    opr_seq = data.get('oprSeq')
    resource_grp_id = data.get('resourceGrpId', '')
    resource_id = data.get('resourceId', '')
    op_code = data.get('opCode', '')
    jc_dept = data.get('jcDept', '')
    capability_id = data.get('capabilityId', '')
    
    print(f"[START] Parsed: emp={emp_id}, job={job_num}, asm={asm_seq}, opr={opr_seq}, resGrp={resource_grp_id}, resId={resource_id}, op={op_code}, dept={jc_dept}, cap={capability_id}")
    
    if not all([emp_id, job_num, opr_seq is not None]):
        print(f"[START] Missing required fields: emp_id={emp_id}, job_num={job_num}, opr_seq={opr_seq}")
        return jsonify({'success': False, 'error': 'Missing required fields'}), 400
    
    print(f"[START] Calling start_activity...")
    result = start_activity(emp_id, job_num, asm_seq, opr_seq, resource_grp_id, resource_id, op_code, jc_dept, capability_id)
    print(f"[START] Result: {result}")
    
    if result['success']:
        return jsonify(result)
    else:
        return jsonify(result), 500


@views.route('/api/labor/end', methods=['POST'])
def api_labor_end():
    """
    End labor activity and report quantity.
    
    POST body: {
        "empId": "123",
        "laborHedSeq": 12345,
        "laborDtlSeq": 1,
        "laborQty": 10,
        "scrapQty": 0,
        "complete": false  // Optional - if true, marks operation complete
    }
    """
    from app.logic.epicor_api import end_activity
    
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No data provided'}), 400
    
    emp_id = data.get('empId')
    labor_hed_seq = data.get('laborHedSeq')
    labor_dtl_seq = data.get('laborDtlSeq')
    labor_qty = data.get('laborQty', 0)
    scrap_qty = data.get('scrapQty', 0)
    scrap_reason = data.get('scrapReasonCode', '')
    complete = data.get('complete', False)

    if not all([emp_id, labor_hed_seq, labor_dtl_seq is not None]):
        return jsonify({'success': False, 'error': 'Missing required fields'}), 400

    result = end_activity(emp_id, labor_hed_seq, labor_dtl_seq, labor_qty, scrap_qty, scrap_reason, complete)
    
    if result['success']:
        return jsonify(result)
    else:
        return jsonify(result), 500


@views.route('/api/labor/active/<emp_id>')
def api_labor_active(emp_id):
    """
    Get active labor records for an employee with enriched job details.
    """
    import sys
    print(f"[ACTIVE] Getting active labor for employee: {emp_id}", file=sys.stderr, flush=True)
    from app.logic.epicor_api import get_active_labor
    from app.logic.queries import get_active_labor_details
    
    active = get_active_labor(emp_id)
    print(f"[ACTIVE] Got {len(active)} records", file=sys.stderr, flush=True)
    
    if not active:
        return jsonify([])
    
    # Build list of job/op combinations for enrichment
    job_ops = [{
        'JobNum': rec.get('JobNum'),
        'AssemblySeq': rec.get('AssemblySeq', 0),
        'OprSeq': rec.get('OprSeq')
    } for rec in active]
    
    # Get enriched details from database
    details_map = get_active_labor_details(job_ops)
    
    # Merge details into active records
    enriched = []
    for rec in active:
        key = f"{rec.get('JobNum')}-{rec.get('AssemblySeq', 0)}-{rec.get('OprSeq')}"
        details = details_map.get(key, {})
        enriched.append({
            'LaborHedSeq': rec.get('LaborHedSeq'),
            'LaborDtlSeq': rec.get('LaborDtlSeq'),
            'JobNum': rec.get('JobNum'),
            'AssemblySeq': rec.get('AssemblySeq', 0),
            'OprSeq': rec.get('OprSeq'),
            'OpCode': details.get('OpCode', rec.get('OpCode', '')),
            'PartNum': details.get('PartNum', ''),
            'PartDescription': details.get('PartDescription', ''),
            'ProdQty': details.get('ProdQty', 0),
            'QtyCompleted': details.get('QtyCompleted', 0),
            'QtyLeft': details.get('QtyLeft', 0),
            'QtyAvailable': details.get('QtyAvailable'),  # None for first op
            'IsFirstOp': details.get('IsFirstOp', False),
            'MtlStatus': details.get('MtlStatus', 'none'),
            'MaxMtlQty': details.get('MaxMtlQty')  # None if no material constraint
        })
    
    return jsonify(enriched)


@views.route('/api/labor/report', methods=['POST'])
def api_labor_report():
    """
    Report labor quantity without Start/End tracking.
    Just creates a completed labor record.
    
    POST body: {
        "empId": "123",
        "jobNum": "JOB001",
        "asmSeq": 0,
        "oprSeq": 10,
        "laborQty": 10,
        "scrapQty": 0
    }
    """
    from app.logic.epicor_api import report_quantity_only
    
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No data provided'}), 400
    
    emp_id = data.get('empId')
    job_num = data.get('jobNum')
    asm_seq = data.get('asmSeq', 0)
    opr_seq = data.get('oprSeq')
    labor_qty = data.get('laborQty', 0)
    scrap_qty = data.get('scrapQty', 0)
    
    if not all([emp_id, job_num, opr_seq is not None]):
        return jsonify({'success': False, 'error': 'Missing required fields'}), 400
    
    result = report_quantity_only(emp_id, job_num, asm_seq, opr_seq, labor_qty, scrap_qty)
    
    if result['success']:
        return jsonify(result)
    else:
        return jsonify(result), 500


@views.route('/api/job/update-quantity', methods=['POST'])
def api_job_update_quantity():
    """
    Update job production quantity.
    Used for first operations to set the job quantity based on actual production.
    
    POST body: {
        "jobNum": "JOB001",
        "newQty": 100
    }
    """
    from app.logic.epicor_api import update_job_quantity
    
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No data provided'}), 400
    
    job_num = data.get('jobNum')
    new_qty = data.get('newQty')
    
    if not job_num or new_qty is None:
        return jsonify({'success': False, 'error': 'Missing jobNum or newQty'}), 400
    
    result = update_job_quantity(job_num, new_qty)
    
    if result['success']:
        return jsonify(result)
    else:
        return jsonify(result), 500


# ============================================================================
# Kanban Receipt Routes
# ============================================================================

print("========== KANBAN ROUTES LOADED ==========")

@views.route('/api/kanban/submit', methods=['GET', 'POST'])
def api_kanban_submit():
    """
    Submit a Kanban Receipt - creates job, reports qty, closes job, receives to stock.

    POST body: {
        "empId": "123",
        "partNum": "BILLET-2.5",
        "quantity": 10,
        "scrap": 0,  (optional)
        "scrapReason": "DEFECT"  (required if scrap > 0)
    }
    """
    print("[api_kanban_submit] Route called", flush=True)
    print(f"[api_kanban_submit] Method: {request.method}", flush=True)

    # Allow GET for testing
    if request.method == 'GET':
        return jsonify({'status': 'ok', 'message': 'Kanban endpoint is working. Use POST to submit.'})

    try:
        from app.logic.epicor_api import kanban_receipt
        print("[api_kanban_submit] Import successful", flush=True)

        data = request.get_json()
        print(f"[api_kanban_submit] Data: {data}", flush=True)

        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400

        emp_id = data.get('empId')
        part_num = data.get('partNum')
        quantity = data.get('quantity')
        scrap = data.get('scrap', 0)  # Optional, defaults to 0
        scrap_reason = data.get('scrapReason', '')  # Required if scrap > 0

        if not all([emp_id, part_num, quantity]):
            return jsonify({'success': False, 'error': 'Missing required fields (empId, partNum, quantity)'}), 400

        if quantity < 1:
            return jsonify({'success': False, 'error': 'Quantity must be at least 1'}), 400

        if scrap < 0:
            return jsonify({'success': False, 'error': 'Scrap cannot be negative'}), 400

        if scrap > 0 and not scrap_reason:
            return jsonify({'success': False, 'error': 'Scrap reason is required when scrap > 0'}), 400

        print(f"[api_kanban_submit] Calling kanban_receipt({emp_id}, {part_num}, {quantity}, scrap={scrap}, scrap_reason={scrap_reason})", flush=True)
        result = kanban_receipt(emp_id, part_num, quantity, scrap=scrap, scrap_reason=scrap_reason)
        print(f"[api_kanban_submit] Result: {result}", flush=True)
        
        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify(result), 400  # Use 400 instead of 500 for business errors
    
    except Exception as e:
        import traceback
        print(f"[api_kanban_submit] Exception: {e}", flush=True)
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@views.route('/api/kanban/last/<part_num>')
def api_kanban_last(part_num):
    """API endpoint for last Kanban receipt on a part number."""
    from app.logic.queries import get_last_kanban_receipt
    receipt = get_last_kanban_receipt(part_num)
    return jsonify(receipt)


@views.route('/api/parts/search')
def api_parts_search():
    """Search for parts by part number or description."""
    from app.logic.queries import search_parts
    query = request.args.get('q', '').strip()
    if len(query) < 2:
        return jsonify([])
    parts = search_parts(query)
    return jsonify(parts)


@views.route('/api/billet_summary')
def api_billet_summary():
    """API endpoint for billet summary - for async refresh."""
    billets = get_billet_summary()
    return jsonify(billets)


@views.route('/api/home_stats')
def api_home_stats():
    """
    Get stats for home page: workcell counts, total jobs, active workers.
    Returns all in one call for efficiency.
    """
    from app.logic.queries import get_all_workcell_counts, get_active_worker_count, get_total_queue_count
    
    counts = get_all_workcell_counts()
    active_workers = get_active_worker_count()
    total_jobs = get_total_queue_count()
    
    return jsonify({
        'workcell_counts': counts,
        'active_workers': active_workers,
        'total_jobs': total_jobs
    })
