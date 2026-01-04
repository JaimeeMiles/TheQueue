# app/routes/views.py
# Version 5.0 — 2026-01-02
#
# Routes for The Queue web interface

print("========== VIEWS.PY LOADED ==========")

import os
from flask import Blueprint, render_template, jsonify, request, send_file, abort
from app.logic.queries import (
    get_workcells, get_jobs_for_workcell, get_jobs_with_details, get_job_materials, 
    get_job_operations, get_job_header, get_workcell_config,
    get_last_checkin, get_materials_for_workcell, WORKCELLS
)
from app.config import translate_pdf_path

views = Blueprint('views', __name__)


@views.route('/')
def index():
    """Home page - show work cell buttons."""
    workcells = get_workcells()
    return render_template('index.html', workcells=workcells)


@views.route('/queue/<workcell_id>')
def queue(workcell_id):
    """Queue page - show jobs ready for a work cell."""
    # Get work cell info
    if workcell_id not in WORKCELLS:
        return render_template('error.html', message=f"Work cell '{workcell_id}' not found"), 404
    
    workcell_config = get_workcell_config(workcell_id)
    workcell_name = workcell_config['name']
    jobs = get_jobs_with_details(workcell_id)
    workcells = get_workcells()  # For the dropdown
    
    # Get materials list for dropdown if this workcell uses material grouping
    # DISABLED for performance - need to optimize
    material_list = []
    # if workcell_config.get('group_by_material'):
    #     material_list = get_materials_for_workcell(workcell_id)
    
    return render_template(
        'queue.html',
        workcell_id=workcell_id,
        workcell_name=workcell_name,
        workcell_config=workcell_config,
        jobs=jobs,
        workcells=workcells,
        material_list=material_list
    )


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
    
    if not EPICOR_API_URL or not EPICOR_API_KEY:
        return jsonify({'error': 'Epicor API not configured'}), 500
    
    if not EPICOR_USERNAME or not EPICOR_PASSWORD:
        return jsonify({'error': 'Epicor credentials not configured'}), 500
    
    try:
        # v1 format: /api/v1/Erp.BO.EmpBasicSvc/EmpBasics
        url = f"{EPICOR_API_URL}/Erp.BO.EmpBasicSvc/EmpBasics"
        headers = {
            'Accept': 'application/json'
        }
        
        # Basic Auth (Epicor user) + API Key
        auth = HTTPBasicAuth(EPICOR_USERNAME, EPICOR_PASSWORD)
        
        response = requests.get(url, headers=headers, auth=auth, timeout=10, verify=False)
        
        if response.ok:
            return jsonify({
                'status': 'connected',
                'data': response.json() if response.text else 'empty'
            })
        else:
            return jsonify({
                'status': 'error',
                'code': response.status_code,
                'message': response.text[:500]
            }), response.status_code
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


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
    
    print(f"[START] Parsed: emp={emp_id}, job={job_num}, asm={asm_seq}, opr={opr_seq}, resGrp={resource_grp_id}, resId={resource_id}, op={op_code}, dept={jc_dept}")
    
    if not all([emp_id, job_num, opr_seq is not None]):
        print(f"[START] Missing required fields: emp_id={emp_id}, job_num={job_num}, opr_seq={opr_seq}")
        return jsonify({'success': False, 'error': 'Missing required fields'}), 400
    
    print(f"[START] Calling start_activity...")
    result = start_activity(emp_id, job_num, asm_seq, opr_seq, resource_grp_id, resource_id, op_code, jc_dept)
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
        "scrapQty": 0
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
    
    if not all([emp_id, labor_hed_seq, labor_dtl_seq is not None]):
        return jsonify({'success': False, 'error': 'Missing required fields'}), 400
    
    result = end_activity(emp_id, labor_hed_seq, labor_dtl_seq, labor_qty, scrap_qty)
    
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
    print(f"[ACTIVE] Got {len(active)} records: {active}", file=sys.stderr, flush=True)
    
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
            'IsFirstOp': details.get('IsFirstOp', False)
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
