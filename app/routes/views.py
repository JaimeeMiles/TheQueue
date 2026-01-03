# app/routes/views.py
# Version 5.0 — 2026-01-02
#
# Routes for The Queue web interface

from flask import Blueprint, render_template, jsonify, request
from app.logic.queries import (
    get_workcells, get_jobs_for_workcell, get_jobs_with_details, get_job_materials, 
    get_job_operations, get_job_header, get_workcell_config,
    get_last_checkin, get_materials_for_workcell, WORKCELLS
)

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


@views.route('/api/last_checkin/<part_num>')
@views.route('/api/last_checkin/<part_num>/<op_code>')
def api_last_checkin(part_num, op_code=None):
    """API endpoint for last labor check-in on a part number at an operation (for WELD)."""
    checkin = get_last_checkin(part_num, op_code)
    return jsonify(checkin)
