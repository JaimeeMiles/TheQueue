# app/logic/epicor_api.py
# Version 4.0 — 2026-01-03
#
# Epicor REST API v1 helper for labor transactions

import requests
from requests.auth import HTTPBasicAuth
from app.config import EPICOR_API_URL, EPICOR_USERNAME, EPICOR_PASSWORD

# Disable SSL warnings for self-signed certs
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_auth():
    """Get HTTP Basic Auth object."""
    return HTTPBasicAuth(EPICOR_USERNAME, EPICOR_PASSWORD)


def get_headers():
    """Standard headers for API calls."""
    return {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }


def api_get(endpoint):
    """GET request to Epicor API."""
    url = f"{EPICOR_API_URL}/{endpoint}"
    response = requests.get(url, headers=get_headers(), auth=get_auth(), verify=False, timeout=30)
    return response


def api_post(endpoint, data=None):
    """POST request to Epicor API."""
    url = f"{EPICOR_API_URL}/{endpoint}"
    response = requests.post(url, headers=get_headers(), auth=get_auth(), json=data or {}, verify=False, timeout=30)
    return response


def start_activity(emp_id, job_num, asm_seq, opr_seq, resource_grp_id='', resource_id='', op_code='', jc_dept=''):
    """
    Start labor activity on a job operation.
    
    Returns dict with success status and labor record or error message.
    """
    try:
        # Step 1: Clock in using EmpBasicSvc
        clockin_resp = api_post('Erp.BO.EmpBasicSvc/ClockIn', {
            'employeeID': emp_id,
            'shift': 1
        })
        
        clockin_error = ''
        if not clockin_resp.ok:
            clockin_error = f"ClockIn response: {clockin_resp.status_code} - {clockin_resp.text[:300]}"
        
        # Step 2: Get the active LaborHed for this employee
        active_resp = api_get(f"Erp.BO.LaborSvc/Labors?$filter=EmployeeNum eq '{emp_id}' and ActiveTrans eq true&$orderby=LaborHedSeq desc&$top=1")
        
        if not active_resp.ok:
            return {
                'success': False,
                'error': f"Could not find LaborHed: {active_resp.status_code}"
            }
        
        labor_heds = active_resp.json().get('value', [])
        if not labor_heds:
            return {
                'success': False,
                'error': f'No active LaborHed found after clock in. {clockin_error}'
            }
        
        labor_hed_seq = labor_heds[0]['LaborHedSeq']
        
        # Step 3: Get the full dataset
        getbyid_resp = api_post('Erp.BO.LaborSvc/GetByID', {
            'laborHedSeq': labor_hed_seq
        })
        
        if not getbyid_resp.ok:
            return {
                'success': False,
                'error': f"GetByID failed: {getbyid_resp.status_code} - {getbyid_resp.text[:500]}"
            }
        
        ds = getbyid_resp.json().get('returnObj', {})
        
        # Clear out existing LaborDtl - we don't want to touch them
        # StartActivity will create a fresh one
        ds['LaborDtl'] = []
        
        # Step 4: StartActivity to create a NEW LaborDtl
        start_resp = api_post('Erp.BO.LaborSvc/StartActivity', {
            'LaborHedSeq': labor_hed_seq,
            'StartType': 'P',
            'ds': ds
        })
        
        if not start_resp.ok:
            return {
                'success': False,
                'error': f"StartActivity failed: {start_resp.status_code} - {start_resp.text[:500]}"
            }
        
        ds = start_resp.json().get('parameters', {}).get('ds', {})
        
        # Now ds should have exactly ONE LaborDtl - the new one
        if not ds.get('LaborDtl') or len(ds['LaborDtl']) == 0:
            return {
                'success': False,
                'error': 'No LaborDtl record created by StartActivity'
            }
        
        # Work with the first (and should be only) record
        ds['LaborDtl'][0]['JobNum'] = job_num
        if op_code:
            ds['LaborDtl'][0]['OpCode'] = op_code
        
        default_job_resp = api_post('Erp.BO.LaborSvc/DefaultJobNum', {
            'jobNum': job_num,
            'ds': ds
        })
        
        if default_job_resp.ok:
            ds = default_job_resp.json().get('parameters', {}).get('ds', ds)
        else:
            return {
                'success': False,
                'error': f"DefaultJobNum failed: {default_job_resp.status_code} - {default_job_resp.text[:500]}"
            }
        
        # Step 6: Set operation and call DefaultOprSeq
        if ds.get('LaborDtl') and len(ds['LaborDtl']) > 0:
            ds['LaborDtl'][0]['AssemblySeq'] = asm_seq
            ds['LaborDtl'][0]['OprSeq'] = opr_seq
        
        default_opr_resp = api_post('Erp.BO.LaborSvc/DefaultOprSeq', {
            'OprSeq': opr_seq,
            'ds': ds
        })
        
        if default_opr_resp.ok:
            ds = default_opr_resp.json().get('parameters', {}).get('ds', ds)
        
        # Step 7: Set ResourceGrpID, ResourceID, JcDept, and Rework
        if ds.get('LaborDtl') and len(ds['LaborDtl']) > 0:
            if resource_grp_id:
                ds['LaborDtl'][0]['ResourceGrpID'] = resource_grp_id
            if resource_id:
                ds['LaborDtl'][0]['ResourceID'] = resource_id
            if jc_dept:
                ds['LaborDtl'][0]['JcDept'] = jc_dept
            ds['LaborDtl'][0]['Rework'] = False
        
        # Try calling DefaultResourceGrpID
        if resource_grp_id:
            default_res_resp = api_post('Erp.BO.LaborSvc/DefaultResourceGrpID', {
                'ResourceGrpID': resource_grp_id,
                'ds': ds
            })
            if default_res_resp.ok:
                ds = default_res_resp.json().get('parameters', {}).get('ds', ds)
        
        # Step 8: Update to save
        update_resp = api_post('Erp.BO.LaborSvc/Update', {
            'ds': ds
        })
        
        if not update_resp.ok:
            dtl_info = []
            if ds.get('LaborDtl'):
                for dtl in ds['LaborDtl']:
                    dtl_info.append(f"Job={dtl.get('JobNum')}, Op={dtl.get('OprSeq')}, RowMod={dtl.get('RowMod')}, ResGrp={dtl.get('ResourceGrpID')}, ResID={dtl.get('ResourceID')}, JcDept={dtl.get('JcDept')}, Rework={dtl.get('Rework')}")
            return {
                'success': False,
                'error': f"Update failed: {update_resp.status_code} - {update_resp.text[:300]}. LaborDtl: {dtl_info}"
            }
        
        final_ds = update_resp.json().get('parameters', {}).get('ds', {})
        labor_hed = final_ds.get('LaborHed', [{}])[0] if final_ds.get('LaborHed') else {}
        labor_dtl = final_ds.get('LaborDtl', [{}])[0] if final_ds.get('LaborDtl') else {}
        
        return {
            'success': True,
            'laborHedSeq': labor_hed.get('LaborHedSeq') or labor_dtl.get('LaborHedSeq'),
            'laborDtlSeq': labor_dtl.get('LaborDtlSeq'),
            'message': f"Started activity on {job_num} Op {opr_seq}"
        }
        
    except Exception as e:
        import traceback
        return {
            'success': False,
            'error': f"{str(e)}\n{traceback.format_exc()}"
        }


def end_activity(emp_id, labor_hed_seq, labor_dtl_seq, labor_qty, scrap_qty=0):
    """
    End labor activity and report quantity.
    
    Returns dict with success status or error message.
    """
    try:
        # Step 1: Get the labor dataset
        getbyid_resp = api_post('Erp.BO.LaborSvc/GetByID', {
            'laborHedSeq': labor_hed_seq
        })
        
        if not getbyid_resp.ok:
            return {
                'success': False,
                'error': f"GetByID failed: {getbyid_resp.status_code} - {getbyid_resp.text[:500]}"
            }
        
        ds = getbyid_resp.json().get('returnObj', {})
        
        # Find the right LaborDtl record and set quantities
        labor_dtl = None
        for dtl in ds.get('LaborDtl', []):
            if dtl.get('LaborDtlSeq') == labor_dtl_seq:
                labor_dtl = dtl
                break
        
        if not labor_dtl:
            return {
                'success': False,
                'error': f"LaborDtl record {labor_dtl_seq} not found"
            }
        
        # Set quantities
        labor_dtl['LaborQty'] = labor_qty
        labor_dtl['ScrapQty'] = scrap_qty
        labor_dtl['RowMod'] = 'U'
        
        # Step 2: End Activity
        end_resp = api_post('Erp.BO.LaborSvc/EndActivity', {
            'ds': ds
        })
        
        if not end_resp.ok:
            return {
                'success': False,
                'error': f"EndActivity failed: {end_resp.status_code} - {end_resp.text[:500]}"
            }
        
        ds = end_resp.json().get('parameters', {}).get('ds', ds)
        
        # Step 3: Update to commit
        update_resp = api_post('Erp.BO.LaborSvc/Update', {
            'ds': ds
        })
        
        if not update_resp.ok:
            return {
                'success': False,
                'error': f"Update failed: {update_resp.status_code} - {update_resp.text[:500]}"
            }
        
        return {
            'success': True,
            'message': f"Ended activity - reported {labor_qty} qty, {scrap_qty} scrap"
        }
        
    except Exception as e:
        import traceback
        return {
            'success': False,
            'error': f"{str(e)}\n{traceback.format_exc()}"
        }


def get_active_labor(emp_id):
    """
    Get active labor records for an employee.
    
    Returns list of active labor detail records.
    """
    import sys
    try:
        # Query LaborHed for active transactions (LaborDtl doesn't have EmployeeNum)
        url = f"Erp.BO.LaborSvc/Labors?$filter=EmployeeNum eq '{emp_id}' and ActiveTrans eq true&$expand=LaborDtls"
        print(f"[get_active_labor] Querying: {url}", file=sys.stderr, flush=True)
        resp = api_get(url)
        
        print(f"[get_active_labor] Response status: {resp.status_code}", file=sys.stderr, flush=True)
        if not resp.ok:
            print(f"[get_active_labor] Error response: {resp.text[:500]}", file=sys.stderr, flush=True)
            return []
        
        data = resp.json()
        labor_heds = data.get('value', [])
        print(f"[get_active_labor] Found {len(labor_heds)} active LaborHed records", file=sys.stderr, flush=True)
        
        # Collect all LaborDtl records from active LaborHeds
        records = []
        for hed in labor_heds:
            labor_hed_seq = hed.get('LaborHedSeq')
            dtls = hed.get('LaborDtls', [])
            print(f"[get_active_labor]   LaborHedSeq={labor_hed_seq}, has {len(dtls)} details", file=sys.stderr, flush=True)
            for dtl in dtls:
                # Only include active (not ended) details
                if dtl.get('ActiveTrans', True):  # Include if ActiveTrans is True or not present
                    dtl['LaborHedSeq'] = labor_hed_seq  # Ensure HedSeq is on the record
                    records.append(dtl)
                    print(f"[get_active_labor]     - Job={dtl.get('JobNum')}, Op={dtl.get('OprSeq')}, DtlSeq={dtl.get('LaborDtlSeq')}", file=sys.stderr, flush=True)
        
        return records
        
    except Exception as e:
        print(f"[get_active_labor] Exception: {e}", file=sys.stderr, flush=True)
        import traceback
        traceback.print_exc()
        return []
