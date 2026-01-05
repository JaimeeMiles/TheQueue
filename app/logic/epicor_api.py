# app/logic/epicor_api.py
# Version 4.0 — 2026-01-03
#
# Epicor REST API v1 helper for labor transactions

import requests
from requests.auth import HTTPBasicAuth
from app.config import EPICOR_API_URL, EPICOR_API_KEY, EPICOR_USERNAME, EPICOR_PASSWORD

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
        'Content-Type': 'application/json',
        'x-api-key': EPICOR_API_KEY
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


def end_activity(emp_id, labor_hed_seq, labor_dtl_seq, labor_qty, scrap_qty=0, complete=False):
    """
    End labor activity and report quantity.
    
    Args:
        emp_id: Employee ID
        labor_hed_seq: Labor header sequence
        labor_dtl_seq: Labor detail sequence
        labor_qty: Quantity completed
        scrap_qty: Scrap quantity
        complete: If True, also mark the operation as complete (OpComplete = True)
    
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
        
        # If complete flag is set, mark both the labor record and operation complete
        if complete:
            labor_dtl['OpComplete'] = True   # Marks JobOper as complete
            labor_dtl['Complete'] = True     # Marks this labor transaction as the completing one
        
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
            'message': f"Ended activity - reported {labor_qty} qty, {scrap_qty} scrap" + (" (Op Complete)" if complete else "")
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


def kanban_receipt(emp_id, part_num, quantity, warehouse='PROD', bin_num='PR-01'):
    """
    Process a Kanban Receipt - creates job, reports qty, closes job, receives to stock.
    
    Uses Erp.BO.KanbanReceiptsSvc which wraps all the steps into one transaction.
    
    Args:
        emp_id: Employee ID
        part_num: Part number to receive
        quantity: Quantity to receive
        warehouse: Warehouse code (default: PROD)
        bin_num: Bin number (default: PR-01)
    
    Returns dict with success status or error message.
    """
    import traceback
    
    # Write to a log file for debugging
    log_file = r'C:\Users\Jaimee\OneDrive - JD Squared Inc\Claude\TheQueue\kanban_debug.log'
    
    def log(msg):
        with open(log_file, 'a') as f:
            f.write(f"{msg}\n")
        print(msg, flush=True)
    
    try:
        log(f"[kanban_receipt] Starting: Part={part_num}, Qty={quantity}, Emp={emp_id}")
        
        # Step 1: KanbanReceiptsGetNew - create a new KanbanReceipts row
        getnew_resp = api_post('Erp.BO.KanbanReceiptsSvc/KanbanReceiptsGetNew', {})
        
        if not getnew_resp.ok:
            log(f"[kanban_receipt] KanbanReceiptsGetNew failed: {getnew_resp.status_code} - {getnew_resp.text[:500]}")
            return {
                'success': False,
                'error': f"KanbanReceiptsGetNew failed: {getnew_resp.status_code} - {getnew_resp.text[:500]}"
            }
        
        result = getnew_resp.json()
        log(f"[kanban_receipt] KanbanReceiptsGetNew response keys: {result.keys()}")
        
        # Extract the dataset - could be in 'parameters' or 'returnObj'
        ds = result.get('parameters', {}).get('ds', {})
        if not ds:
            ds = result.get('returnObj', {})
        if not ds:
            ds = result  # Maybe the whole response is the dataset
        
        log(f"[kanban_receipt] Dataset keys: {ds.keys() if isinstance(ds, dict) else 'not a dict'}")
        log(f"[kanban_receipt] KanbanReceipts records: {len(ds.get('KanbanReceipts', []))}")
        
        # Step 2: Set the part number and call ChangePart to validate/populate
        if ds.get('KanbanReceipts') and len(ds['KanbanReceipts']) > 0:
            ds['KanbanReceipts'][0]['PartNum'] = part_num
            log(f"[kanban_receipt] Set PartNum to {part_num}")
        else:
            log(f"[kanban_receipt] No KanbanReceipts record in dataset")
            return {
                'success': False,
                'error': 'No KanbanReceipts record created by GetNew'
            }
        
        change_part_resp = api_post('Erp.BO.KanbanReceiptsSvc/ChangePart', {
            'ds': ds,
            'partNum': part_num,
            'uomCode': 'EA'
        })
        
        if not change_part_resp.ok:
            log(f"[kanban_receipt] ChangePart failed: {change_part_resp.status_code} - {change_part_resp.text[:500]}")
            return {
                'success': False,
                'error': f"ChangePart failed: {change_part_resp.status_code} - {change_part_resp.text[:500]}"
            }
        
        ds = change_part_resp.json().get('parameters', {}).get('ds', ds)
        log(f"[kanban_receipt] ChangePart successful")
        
        # Step 3: Set quantity, warehouse, bin, employee
        if ds.get('KanbanReceipts') and len(ds['KanbanReceipts']) > 0:
            ds['KanbanReceipts'][0]['Quantity'] = float(quantity)
            ds['KanbanReceipts'][0]['WarehouseCode'] = warehouse
            ds['KanbanReceipts'][0]['BinNum'] = bin_num
            ds['KanbanReceipts'][0]['EmployeeID'] = str(emp_id)  # Field is EmployeeID not EmployeeNum
            log(f"[kanban_receipt] Set Qty={quantity}, Warehouse={warehouse}, Bin={bin_num}, EmployeeID={emp_id}")
        
        # Skip ChangeEmployee - we set EmployeeID directly
        # change_emp_resp = api_post(...)
        
        # Step 5: Call ChangeWarehouse
        change_wh_resp = api_post('Erp.BO.KanbanReceiptsSvc/ChangeWarehouse', {
            'ds': ds,
            'warehouseCode': warehouse
        })
        
        if change_wh_resp.ok:
            ds = change_wh_resp.json().get('parameters', {}).get('ds', ds)
            log(f"[kanban_receipt] ChangeWarehouse successful")
        else:
            log(f"[kanban_receipt] ChangeWarehouse warning: {change_wh_resp.status_code}")
        
        # Step 6: Call ChangeBin
        change_bin_resp = api_post('Erp.BO.KanbanReceiptsSvc/ChangeBin', {
            'ds': ds,
            'binNum': bin_num
        })
        
        if change_bin_resp.ok:
            ds = change_bin_resp.json().get('parameters', {}).get('ds', ds)
            log(f"[kanban_receipt] ChangeBin successful")
        else:
            log(f"[kanban_receipt] ChangeBin warning: {change_bin_resp.status_code}")
        
        # Step 7: PreProcessKanbanReceipts - validates everything
        preprocess_resp = api_post('Erp.BO.KanbanReceiptsSvc/PreProcessKanbanReceipts', {
            'ds': ds
        })
        
        if not preprocess_resp.ok:
            log(f"[kanban_receipt] PreProcess failed: {preprocess_resp.status_code} - {preprocess_resp.text[:500]}")
            return {
                'success': False,
                'error': f"PreProcessKanbanReceipts failed: {preprocess_resp.status_code} - {preprocess_resp.text[:500]}"
            }
        
        preprocess_result = preprocess_resp.json()
        log(f"[kanban_receipt] PreProcess response keys: {preprocess_result.keys()}")
        ds = preprocess_result.get('parameters', {}).get('ds', ds)
        if not ds:
            ds = preprocess_result.get('returnObj', ds)
        log(f"[kanban_receipt] PreProcess successful")
        
        # Log the dataset state before Process
        if ds.get('KanbanReceipts') and len(ds['KanbanReceipts']) > 0:
            kr = ds['KanbanReceipts'][0]
            log(f"[kanban_receipt] ALL fields before Process:")
            for key, val in sorted(kr.items()):
                log(f"  {key}: {repr(val)}")
            
            # Try setting ValidateOK to True
            kr['ValidateOK'] = True
            log(f"[kanban_receipt] Set ValidateOK = True")
        
        # Step 8: ProcessKanbanReceipts - does everything (create job, report, close, receive)
        process_resp = api_post('Erp.BO.KanbanReceiptsSvc/ProcessKanbanReceipts', {
            'ds': ds,
            'dSerialNoQty': 0
        })
        
        if not process_resp.ok:
            log(f"[kanban_receipt] Process failed: {process_resp.status_code}")
            log(f"[kanban_receipt] Process error response: {process_resp.text}")
            return {
                'success': False,
                'error': f"ProcessKanbanReceipts failed: {process_resp.status_code} - {process_resp.text[:500]}"
            }
        
        result = process_resp.json()
        log(f"[kanban_receipt] ProcessKanbanReceipts successful: {str(result)[:500]}")
        
        return {
            'success': True,
            'message': f"Received {quantity} of {part_num} to {warehouse}/{bin_num}"
        }
        
    except Exception as e:
        log(f"[kanban_receipt] Exception: {e}")
        log(traceback.format_exc())
        return {
            'success': False,
            'error': str(e)
        }


def update_job_quantity(job_num, new_qty):
    """
    Update the job production quantity via the Make To Stock demand link.
    
    This updates JobProd.MakeToStockQty which cascades to JobHead.ProdQty
    and recalculates all operation quantities.
    
    Args:
        job_num: Job number
        new_qty: New production quantity
    
    Returns dict with success status or error message.
    """
    import sys
    try:
        print(f"[update_job_quantity] Updating job {job_num} to qty {new_qty}", file=sys.stderr, flush=True)
        
        # Step 1: Get the job dataset via JobEntry BO
        getbyid_resp = api_post('Erp.BO.JobEntrySvc/GetByID', {
            'jobNum': job_num
        })
        
        if not getbyid_resp.ok:
            return {
                'success': False,
                'error': f"GetByID failed: {getbyid_resp.status_code} - {getbyid_resp.text[:500]}"
            }
        
        ds = getbyid_resp.json().get('returnObj', {})
        
        # Step 2: Find and update the JobProd record (Make To Stock demand link)
        job_prods = ds.get('JobProd', [])
        print(f"[update_job_quantity] Found {len(job_prods)} JobProd records", file=sys.stderr, flush=True)
        
        if not job_prods:
            # No JobProd record - this job might not have a demand link
            # Try updating JobHead.ProdQty directly as fallback
            job_heads = ds.get('JobHead', [])
            if job_heads:
                print(f"[update_job_quantity] No JobProd, updating JobHead directly", file=sys.stderr, flush=True)
                job_heads[0]['ProdQty'] = float(new_qty)
                job_heads[0]['RowMod'] = 'U'
            else:
                return {
                    'success': False,
                    'error': 'No JobHead or JobProd records found'
                }
        else:
            # Update the first JobProd record (Make To Stock qty)
            job_prods[0]['MakeToStockQty'] = float(new_qty)
            job_prods[0]['RowMod'] = 'U'
            print(f"[update_job_quantity] Set JobProd.MakeToStockQty = {new_qty}", file=sys.stderr, flush=True)
        
        # Step 3: Call Update to save
        update_resp = api_post('Erp.BO.JobEntrySvc/Update', {
            'ds': ds
        })
        
        if not update_resp.ok:
            return {
                'success': False,
                'error': f"Update failed: {update_resp.status_code} - {update_resp.text[:500]}"
            }
        
        print(f"[update_job_quantity] Successfully updated job {job_num} to qty {new_qty}", file=sys.stderr, flush=True)
        
        return {
            'success': True,
            'message': f"Updated job {job_num} quantity to {new_qty}"
        }
        
    except Exception as e:
        import traceback
        print(f"[update_job_quantity] Exception: {e}", file=sys.stderr, flush=True)
        traceback.print_exc()
        return {
            'success': False,
            'error': f"{str(e)}\n{traceback.format_exc()}"
        }
