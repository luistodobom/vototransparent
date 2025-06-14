import argparse
import os
import re
import json
import fitz # PyMuPDF
import pandas as pd
from datetime import datetime
from threading import Lock
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor

from utils import (download_file, generate_session_pdf_filename, init_directories, load_or_initialize_dataframe,
                   save_dataframe, extract_hyperlink_table_data, get_dataframe_columns)
from config import (GEMINI_API_KEY, PDF_PAGE_PARTITION_SIZE, SESSION_PDF_DIR,
                    PROPOSAL_DOC_DIR, YEAR, NUM_THREADS)
from prompts import create_prompt_for_session_pdf, create_prompt_for_proposal_pdf, call_gemini_api, validate_llm_proposals_response
from parliament_scraper import ParliamentPDFScraper, fetch_proposal_details_and_download_doc

# --- Step 1: Extract the Votes and Proposals from the Session PDF ---


def extract_votes_from_session_pdf(session_pdf_path, session_date):
    """
    Extracts voting information from a session PDF.
    It processes short PDFs as a single unit and long PDFs in page-based chunks.
    """
    print(f"Starting PDF parsing for: {session_pdf_path}")

    try:
        doc_fitz = fitz.open(session_pdf_path)
        page_count = len(doc_fitz)
        doc_fitz.close()
        print(f"PDF has {page_count} pages.")
    except Exception as e:
        print(f"Error opening PDF or getting page count: {e}")
        return None, f"Critical failure in PDF pre-processing: {e}"

    partitions_info = []
    process_as_single_unit = page_count <= PDF_PAGE_PARTITION_SIZE

    if process_as_single_unit:
        print(f"Processing PDF ({page_count} pages) as a single unit.")
        partitions_info.append({'start_page': 1, 'end_page': page_count})
    else:
        print(
            f"Processing long PDF ({page_count} pages) in chunks of {PDF_PAGE_PARTITION_SIZE} pages.")
        current_page = 1
        while current_page <= page_count:
            end_page = min(
                current_page + PDF_PAGE_PARTITION_SIZE - 1, page_count)
            partitions_info.append(
                {'start_page': current_page, 'end_page': end_page})
            current_page = end_page + 1
        print(f"Created {len(partitions_info)} partitions: {partitions_info}")

    all_proposals_collected = []
    accumulated_errors = []

    for i, part_info in enumerate(partitions_info):
        start_page = part_info['start_page']
        end_page = part_info['end_page']

        partition_label = f"Partition {i+1}/{len(partitions_info)}" if not process_as_single_unit else "PDF"
        print(f"Processing {partition_label}: pages {start_page}-{end_page}")

        try:
            # Determine how to call extract_hyperlink_table_data based on original behavior.
            # Original short PDF path: extract_hyperlink_table_data(session_pdf_path)
            # Original chunked path: extract_hyperlink_table_data(session_pdf_path, start_page, end_page)
            # This assumes extract_hyperlink_table_data can be called in these two ways.
            if process_as_single_unit:
                hyperlink_table_pairs, unpaired_links = extract_hyperlink_table_data(
                    session_pdf_path)
            else:
                hyperlink_table_pairs, unpaired_links = extract_hyperlink_table_data(
                    session_pdf_path, start_page=start_page, end_page=end_page
                )

            print(f"{partition_label}: Found {len(hyperlink_table_pairs)} hyperlink-table pairs and {len(unpaired_links)} unpaired links")

            if not hyperlink_table_pairs and not unpaired_links:
                print(
                    f"{partition_label}: No data extracted from PDF content, skipping LLM call.")
                continue

            prompt = create_prompt_for_session_pdf(
                hyperlink_table_pairs, unpaired_links, session_date)
            extracted_data, llm_error = call_gemini_api(
                prompt, expect_json=True, responseSchema=None)

            if llm_error:
                error_message = f"LLM API call failed: {llm_error}"
                print(f"{partition_label}: {error_message}")
                if process_as_single_unit:
                    return None, error_message  # Mimic original short PDF error
                accumulated_errors.append(
                    f"{partition_label}: {error_message}")
                continue

            if not isinstance(extracted_data, list):
                if isinstance(extracted_data, dict) and 'proposal_name' in extracted_data:
                    extracted_data = [extracted_data]
                else:
                    error_message = f"LLM did not return a list as expected. Got: {type(extracted_data)}"
                    print(f"{partition_label}: {error_message}")
                    if process_as_single_unit:
                        return None, error_message  # Mimic original short PDF error
                    accumulated_errors.append(
                        f"{partition_label}: {error_message}")
                    continue

            valid_proposals_from_partition = validate_llm_proposals_response(
                extracted_data)

            if valid_proposals_from_partition:
                print(
                    f"{partition_label}: Successfully extracted {len(valid_proposals_from_partition)} proposals.")
                all_proposals_collected.extend(valid_proposals_from_partition)
            else:
                if extracted_data:
                    # Log shorter message for multi-chunk, specific return for single unit
                    log_msg = f"LLM returned data but no valid proposal structures found. Raw: {str(extracted_data)[:100]}"
                    print(f"{partition_label}: {log_msg}")
                    if process_as_single_unit:
                        return None, f"LLM returned data but no valid proposal structures found. Raw: {str(extracted_data)[:500]}"
                    accumulated_errors.append(f"{partition_label}: {log_msg}")
                else:
                    print(
                        f"{partition_label}: No proposals extracted from LLM response (empty data).")
                    if process_as_single_unit:
                        return [], None  # Mimic original short PDF behavior for empty valid data

        except Exception as e:
            error_message = f"General error processing {partition_label} (pages {start_page}-{end_page}): {e}"
            print(error_message)
            if process_as_single_unit:
                # Mimic original "Critical failure in manual PDF parsing" for short PDFs
                return None, f"Critical failure in manual PDF parsing for {partition_label}: {e}"
            accumulated_errors.append(error_message)

    # After processing all partitions
    if all_proposals_collected:
        final_proposals = all_proposals_collected
        # Deduplicate if multiple partitions were processed
        if not process_as_single_unit and len(partitions_info) > 1:
            seen_proposal_identifiers = set()
            final_proposals = []
            for proposal in all_proposals_collected:
                prop_id = (proposal.get('proposal_name'),
                           proposal.get('proposal_link'))
                if prop_id not in seen_proposal_identifiers:
                    final_proposals.append(proposal)
                    seen_proposal_identifiers.add(prop_id)
            print(
                f"Successfully extracted {len(final_proposals)} unique proposals from all partitions.")
        else:
            print(f"Successfully extracted {len(final_proposals)} proposals.")

        if accumulated_errors:
            print(
                f"Note: Encountered {len(accumulated_errors)} non-critical errors during partition processing: {accumulated_errors}")
        return final_proposals, None

    elif accumulated_errors:
        return None, f"Failed to process PDF. Errors: {'; '.join(accumulated_errors)}"
    else:
        return [], None

# --- Step 2: Proposal Summary (Summarize Proposal Document with LLM) ---


def summarize_proposal_text(proposal_document_path):
    prompt = create_prompt_for_proposal_pdf()

    summary_data, error = call_gemini_api(
        prompt, document_path=proposal_document_path, expect_json=True)
    if error:
        return None, f"LLM API call failed for summary: {error}"

    if not isinstance(summary_data, dict):
        return None, f"LLM did not return a JSON object as expected. Got: {type(summary_data)}"

    required_fields = ['general_summary', 'critical_analysis', 'fiscal_impact',
                       'colloquial_summary', 'categories', 'short_title', 'proposing_party']
    for field in required_fields:
        if field not in summary_data:
            if field == 'proposing_party' and summary_data.get(field) is None:
                summary_data[field] = None
            else:
                return None, f"Missing required field '{field}' in LLM response: {summary_data}"

    # Handle categories field
    if not isinstance(summary_data.get('categories'), list):
        if isinstance(summary_data.get('categories'), int):
            summary_data['categories'] = [summary_data['categories']]
        elif summary_data.get('categories') is None:
            summary_data['categories'] = []
        else:
            return None, f"Field 'categories' should be a list, got: {type(summary_data.get('categories'))}"

    summary_data['categories'] = json.dumps(summary_data['categories'])

    # Ensure all text fields are strings, not lists
    text_fields = ['general_summary', 'critical_analysis', 'fiscal_impact', 
                   'colloquial_summary', 'short_title', 'proposing_party']
    for field in text_fields:
        value = summary_data.get(field)
        if isinstance(value, list):
            # If it's a list, join it into a string
            summary_data[field] = ' '.join(str(item) for item in value) if value else None
        elif value is not None and not isinstance(value, str):
            # Convert non-string, non-None values to string
            summary_data[field] = str(value)

    return summary_data, None

# --- Main Pipeline Orchestrator ---


def run_pipeline(start_year=None, end_year=None, max_sessions_to_process=None, dataframe_path=None):
    if not GEMINI_API_KEY:
        print("Critical Error: GEMINI_API_KEY is not set. The pipeline cannot run LLM-dependent stages.")
        return

    init_directories()
    df = load_or_initialize_dataframe(dataframe_path)
    df_lock = Lock()

    processed_dates_in_df = set()
    dates_to_reprocess = set()  # Last NUM_THREADS dates for multithreaded safety

    if not df.empty and 'session_date' in df.columns:
        all_dates_in_df = df['session_date'].dropna().unique()
        processed_dates_in_df = set(all_dates_in_df)
        
        if len(all_dates_in_df) > 0:
            # Get the last NUM_THREADS unique dates in the order they appear in the CSV
            # (which reflects processing completion order, not chronological order)
            # We need to preserve the order they appear in the DataFrame
            seen_dates = set()
            dates_in_file_order = []
            
            # Iterate through the DataFrame in reverse order to get the last appearances
            for date in reversed(df['session_date'].dropna().tolist()):
                if date not in seen_dates:
                    dates_in_file_order.append(date)
                    seen_dates.add(date)
                    if len(dates_in_file_order) >= NUM_THREADS:
                        break
            
            dates_to_reprocess = set(str(date) for date in dates_in_file_order)
            
            print(f"Found {len(processed_dates_in_df)} unique processed session dates in CSV.")
            print(f"Will reprocess last {len(dates_to_reprocess)} unique dates from end of CSV for multithreaded safety: {sorted(dates_to_reprocess)}")
        else:
            print("No valid session dates found in CSV.")

    scraper = ParliamentPDFScraper()
    current_year = datetime.now().year
    _start_year = start_year if start_year else current_year - 5
    _end_year = end_year if end_year else current_year

    print(
        f"--- Stage 1: Fetching all session PDF links from website for years {_start_year}-{_end_year} ---")
    all_session_pdf_infos_from_web = scraper.scrape_years(
        start_year=_start_year, end_year=_end_year)
    print(
        f"Found {len(all_session_pdf_infos_from_web)} potential session PDF links from web.")

    TERMINAL_SUCCESS_STATUSES = {
        'Success',
        'Completed (No Propostas)',
        'Completed (No Proposal Doc to Summarize)',
        'Completed (No Gov Link for Details)'
    }

    sessions_to_process_infos = []
    if not df.empty and 'session_date' in df.columns and processed_dates_in_df:
        for info in all_session_pdf_infos_from_web:
            current_web_session_date = info.get('date')  # Expected 'YYYY-MM-DD'

            if pd.isna(current_web_session_date):
                # If date from web is missing, process it to be safe or log error
                print(f"Warning: Session info from web has no date: {info['url']}. Adding for processing.")
                sessions_to_process_infos.append(info)
                continue

            if current_web_session_date in processed_dates_in_df:
                if current_web_session_date in dates_to_reprocess:
                    # This date is in the last NUM_THREADS dates, mark for re-processing.
                    print(f"Session date {current_web_session_date} is in last {NUM_THREADS} dates. Adding for re-processing.")
                    sessions_to_process_infos.append(info)
                else:
                    # This date is in CSV but not in the last NUM_THREADS dates, so skip.
                    # print(f"Skipping already processed session date: {current_web_session_date}")
                    pass
            else:
                # This date is not in CSV, so process.
                sessions_to_process_infos.append(info)
        print(f"Filtered to {len(sessions_to_process_infos)} sessions after considering processed dates.")
    else:  # DataFrame is empty or has no session_date column or no processed dates
        sessions_to_process_infos = all_session_pdf_infos_from_web
        print("Processing all sessions found from web (CSV empty or no relevant dates).")

    # Sort sessions: prioritize reprocessing dates from dates_to_reprocess, then by date.
    if dates_to_reprocess:
        sessions_to_process_infos.sort(key=lambda x: (str(x.get('date', '1900-01-01')) not in dates_to_reprocess, str(x.get('date', '1900-01-01')), x['url']))
    else:
        sessions_to_process_infos.sort(key=lambda x: (str(x.get('date', '1900-01-01')), x['url']))

    print(
        f"Total sessions to iterate through after filtering and sorting: {len(sessions_to_process_infos)}")

    sessions_to_actually_process = sessions_to_process_infos
    if max_sessions_to_process is not None and len(sessions_to_process_infos) > max_sessions_to_process:
        print(
            f"Limiting processing to {max_sessions_to_process} sessions due to max_sessions_to_process limit.")
        sessions_to_actually_process = sessions_to_process_infos[:max_sessions_to_process]

    # Nested function to process a single session
    def _process_single_session(session_info, df_obj, lock_obj, session_pdf_dir, proposal_doc_dir,
                                pipeline_start_year, dates_to_reprocess_set,
                                terminal_statuses, columns_func, dataframe_path):

        current_session_pdf_url = session_info['url']
        session_year = session_info.get('year')
        session_date = session_info.get('date')

        if "XVI_1_95_2025-03-05_ResultadoVotacoes_2025-03-05" in current_session_pdf_url:
            print(f"🔍 DEBUGGING: Processing target session PDF: {current_session_pdf_url}")
            # Set your breakpoint on the next line

        if not session_year:
            try:
                parsed_q = parse_qs(urlparse(current_session_pdf_url).query)
                fich_param = parsed_q.get('Fich', [None])[0]
                if fich_param:
                    match = re.search(r'(\d{4})[-_]\d{2}[-_]\d{2}', fich_param)
                    if match:
                        session_year = int(match.group(1))
            except:
                session_year = pipeline_start_year
        if not session_date:
            session_date = f"{session_year}-01-01" if session_year else f"{pipeline_start_year}-01-01"

        print(
            f"\n>>> Processing Session PDF URL: {current_session_pdf_url} (Year: {session_year}, Date: {session_date})")

        session_pdf_filename = generate_session_pdf_filename(
            current_session_pdf_url, session_year)
        session_pdf_local_path_for_download = os.path.join(
            session_pdf_dir, session_pdf_filename)

        with lock_obj:
            existing_rows_for_session_pdf = df_obj[df_obj['session_pdf_url']
                                                   == current_session_pdf_url]

        actual_session_pdf_disk_path = None
        session_pdf_download_status_for_df = 'Not Attempted'
        session_pdf_download_error_for_df = None

        if not existing_rows_for_session_pdf.empty:
            summary_rows = existing_rows_for_session_pdf[pd.isna(
                existing_rows_for_session_pdf['proposal_name_from_session'])]
            ref_row_candidates = summary_rows if not summary_rows.empty else existing_rows_for_session_pdf

            for _, ref_row in ref_row_candidates.iterrows():
                is_download_success = pd.notna(
                    ref_row['session_pdf_download_status']) and ref_row['session_pdf_download_status'] == 'Success'
                path_exists = pd.notna(ref_row['session_pdf_text_path']) and os.path.exists(
                    ref_row['session_pdf_text_path'])

                if is_download_success and path_exists:
                    print(
                        f"Session PDF already downloaded: {ref_row['session_pdf_text_path']}")
                    actual_session_pdf_disk_path = ref_row['session_pdf_text_path']
                    session_pdf_download_status_for_df = 'Success'
                    break

            if actual_session_pdf_disk_path is None and not ref_row_candidates.empty:
                if any(pd.notna(status) and status == 'Success' for status in ref_row_candidates['session_pdf_download_status']):
                    print(
                        f"Session PDF {current_session_pdf_url} marked downloaded in CSV but file missing or path invalid. Re-downloading.")

        if not actual_session_pdf_disk_path:
            download_success, msg_or_path = download_file(
                current_session_pdf_url, session_pdf_local_path_for_download)
            if download_success:
                actual_session_pdf_disk_path = msg_or_path
                session_pdf_download_status_for_df = 'Success'
            else:
                session_pdf_download_status_for_df = 'Download Failed'
                session_pdf_download_error_for_df = str(msg_or_path)

                with lock_obj:
                    placeholder_indices = df_obj[(df_obj['session_pdf_url'] == current_session_pdf_url) &
                                                 (df_obj['proposal_name_from_session'].isna())].index

                    if placeholder_indices.empty:
                        new_idx = len(df_obj)
                        df_obj.loc[new_idx,
                                   'session_pdf_url'] = current_session_pdf_url
                        df_obj.loc[new_idx, 'session_year'] = session_year
                        df_obj.loc[new_idx, 'session_date'] = session_date
                        for col in columns_func():
                            if col not in ['session_pdf_url', 'session_year', 'session_date']:
                                df_obj.loc[new_idx, col] = pd.NA
                    else:
                        new_idx = placeholder_indices[0]

                    df_obj.loc[new_idx,
                               'session_pdf_download_status'] = session_pdf_download_status_for_df
                    df_obj.loc[new_idx,
                               'last_error_message'] = session_pdf_download_error_for_df
                    df_obj.loc[new_idx,
                               'overall_status'] = 'Failed Stage 1 (Session PDF Download)'
                    df_obj.loc[new_idx, 'last_processed_timestamp'] = datetime.now(
                    ).isoformat()

                    other_indices = df_obj[(df_obj['session_pdf_url'] == current_session_pdf_url) &
                                           (df_obj['proposal_name_from_session'].notna())].index
                    for idx_other in other_indices:
                        df_obj.loc[idx_other,
                                   'session_pdf_download_status'] = session_pdf_download_status_for_df
                        df_obj.loc[idx_other,
                                   'last_error_message'] = session_pdf_download_error_for_df
                        df_obj.loc[idx_other,
                                   'overall_status'] = 'Failed Stage 1 (Session PDF Download)'
                        df_obj.loc[idx_other, 'last_processed_timestamp'] = datetime.now(
                        ).isoformat()
                return  # End processing for this session

        proposals_from_llm = None
        session_parse_status_for_df = 'Not Attempted'
        session_parse_error_for_df = None
        run_stage2_llm_parse = True

        with lock_obj:  # Protect read access to existing_rows_for_session_pdf for consistency
            # Re-fetch or ensure existing_rows_for_session_pdf is consistent if df_obj could change between locks
            existing_rows_for_session_pdf = df_obj[df_obj['session_pdf_url']
                                                   == current_session_pdf_url]
            if not existing_rows_for_session_pdf.empty:
                summary_row_no_propostas_status = existing_rows_for_session_pdf[
                    (pd.notna(existing_rows_for_session_pdf['session_parse_status'])) &
                    (existing_rows_for_session_pdf['session_parse_status'] == 'LLM Parsed - No Propostas Encontradas') &
                    (pd.isna(
                        existing_rows_for_session_pdf['proposal_name_from_session']))
                ]

                proposal_rows = existing_rows_for_session_pdf[pd.notna(
                    existing_rows_for_session_pdf['proposal_name_from_session'])]
                all_proposal_rows_parsed_successfully = True
                if not proposal_rows.empty:
                    all_proposal_rows_parsed_successfully = all(
                        pd.notna(status) and status == 'Success' for status in proposal_rows['session_parse_status'].dropna()
                    )
                else:
                    all_proposal_rows_parsed_successfully = True

                any_row_parsed_successfully = any(
                    pd.notna(status) and status == 'Success' for status in existing_rows_for_session_pdf['session_parse_status']
                )

                if not summary_row_no_propostas_status.empty or \
                   (not proposal_rows.empty and all_proposal_rows_parsed_successfully) or \
                   (proposal_rows.empty and any_row_parsed_successfully):

                    print(
                        f"Session PDF {current_session_pdf_url} appears to be parsed previously. Reconstructing proposals from CSV if any.")
                    run_stage2_llm_parse = False
                    proposals_from_llm = []
                    for _, row in existing_rows_for_session_pdf.iterrows():
                        if pd.notna(row['proposal_name_from_session']):
                            try:
                                voting_summary_obj = json.loads(row['voting_details_json']) if pd.notna(
                                    row['voting_details_json']) else None
                            except json.JSONDecodeError:
                                voting_summary_obj = None
                            proposals_from_llm.append({
                                'proposal_name': row['proposal_name_from_session'],
                                'proposal_link': row['proposal_gov_link'],
                                'voting_summary': voting_summary_obj,
                                'proposal_approval_status': row['proposal_approval_status']
                            })
                    if not proposals_from_llm and not summary_row_no_propostas_status.empty:
                        session_parse_status_for_df = 'LLM Parsed - No Propostas Encontradas'
                    elif proposals_from_llm:
                        session_parse_status_for_df = 'Success'
                    elif existing_rows_for_session_pdf['session_parse_status'].notna().any():
                        session_parse_status_for_df = existing_rows_for_session_pdf['session_parse_status'].dropna().iloc[0] \
                            if not existing_rows_for_session_pdf['session_parse_status'].dropna().empty else 'Unknown (Reconstructed)'
                    else:
                        session_parse_status_for_df = 'Unknown (Reconstructed)'

        if run_stage2_llm_parse:
            print(
                f"Running LLM parse for session PDF: {actual_session_pdf_disk_path}")
            with lock_obj:
                indices_to_drop = df_obj[(df_obj['session_pdf_url'] == current_session_pdf_url) &
                                         (df_obj['proposal_name_from_session'].notna())].index
                if not indices_to_drop.empty:
                    print(
                        f"Dropping {len(indices_to_drop)} old proposal entries for this session before re-parsing.")
                    df_obj.drop(indices_to_drop, inplace=True)
                    df_obj.reset_index(drop=True, inplace=True)

            print("This is the LLM Call for session PDF parsing.")
            proposals_from_llm, llm_error = extract_votes_from_session_pdf(
                actual_session_pdf_disk_path, session_date)

            if llm_error:
                session_parse_status_for_df = f'LLM Parse Failed: {llm_error}'
                session_parse_error_for_df = llm_error
            elif not proposals_from_llm:
                session_parse_status_for_df = 'LLM Parsed - No Propostas Encontradas'
            else:
                session_parse_status_for_df = 'Success'

        if session_parse_error_for_df or (session_parse_status_for_df == 'LLM Parsed - No Propostas Encontradas' and not proposals_from_llm):
            with lock_obj:
                summary_row_indices = df_obj[(df_obj['session_pdf_url'] == current_session_pdf_url) &
                                             (df_obj['proposal_name_from_session'].isna())].index

                summary_idx_to_update = -1
                if not summary_row_indices.empty:
                    summary_idx_to_update = summary_row_indices[0]
                else:
                    summary_idx_to_update = len(df_obj)
                    df_obj.loc[summary_idx_to_update,
                               'session_pdf_url'] = current_session_pdf_url
                    for col in columns_func():
                        if col not in ['session_pdf_url']:
                            df_obj.loc[summary_idx_to_update, col] = pd.NA

                df_obj.loc[summary_idx_to_update,
                           'session_year'] = session_year
                df_obj.loc[summary_idx_to_update,
                           'session_date'] = session_date
                df_obj.loc[summary_idx_to_update,
                           'session_pdf_text_path'] = actual_session_pdf_disk_path
                df_obj.loc[summary_idx_to_update,
                           'session_pdf_download_status'] = 'Success'
                df_obj.loc[summary_idx_to_update,
                           'session_parse_status'] = session_parse_status_for_df
                df_obj.loc[summary_idx_to_update,
                           'last_error_message'] = session_parse_error_for_df
                df_obj.loc[summary_idx_to_update,
                           'overall_status'] = 'Failed Stage 2 (LLM Session Parse)' if session_parse_error_for_df else 'Completed (No Propostas)'
                df_obj.loc[summary_idx_to_update,
                           'last_processed_timestamp'] = datetime.now().isoformat()

                if run_stage2_llm_parse:
                    indices_to_drop = df_obj[(df_obj['session_pdf_url'] == current_session_pdf_url) &
                                             (df_obj['proposal_name_from_session'].notna())].index
                    if not indices_to_drop.empty:
                        df_obj.drop(indices_to_drop, inplace=True)
                        df_obj.reset_index(drop=True, inplace=True)
                save_dataframe(df_obj, dataframe_path)
            return  # End processing for this session

        if proposals_from_llm is None or (not proposals_from_llm and not run_stage2_llm_parse):
            with lock_obj:
                summary_row_indices = df_obj[(df_obj['session_pdf_url'] == current_session_pdf_url) &
                                             (df_obj['proposal_name_from_session'].isna())].index
                if not summary_row_indices.empty:
                    summary_idx = summary_row_indices[0]
                    current_overall_status_val = df_obj.loc[summary_idx,
                                                            'overall_status']
                    is_terminal = pd.notna(
                        current_overall_status_val) and current_overall_status_val in terminal_statuses
                    if pd.isna(current_overall_status_val) or not is_terminal:
                        df_obj.loc[summary_idx,
                                   'overall_status'] = 'Completed (No Propostas)'
                        df_obj.loc[summary_idx,
                                   'session_parse_status'] = session_parse_status_for_df
                        df_obj.loc[summary_idx, 'last_processed_timestamp'] = datetime.now(
                        ).isoformat()
                else:
                    summary_idx = len(df_obj)
                    df_obj.loc[summary_idx,
                               'session_pdf_url'] = current_session_pdf_url
                    df_obj.loc[summary_idx, 'session_year'] = session_year
                    df_obj.loc[summary_idx, 'session_date'] = session_date
                    df_obj.loc[summary_idx,
                               'session_pdf_text_path'] = actual_session_pdf_disk_path
                    df_obj.loc[summary_idx,
                               'session_pdf_download_status'] = 'Success'
                    df_obj.loc[summary_idx,
                               'session_parse_status'] = session_parse_status_for_df
                    df_obj.loc[summary_idx,
                               'overall_status'] = 'Completed (No Propostas)'
                    df_obj.loc[summary_idx, 'last_processed_timestamp'] = datetime.now(
                    ).isoformat()
                save_dataframe(df_obj, dataframe_path)
            print(
                f"No proposals found or reconstructed for {current_session_pdf_url}.")
            return  # End processing for this session

        print(
            f"Found/Reconstructed {len(proposals_from_llm)} proposals for {current_session_pdf_url}.")

        for proposal_data_from_llm in proposals_from_llm:
            proposal_name = proposal_data_from_llm.get('proposal_name')
            proposal_gov_link = proposal_data_from_llm.get('proposal_link')
            voting_summary = proposal_data_from_llm.get('voting_summary')
            approval_status_from_llm = proposal_data_from_llm.get(
                'proposal_approval_status')

            if not proposal_name:
                print(
                    f"Skipping proposal with no name from LLM for session {current_session_pdf_url}")
                continue

            with lock_obj:
                proposal_row_match_indices = df_obj[
                    (df_obj['session_pdf_url'] == current_session_pdf_url) &
                    (df_obj['proposal_name_from_session'] == proposal_name) &
                    ((df_obj['proposal_gov_link'] == proposal_gov_link) if pd.notna(
                        proposal_gov_link) else df_obj['proposal_gov_link'].isna())
                ].index

                row_idx = -1
                if proposal_row_match_indices.empty:
                    row_idx = len(df_obj)
                    df_obj.loc[row_idx,
                               'session_pdf_url'] = current_session_pdf_url
                    df_obj.loc[row_idx, 'session_year'] = session_year
                    df_obj.loc[row_idx,
                               'proposal_name_from_session'] = proposal_name
                    for col in columns_func():
                        if col not in ['session_pdf_url', 'session_year', 'proposal_name_from_session']:
                            df_obj.loc[row_idx, col] = pd.NA
                else:
                    row_idx = proposal_row_match_indices[0]

                df_obj.loc[row_idx, 'session_date'] = session_date
                df_obj.loc[row_idx,
                           'session_pdf_text_path'] = actual_session_pdf_disk_path
                df_obj.loc[row_idx, 'session_pdf_download_status'] = 'Success'
                df_obj.loc[row_idx, 'proposal_gov_link'] = proposal_gov_link
                df_obj.loc[row_idx, 'voting_details_json'] = json.dumps(
                    voting_summary) if voting_summary else None
                df_obj.loc[row_idx,
                           'session_parse_status'] = session_parse_status_for_df
                df_obj.loc[row_idx,
                           'proposal_approval_status'] = approval_status_from_llm

                current_overall_status = df_obj.loc[row_idx, 'overall_status']
                is_current_overall_status_terminal = pd.notna(
                    current_overall_status) and current_overall_status in terminal_statuses

                if pd.isna(current_overall_status) or not is_current_overall_status_terminal:
                    df_obj.loc[row_idx,
                               'overall_status'] = 'Pending Further Stages'
                    df_obj.loc[row_idx, 'last_error_message'] = pd.NA
                    df_obj.loc[row_idx,
                               'proposal_details_scrape_status'] = pd.NA
                    df_obj.loc[row_idx, 'proposal_doc_download_status'] = pd.NA
                    df_obj.loc[row_idx, 'proposal_summarize_status'] = pd.NA

            # --- Stage 3: Get Proposal Details & Document ---
            needs_stage3_run = False
            if pd.notna(proposal_gov_link) and isinstance(proposal_gov_link, str) and proposal_gov_link.startswith("http"):
                current_scrape_status = df_obj.loc[row_idx,
                                                   'proposal_details_scrape_status']
                scrape_status_is_na = pd.isna(current_scrape_status)

                is_terminal_status_for_stage3 = False
                if not scrape_status_is_na:
                    is_terminal_status_for_stage3 = current_scrape_status in [
                        'Success', 'Success (No Doc Link)', 'No Gov Link', 'Fetch Failed']

                rerun_if_part_of_reprocessed_dates = False
                # Check if current session's date is in dates being reprocessed
                if str(session_date) in dates_to_reprocess_set:
                    is_perfect_stage3_success = False
                    if not scrape_status_is_na and current_scrape_status in ['Success', 'Success (No Doc Link)']:
                        is_perfect_stage3_success = True
                    if not is_perfect_stage3_success:
                        rerun_if_part_of_reprocessed_dates = True

                if scrape_status_is_na or not is_terminal_status_for_stage3 or rerun_if_part_of_reprocessed_dates:
                    needs_stage3_run = True
            else:
                current_overall_status_for_else = df_obj.loc[row_idx,
                                                             'overall_status']
                update_overall_status_to_no_gov_link = False
                if pd.notna(current_overall_status_for_else):
                    if current_overall_status_for_else == 'Pending Further Stages':
                        update_overall_status_to_no_gov_link = True
                elif pd.isna(current_overall_status_for_else):
                    update_overall_status_to_no_gov_link = True

                if update_overall_status_to_no_gov_link:
                    df_obj.loc[row_idx,
                               'overall_status'] = 'Completed (No Gov Link for Details)'
                df_obj.loc[row_idx,
                           'proposal_details_scrape_status'] = 'No Gov Link'

            if needs_stage3_run:
                print(
                    f"  Fetching details for proposal: {proposal_name} from {proposal_gov_link}")
                details_result = fetch_proposal_details_and_download_doc(
                    proposal_gov_link, proposal_doc_dir)
                df_obj.loc[row_idx,
                           'proposal_authors_json'] = details_result['authors_json']
                df_obj.loc[row_idx,
                           'proposal_document_url'] = details_result['document_info']['link']
                df_obj.loc[row_idx,
                           'proposal_document_type'] = details_result['document_info']['type']
                df_obj.loc[row_idx, 'proposal_document_local_path'] = details_result['document_info']['local_path']
                df_obj.loc[row_idx, 'proposal_doc_download_status'] = details_result['document_info']['download_status']
                df_obj.loc[row_idx,
                           'proposal_details_scrape_status'] = details_result['scrape_status']

                if details_result['error'] and \
                   (pd.isna(details_result['scrape_status']) or details_result['scrape_status'] != 'Success (No Doc Link)'):
                    df_obj.loc[row_idx, 'last_error_message'] = str(
                        details_result['error'])
                    df_obj.loc[row_idx,
                               'overall_status'] = 'Failed Stage 3 (Proposal Details Scrape)'
                elif pd.notna(df_obj.loc[row_idx, 'overall_status']) and df_obj.loc[row_idx, 'overall_status'] == 'Pending Further Stages':
                    df_obj.loc[row_idx, 'overall_status'] = 'Pending Stage 4'

            # --- Stage 4: Summarize Proposal Document ---
            needs_stage4_run = False
            doc_dl_status_s4 = df_obj.loc[row_idx,
                                          'proposal_doc_download_status']
            doc_is_successful_s4 = pd.notna(
                doc_dl_status_s4) and doc_dl_status_s4 == 'Success'

            overall_status_s4_val = df_obj.loc[row_idx, 'overall_status']
            overall_status_s4_str = str(
                overall_status_s4_val)  # Safe for startswith

            if doc_is_successful_s4 and \
               pd.notna(df_obj.loc[row_idx, 'proposal_document_local_path']) and \
               not overall_status_s4_str.startswith('Failed Stage 3'):

                current_summary_status_s4 = df_obj.loc[row_idx,
                                                       'proposal_summarize_status']

                force_rerun_summary_for_reprocessed_dates = False
                # Check if current session's date is in dates being reprocessed
                if str(session_date) in dates_to_reprocess_set:
                    if pd.isna(current_summary_status_s4) or (pd.notna(current_summary_status_s4) and current_summary_status_s4 != 'Success'):
                        force_rerun_summary_for_reprocessed_dates = True

                if pd.isna(current_summary_status_s4) or \
                   (pd.notna(current_summary_status_s4) and current_summary_status_s4 != 'Success') or \
                   force_rerun_summary_for_reprocessed_dates:
                    needs_stage4_run = True

            if needs_stage4_run:
                proposal_doc_disk_path_for_summary = df_obj.loc[row_idx,
                                                                'proposal_document_local_path']
                print(
                    f"  Summarizing proposal document: {proposal_doc_disk_path_for_summary}")
                summary_data, summary_err = summarize_proposal_text(
                    proposal_doc_disk_path_for_summary)
                if summary_err:
                    df_obj.loc[row_idx,
                               'proposal_summarize_status'] = f'LLM Summary Failed: {summary_err}'
                    df_obj.loc[row_idx, 'last_error_message'] = summary_err
                    df_obj.loc[row_idx,
                               'overall_status'] = 'Failed Stage 4 (LLM Summary)'
                else:
                    try:
                        df_obj.loc[row_idx,
                                   'proposal_summary_general'] = summary_data['general_summary']
                        df_obj.loc[row_idx,
                                   'proposal_summary_analysis'] = summary_data['critical_analysis']
                        df_obj.loc[row_idx,
                                   'proposal_summary_fiscal_impact'] = summary_data['fiscal_impact']
                        df_obj.loc[row_idx,
                                   'proposal_summary_colloquial'] = summary_data['colloquial_summary']
                        df_obj.loc[row_idx,
                                   'proposal_category'] = summary_data['categories']
                        df_obj.loc[row_idx,
                                   'proposal_short_title'] = summary_data['short_title']
                        df_obj.loc[row_idx,
                                   'proposal_proposing_party'] = summary_data['proposing_party']
                        df_obj.loc[row_idx,
                                   'proposal_summarize_status'] = 'Success'
                        df_obj.loc[row_idx, 'overall_status'] = 'Success'
                    except ValueError as e:
                        error_msg = f"DataFrame assignment error: {e}. Summary data types: {[(k, type(v)) for k, v in summary_data.items()]}"
                        print(f"Error in summary data assignment: {error_msg}")
                        df_obj.loc[row_idx, 'proposal_summarize_status'] = f'Assignment Error: {str(e)}'
                        df_obj.loc[row_idx, 'last_error_message'] = error_msg
                        df_obj.loc[row_idx, 'overall_status'] = 'Failed Stage 4 (Data Assignment)'

            current_os_final = df_obj.loc[row_idx, 'overall_status']
            is_pending_for_final_update = False
            if pd.notna(current_os_final):
                if current_os_final in ['Pending Further Stages', 'Pending Stage 4']:
                    is_pending_for_final_update = True
            elif pd.isna(current_os_final):
                is_pending_for_final_update = True

            if is_pending_for_final_update:
                summarize_status_val = df_obj.loc[row_idx,
                                                  'proposal_summarize_status']
                is_summarize_success = pd.notna(
                    summarize_status_val) and summarize_status_val == 'Success'

                doc_dl_status_final = df_obj.loc[row_idx,
                                                 'proposal_doc_download_status']
                details_scrape_status_final = df_obj.loc[row_idx,
                                                         'proposal_details_scrape_status']

                if is_summarize_success:
                    df_obj.loc[row_idx, 'overall_status'] = 'Success'
                else:
                    doc_not_success_final = True
                    if pd.notna(doc_dl_status_final) and doc_dl_status_final == 'Success':
                        doc_not_success_final = False

                    details_scrape_is_success_variant_final = False
                    if pd.notna(details_scrape_status_final) and details_scrape_status_final in ['Success', 'Success (No Doc Link)']:
                        details_scrape_is_success_variant_final = True

                    details_scrape_is_no_gov_link_final = False
                    if pd.notna(details_scrape_status_final) and details_scrape_status_final == 'No Gov Link':
                        details_scrape_is_no_gov_link_final = True

                    if doc_not_success_final and details_scrape_is_success_variant_final:
                        df_obj.loc[row_idx,
                                   'overall_status'] = 'Completed (No Proposal Doc to Summarize)'
                    elif details_scrape_is_no_gov_link_final:
                        df_obj.loc[row_idx,
                                   'overall_status'] = 'Completed (No Gov Link for Details)'

            df_obj.loc[row_idx,
                       'last_processed_timestamp'] = datetime.now().isoformat()
            save_dataframe(df_obj, dataframe_path)
        # End of for proposal_data_from_llm in proposals_from_llm
    # End of _process_single_session function

    # Prepare arguments for starmap
    starmap_args = []
    for s_info in sessions_to_actually_process:
        starmap_args.append((
            s_info, df, df_lock,
            SESSION_PDF_DIR, PROPOSAL_DOC_DIR, _start_year,
            dates_to_reprocess, TERMINAL_SUCCESS_STATUSES,
            get_dataframe_columns,  # Pass the function itself
            dataframe_path  # Pass the dataframe path
        ))

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        # Use executor.map with a lambda to unpack arguments for _process_single_session
        results = list(executor.map(
            lambda p: _process_single_session(*p), starmap_args))
        # results will contain None for each call as _process_single_session doesn't explicitly return a value other than early exits.
        # Error handling within _process_single_session updates the DataFrame.
        # If _process_single_session could raise exceptions that aren't caught, they would surface here.

    print("\n--- Pipeline Run Finished ---")
    if not df.empty:
        print("Overall Status Counts:")
        print(df['overall_status'].value_counts(dropna=False))
    else:
        print("DataFrame is empty.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Run the Parliament PDF Scraper Pipeline.")
    parser.add_argument(
        '--year', type=int, help="Start year for scraping (default: current year - 5)", default=YEAR)
    parser.add_argument(
        '--year_end', type=int, help="End year for scraping (default: 2020)", default=2020
    )

    args = parser.parse_args()
    year_to_use = args.year
    year_to_end = args.year_end
    dataframe_path_to_use = f"data/parliament_data_{year_to_use}.csv"

    run_pipeline(start_year=year_to_use, end_year=year_to_end, max_sessions_to_process=None, dataframe_path=dataframe_path_to_use)
