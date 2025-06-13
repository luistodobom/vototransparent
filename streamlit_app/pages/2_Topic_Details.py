import streamlit as st
import pandas as pd
import os
import json
import re
from ast import literal_eval
import math # Added
import matplotlib.pyplot as plt # Added
from matplotlib.patches import Wedge # Added
import matplotlib.colors as mcolors # Added
import matplotlib.patches as mpatches # Added
import matplotlib.patheffects as path_effects 

# --- Page Configuration ---
st.set_page_config(
    page_title="Detalhes da Votação - VotoTransparente PT",
    page_icon="🇵🇹",
    layout="wide"
)

st.markdown("""
<style>
    /* Hide Streamlit's default sidebar navigation for multi-page apps */
    div[data-testid="stSidebarNav"] {
        display: none;
    }
    .stButton button {
        /* Ensure back button is not overly large if it inherits global styles */
        /* width: auto; */ /* Commented out as it might conflict with other buttons if not specific enough */
    }
</style>
""", unsafe_allow_html=True)

# --- Party Metadata and Chart Configuration ---
PARTY_METADATA = {
    "PS": {"color": "#CE18BC"},  # Pink
    "PSD": {"color": "#F26932"},   # Dark Orange
    "CH": {"color": "#001BA4"},    # Navy
    "IL": {"color": "#329DC1"},     # Deep Sky Blue
    "PCP": {"color": "#D01F20"},    # Red
    "BE": {"color": "#7E1CAA"},     # Dark Red / Maroon
    "PAN": {"color": "#76A639"},    # Green
    "L": {"color": "#2D2C31"},       # Light Sea Green
    "CDS-PP": {"color": "#0093DB"}, # Light Blue
}
ORDERED_PARTIES = ["PCP", "BE",  "L", "PS", "PAN", "PSD", "IL", "CH"] # Left to Right overall

# Define left/right groupings for abstention layout (must cover all parties in ORDERED_PARTIES)
# PAN is often center/center-left; placing with left for this layout.
LEFT_PARTIES_FOR_LAYOUT = ["BE", "PCP", "L", "PS", "PAN"]
RIGHT_PARTIES_FOR_LAYOUT = ["IL", "PSD", "CH"]


# Define wedge visual properties
DEFAULT_WEDGE_RADIUS = 1.0
DEFAULT_WEDGE_WIDTH = 0.35 
FAVOR_ALPHA = 1.0  # Opaque
CONTRA_ALPHA = 0.2 # More transparent for 'against'
ABSTAIN_COLOR = "#A9A9A9" # DarkGray for abstentions
ABSTAIN_ALPHA = 0.7


def generate_parliament_viz(all_party_vote_data_with_stance):
    active_parties_data = [
        p for p in all_party_vote_data_with_stance
        if p["stance"] == "favor" or p["stance"] == "contra"
    ]
    active_parties_data.sort(key=lambda x: ORDERED_PARTIES.index(x['name']))

    abstain_neutral_parties_data = [
        p for p in all_party_vote_data_with_stance
        if p["stance"] == "abstain" or p["stance"] == "neutral"
    ]

    if not active_parties_data and not abstain_neutral_parties_data:
        return None

    total_mps_active = sum(p["mps"] for p in active_parties_data)
    
    fig, ax = plt.subplots(figsize=(10, 6.5)) 
    ax.set_xlim(-1.3, 1.3) 
    ax.set_ylim(-1.3, 1.3) 
    ax.set_aspect('equal')
    ax.axis('off')

    # Add horizontal line at y=0 to separate top and bottom
    ax.axhline(0, color='black', linewidth=0.75, linestyle='-') # MODIFIED: Added horizontal line

    # --- Draw Top Semi-circle (Active Votes: Favor/Contra) ---
    if total_mps_active > 0:
        current_angle_deg_top = 180.0
        for party_data in active_parties_data: 
            party_name = party_data["name"]
            party_mps = party_data["mps"]
            base_color = party_data["base_color"]
            stance = party_data["stance"]

            if party_mps == 0: continue

            angle_span_deg = (party_mps / total_mps_active) * 180.0
            
            start_wedge_angle_deg = current_angle_deg_top - angle_span_deg
            end_wedge_angle_deg = current_angle_deg_top

            chosen_color = base_color
            current_alpha = FAVOR_ALPHA
            if stance == "contra":
                current_alpha = CONTRA_ALPHA
            
            # Determine linewidth based on party size
            if party_mps < 3:
                linewidth = 1
            elif party_mps <= 10:
                linewidth = 1.5
            else:
                linewidth = 2
            
            wedge = Wedge(center=(0, 0), r=DEFAULT_WEDGE_RADIUS, 
                          theta1=start_wedge_angle_deg, theta2=end_wedge_angle_deg, 
                          width=DEFAULT_WEDGE_WIDTH, facecolor=chosen_color, alpha=current_alpha, 
                          edgecolor=base_color, linewidth=linewidth)
            ax.add_patch(wedge)

            mid_angle_rad = math.radians((start_wedge_angle_deg + end_wedge_angle_deg) / 2)
            label_text_radius_base = DEFAULT_WEDGE_RADIUS - DEFAULT_WEDGE_WIDTH / 2 + 0.1 
            label_radius_factor = 1.15
            text_x = label_text_radius_base * math.cos(mid_angle_rad) * label_radius_factor
            text_y = label_text_radius_base * math.sin(mid_angle_rad) * label_radius_factor
            if 0 <= text_y < 0.05: text_y = 0.05 
            
            ax.text(text_x, text_y, f"{party_name}\n{party_mps}", 
                    ha='center', va='center', fontsize=7,
                    path_effects=[path_effects.withStroke(linewidth=1.5, foreground="white")])
            current_angle_deg_top = start_wedge_angle_deg # Corrected from -= angle_span_deg

    # --- Draw Bottom Arcs (Abstaining/Neutral Votes) ---
    abstain_left_to_draw = sorted(
        [p for p in abstain_neutral_parties_data if p["name"] in LEFT_PARTIES_FOR_LAYOUT and p["mps"] > 0],
        key=lambda x: ORDERED_PARTIES.index(x['name'])
    )
    abstain_right_to_draw = sorted(
        [p for p in abstain_neutral_parties_data if p["name"] in RIGHT_PARTIES_FOR_LAYOUT and p["mps"] > 0],
        key=lambda x: ORDERED_PARTIES.index(x['name']) 
    )
    
    total_mps_abstain_left = sum(p["mps"] for p in abstain_left_to_draw)
    total_mps_abstain_right = sum(p["mps"] for p in abstain_right_to_draw)
    total_mps_all_abstain = total_mps_abstain_left + total_mps_abstain_right
    
    MAX_ABSTAIN_ANGLE_CONCEPTUAL_TOTAL = 90.0 # Conceptual total for proportionality
    MAX_ANGLE_PER_SIDE_SLOT = 45.0 # Max degrees for each physical slot (left/right bottom)

    # Draw Left Abstaining (180 to 225 degrees - 45 degree slot)
    if total_mps_abstain_left > 0 and total_mps_all_abstain > 0:
        current_angle_deg_bottom_left = 180.0 
        for party_data in abstain_left_to_draw:
            party_name = party_data["name"]
            party_mps = party_data["mps"]
            
            target_span = (party_mps / total_mps_all_abstain) * MAX_ABSTAIN_ANGLE_CONCEPTUAL_TOTAL
            # Span for this party, capped at the max for a single side (e.g. if it's a huge party)
            span_this_party_capped = min(target_span, MAX_ANGLE_PER_SIDE_SLOT) 
            
            # Actual span to draw, further constrained by remaining space in this side's 45-deg slot
            remaining_slot_angle = (180.0 + MAX_ANGLE_PER_SIDE_SLOT) - current_angle_deg_bottom_left
            final_span_to_draw = min(span_this_party_capped, remaining_slot_angle)

            if final_span_to_draw < 0.01: # Effectively zero, skip or break
                continue

            start_wedge_angle_deg = current_angle_deg_bottom_left
            end_wedge_angle_deg = current_angle_deg_bottom_left + final_span_to_draw
            
            # Determine linewidth based on party size
            if party_mps < 3:
                linewidth = 1
            elif party_mps <= 10:
                linewidth = 1.5
            else:
                linewidth = 2
            
            wedge = Wedge(center=(0, 0), r=DEFAULT_WEDGE_RADIUS, 
                          theta1=start_wedge_angle_deg, theta2=end_wedge_angle_deg, 
                          width=DEFAULT_WEDGE_WIDTH, facecolor=ABSTAIN_COLOR, alpha=ABSTAIN_ALPHA, 
                          edgecolor='black', linewidth=linewidth)
            ax.add_patch(wedge)

            if final_span_to_draw > 1.0: # Only add label if wedge is somewhat visible
                mid_angle_rad = math.radians((start_wedge_angle_deg + end_wedge_angle_deg) / 2)
                label_text_radius_base = DEFAULT_WEDGE_RADIUS - DEFAULT_WEDGE_WIDTH / 2 + 0.1
                label_radius_factor = 1.15
                text_x = label_text_radius_base * math.cos(mid_angle_rad) * label_radius_factor
                text_y = label_text_radius_base * math.sin(mid_angle_rad) * label_radius_factor
                if 0 > text_y > -0.05: text_y = -0.05 

                ax.text(text_x, text_y, f"{party_name}\n{party_mps}", 
                        ha='center', va='center', fontsize=7,
                        path_effects=[path_effects.withStroke(linewidth=1.5, foreground="white")])
            current_angle_deg_bottom_left = end_wedge_angle_deg 

    # Draw Right Abstaining (315 to 360 degrees - 45 degree slot)
    if total_mps_abstain_right > 0 and total_mps_all_abstain > 0:
        current_angle_deg_bottom_right = 360.0 
        for party_data in reversed(abstain_right_to_draw): 
            party_name = party_data["name"]
            party_mps = party_data["mps"]

            target_span = (party_mps / total_mps_all_abstain) * MAX_ABSTAIN_ANGLE_CONCEPTUAL_TOTAL
            span_this_party_capped = min(target_span, MAX_ANGLE_PER_SIDE_SLOT)
            
            remaining_slot_angle = current_angle_deg_bottom_right - (360.0 - MAX_ANGLE_PER_SIDE_SLOT)
            final_span_to_draw = min(span_this_party_capped, remaining_slot_angle)

            if final_span_to_draw < 0.01:
                continue
            
            start_wedge_angle_deg = current_angle_deg_bottom_right - final_span_to_draw
            end_wedge_angle_deg = current_angle_deg_bottom_right
            
            # Determine linewidth based on party size
            if party_mps < 3:
                linewidth = 1
            elif party_mps <= 10:
                linewidth = 1.5
            else:
                linewidth = 2
            
            wedge = Wedge(center=(0, 0), r=DEFAULT_WEDGE_RADIUS, 
                          theta1=start_wedge_angle_deg, theta2=end_wedge_angle_deg, 
                          width=DEFAULT_WEDGE_WIDTH, facecolor=ABSTAIN_COLOR, alpha=ABSTAIN_ALPHA, 
                          edgecolor='black', linewidth=linewidth)
            ax.add_patch(wedge)

            if final_span_to_draw > 1.0: # Only add label if wedge is somewhat visible
                mid_angle_rad = math.radians((start_wedge_angle_deg + end_wedge_angle_deg) / 2)
                label_text_radius_base = DEFAULT_WEDGE_RADIUS - DEFAULT_WEDGE_WIDTH / 2 + 0.1
                label_radius_factor = 1.15
                text_x = label_text_radius_base * math.cos(mid_angle_rad) * label_radius_factor
                text_y = label_text_radius_base * math.sin(mid_angle_rad) * label_radius_factor
                if 0 > text_y > -0.05: text_y = -0.05

                ax.text(text_x, text_y, f"{party_name}\n{party_mps}", 
                        ha='center', va='center', fontsize=7,
                        path_effects=[path_effects.withStroke(linewidth=1.5, foreground="white")])
            current_angle_deg_bottom_right = start_wedge_angle_deg
    
    # Legend
    representative_color_for_legend = PARTY_METADATA["PS"]["color"] 
    patch_favor = mpatches.Patch(color=representative_color_for_legend, alpha=FAVOR_ALPHA, label='A Favor')
    patch_contra = mpatches.Patch(color=representative_color_for_legend, alpha=CONTRA_ALPHA, label='Contra')
    patch_abstain = mpatches.Patch(color=ABSTAIN_COLOR, alpha=ABSTAIN_ALPHA, label='Abstenção / Neutro')
    
    handles = []
    if total_mps_active > 0: 
        handles.extend([patch_favor, patch_contra])
    if total_mps_abstain_left > 0 or total_mps_abstain_right > 0: # Check if any abstainers were drawn
        handles.append(patch_abstain)

    if handles:
        ax.legend(handles=handles, 
                  loc='lower center', 
                  bbox_to_anchor=(0.5, 0.1), # Changed: moved much closer from -0.08 to -0.02
                  ncol=len(handles), 
                  fontsize=8, 
                  frameon=False)
    
    fig.subplots_adjust(bottom=0.08)  # Changed: reduced from 0.15 to 0.08 since legend is closer
    
    return fig

# --- Data Loading ---
# [[REPLACE THE EXISTING load_data FUNCTION WITH THE ONE DEFINED ABOVE]]
# The load_data function is identical to the one in streamlit_app.py
# Path adjustment for pages is handled by the default path.
@st.cache_data  
def load_data(csv_path="data/parliament_data.csv"):
    final_csv_path = csv_path
    if not os.path.exists(final_csv_path):
        alternative_path = os.path.join("..", csv_path) 
        if os.path.exists(alternative_path):
            final_csv_path = alternative_path
        else:
            st.error(f"Error: The data file '{os.path.abspath(final_csv_path)}' (and alternative '{os.path.abspath(alternative_path)}') was not found. "
                     f"Working directory: '{os.getcwd()}'. Please ensure it's generated.")
            return pd.DataFrame()

    try:
        raw_df = pd.read_csv(final_csv_path)
        all_vote_details = []

        def extract_bid(url):
            if pd.isna(url) or url == "":
                return None
            match = re.search(r'BID=(\d+)', url)
            if match:
                return match.group(1)
            match_alt = re.search(r'Detalhe(?:Iniciativa|ProjetoVoto)\.aspx\?BID=(\d+)', str(url))
            if match_alt:
                return match_alt.group(1)
            return None

        for index, row in raw_df.iterrows():
            issue_id_str = extract_bid(row.get('proposal_gov_link'))
            if issue_id_str is None:
                issue_id_str = row.get('proposal_name_from_session', f"fallback_id_{index}")
            
            title = row.get('proposal_name_from_session', 'Título não disponível.')
            description_text = row.get('proposal_summary_general', 'Descrição não disponível.')
            hyperlink_url = row.get('proposal_document_url', row.get('proposal_gov_link', ''))
            issue_type = row.get('proposal_document_type', 'N/A')
            authors_json_str = str(row.get('proposal_authors_json', '[]')) if pd.notna(row.get('proposal_authors_json')) else '[]'
            summary_analysis = str(row.get('proposal_summary_analysis', '')) if pd.notna(row.get('proposal_summary_analysis')) else ''
            summary_fiscal = str(row.get('proposal_summary_fiscal_impact', '')) if pd.notna(row.get('proposal_summary_fiscal_impact')) else ''
            summary_colloquial = str(row.get('proposal_summary_colloquial', '')) if pd.notna(row.get('proposal_summary_colloquial')) else ''
            session_pdf_url_val = row.get('session_pdf_url', '')
            session_date_val = row.get('session_date', '')

            # New fields
            proposal_short_title_val = str(row.get('proposal_short_title', 'N/A'))
            proposal_proposing_party_val = str(row.get('proposal_proposing_party', 'N/A'))
            proposal_approval_status_raw = row.get('proposal_approval_status', pd.NA)

            # Parse proposal_category as list of integers
            proposal_category_raw = row.get('proposal_category', '[]')
            proposal_category_list = []
            if pd.notna(proposal_category_raw) and str(proposal_category_raw).strip():
                try:
                    if isinstance(proposal_category_raw, str):
                        proposal_category_list = json.loads(proposal_category_raw.replace("'", '"'))
                    elif isinstance(proposal_category_raw, list):
                        proposal_category_list = proposal_category_raw
                    proposal_category_list = [int(cat) for cat in proposal_category_list if str(cat).isdigit()]
                except (json.JSONDecodeError, ValueError):
                    proposal_category_list = []

            # Parse proposal_proposing_party as list
            try:
                if isinstance(proposal_proposing_party_val, str) and proposal_proposing_party_val.startswith('['):
                    proposal_proposing_party_list = json.loads(proposal_proposing_party_val.replace("'", '"'))
                    if isinstance(proposal_proposing_party_list, list) and proposal_proposing_party_list:
                        proposal_proposing_party_val = proposal_proposing_party_list[0]  # Take first party for display
                    else:
                        proposal_proposing_party_val = 'N/A'
                else:
                    proposal_proposing_party_val = str(proposal_proposing_party_val)
            except:
                proposal_proposing_party_val = 'N/A'

            # Extract parties and votes information from voting_details_json
            voting_details_raw = row.get('voting_details_json', '')
            if pd.isna(voting_details_raw) or voting_details_raw == '':
                continue  # Skip rows with no voting info
            
            try:
                voting_details = json.loads(voting_details_raw)
            except (ValueError, json.JSONDecodeError):
                continue  # Skip rows with malformed voting info

            if not isinstance(voting_details, dict):
                continue

            # Determine overall vote outcome
            total_favor = sum(party_data.get('Favor', 0) for party_data in voting_details.values())
            total_contra = sum(party_data.get('Contra', 0) for party_data in voting_details.values())
            
            overall_outcome = "Resultado Desconhecido"
            if pd.notna(proposal_approval_status_raw):
                try:
                    status_as_int = int(proposal_approval_status_raw)
                    if status_as_int == 1:
                        overall_outcome = "Aprovado"
                    elif status_as_int == 0:
                        overall_outcome = "Rejeitado"
                except ValueError:
                    pass

            total_active_votes = total_favor + total_contra
            is_unanimous_bool = total_active_votes > 0 and (total_favor == total_active_votes or total_contra == total_active_votes)

            # Store party votes for this proposal
            proposal_party_votes_list = []
            for party_name, votes_data in voting_details.items():
                if not isinstance(votes_data, dict):
                    continue
                
                votes_favor = votes_data.get('Favor', 0)
                votes_against = votes_data.get('Contra', 0)
                votes_abstention = votes_data.get('Abstenção', 0)
                votes_not_voted = votes_data.get('Não Votaram', 0)
                
                # Skip party if no data
                if all(v == 0 for v in [votes_favor, votes_against, votes_abstention, votes_not_voted]):
                    continue

                proposal_party_votes_list.append({
                    'party': party_name,
                    'votes_favor': votes_favor,
                    'votes_against': votes_against,
                    'votes_abstention': votes_abstention,
                    'votes_not_voted': votes_not_voted,
                })
            if proposal_party_votes_list:
                for p_vote in proposal_party_votes_list:
                    all_vote_details.append({
                        'issue_identifier': issue_id_str, 'full_title': title, 'description': description_text,
                        'hyperlink': hyperlink_url, 'vote_outcome': overall_outcome, 'is_unanimous': is_unanimous_bool,
                        'issue_type': issue_type, 'party': p_vote['party'],
                        'votes_favor': p_vote['votes_favor'], 'votes_against': p_vote['votes_against'],
                        'votes_abstention': p_vote['votes_abstention'], 'votes_not_voted': p_vote['votes_not_voted'],
                        'authors_json_str': authors_json_str,
                        'proposal_summary_analysis': summary_analysis,
                        'proposal_summary_fiscal_impact': summary_fiscal,
                        'proposal_summary_colloquial': summary_colloquial,
                        'session_pdf_url': session_pdf_url_val,
                        'session_date': session_date_val,
                        'proposal_category_list': proposal_category_list,
                        'proposal_short_title': proposal_short_title_val,
                        'proposal_proposing_party': proposal_proposing_party_val,
                        'proposal_approval_status': proposal_approval_status_raw,
                    })
            else:
                all_vote_details.append({
                    'issue_identifier': issue_id_str, 'full_title': title, 'description': description_text,
                    'hyperlink': hyperlink_url, 'vote_outcome': overall_outcome, 'is_unanimous': is_unanimous_bool,
                    'issue_type': issue_type, 'party': 'N/A',
                    'votes_favor': 0, 'votes_against': 0, 'votes_abstention': 0, 'votes_not_voted': 0,
                    'authors_json_str': authors_json_str,
                    'proposal_summary_analysis': summary_analysis,
                    'proposal_summary_fiscal_impact': summary_fiscal,
                    'proposal_summary_colloquial': summary_colloquial,
                    'session_pdf_url': session_pdf_url_val,
                    'session_date': session_date_val,
                    'proposal_category_list': proposal_category_list,
                    'proposal_short_title': proposal_short_title_val,
                    'proposal_proposing_party': proposal_proposing_party_val,
                    'proposal_approval_status': proposal_approval_status_raw,
                })
        
        if not all_vote_details: st.info("No vote data could be processed."); return pd.DataFrame()
        df = pd.DataFrame(all_vote_details)
        
        # Ensure session_date column exists and convert to datetime
        if 'session_date' in df.columns:
            df['session_date'] = pd.to_datetime(df['session_date'], errors='coerce')
        else:
            df['session_date'] = pd.NaT
            
        expected_cols = [
            'issue_identifier', 'full_title', 'description', 'hyperlink', 'vote_outcome', 'is_unanimous', 
            'issue_type', 'party', 'votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted',
            'authors_json_str', 'proposal_summary_analysis', 'proposal_summary_fiscal_impact', 'proposal_summary_colloquial',
            'session_pdf_url', 'session_date', 'proposal_category_list',
            'proposal_short_title', 'proposal_proposing_party', 'proposal_approval_status' # Added new columns
        ]
        for col in expected_cols:
            if col not in df.columns:
                if col in ['votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted']: df[col] = 0
                elif col == 'is_unanimous': df[col] = False
                elif col == 'proposal_short_title': df[col] = 'N/A'
                elif col == 'proposal_proposing_party': df[col] = 'N/A'
                elif col == 'proposal_approval_status': df[col] = pd.NA
                elif col in ['authors_json_str', 'proposal_summary_analysis', 'proposal_summary_fiscal_impact', 'proposal_summary_colloquial']:
                    df[col] = '' if col != 'authors_json_str' else '[]'
                elif col == 'proposal_category_list': df[col] = df[col].apply(lambda x: [] if pd.isna(x) else x)
                else: df[col] = 'N/A' if col not in ['hyperlink', 'session_pdf_url'] else ''
        
        for col_fill_na in ['full_title', 'description', 'vote_outcome', 'issue_type', 'party']: df[col_fill_na] = df[col_fill_na].fillna('N/A')
        df['hyperlink'] = df['hyperlink'].fillna('')
        df['session_pdf_url'] = df['session_pdf_url'].fillna('')
        df['is_unanimous'] = df['is_unanimous'].fillna(False).astype(bool)
        df['authors_json_str'] = df['authors_json_str'].fillna('[]')
        df['proposal_category_list'] = df['proposal_category_list'].fillna('').apply(lambda x: [] if x == '' else x) # Ensure it's list
        df['proposal_short_title'] = df['proposal_short_title'].fillna('N/A') # Ensure new column handled
        df['proposal_proposing_party'] = df['proposal_proposing_party'].fillna('N/A') # Ensure new column handled
        df['proposal_approval_status'] = pd.to_numeric(df['proposal_approval_status'], errors='coerce') # Ensure new column handled

        for col_fill_empty_str in ['proposal_summary_analysis', 'proposal_summary_fiscal_impact', 'proposal_summary_colloquial']:
            df[col_fill_empty_str] = df[col_fill_empty_str].fillna('')
        for col_to_int in ['votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted']:
            df[col_to_int] = pd.to_numeric(df[col_to_int], errors='coerce').fillna(0).astype(int)
        df['issue_identifier'] = df['issue_identifier'].astype(str)
        return df
    except FileNotFoundError: st.error(f"Error: Data file '{os.path.abspath(final_csv_path)}' not found."); return pd.DataFrame()
    except pd.errors.EmptyDataError: st.error(f"Error: Data file '{final_csv_path}' is empty."); return pd.DataFrame()
    except Exception as e: st.error(f"Error loading data from '{final_csv_path}': {e}"); return pd.DataFrame()


data_df = load_data() 

# Category mapping for display
CATEGORY_MAPPING = {
    0: "Saúde e Cuidados Sociais",
    1: "Educação e Competências", 
    2: "Defesa e Segurança Nacional",
    3: "Justiça, Lei e Ordem",
    4: "Economia e Finanças",
    5: "Bem-Estar e Segurança Social",
    6: "Ambiente, Agricultura e Pescas",
    7: "Energia e Clima",
    8: "Transportes e Infraestruturas",
    9: "Habitação, Comunidades e Administração Local",
    10: "Negócios Estrangeiros e Cooperação Internacional",
    11: "Ciência, Tecnologia e Digital"
}

# --- Get Topic ID and Handle Back Navigation ---
issue_id_param = None
from_page = st.query_params.get("from_page", "home")

# Initialize session state
if 'last_page' not in st.session_state:
    st.session_state.last_page = from_page
if 'search_query' not in st.session_state: # Ensure search_query is initialized
    st.session_state.search_query = ""
if 'selected_categories' not in st.session_state:
    st.session_state.selected_categories = []
if 'selected_approval_label' not in st.session_state:
    st.session_state.selected_approval_label = "Todos"
if 'selected_proposing_party' not in st.session_state:
    st.session_state.selected_proposing_party = "Todos"
if 'selected_government' not in st.session_state:
    st.session_state.selected_government = "Todos"


# --- Back Button Logic ---
# Determine the target page and label for the back button
back_button_target_page_path = "streamlit_app.py" # Default to home
back_button_label = "⬅️ Voltar à Página Inicial"
query_params_for_back = {"from_page": "details"} # Basic param

if st.session_state.last_page == 'browse':
    back_button_target_page_path = "pages/1_Browse_Topics.py"
    back_button_label = "⬅️ Voltar a Todas as Votações"
    query_params_for_back.update({
        "categories": ",".join(st.session_state.get("selected_categories", [])),
        "approval": st.session_state.get("selected_approval_label", "Todos"),
        "proposing_party": st.session_state.get("selected_proposing_party", "Todos"),
        "government": st.session_state.get("selected_government", "Todos"),
        "from_page": "details" # Mark that we are coming back from details
    })
elif st.session_state.last_page == 'home':
    back_button_target_page_path = "streamlit_app.py"
    back_button_label = "⬅️ Voltar à Página Inicial"
    query_params_for_back.update({
        "search_query": st.session_state.get("search_query", ""),
        "from_page": "details" # Mark that we are coming back from details
    })

if st.button(back_button_label, key="back_button_topic_details"):
    st.query_params.clear() # Clear current issue_id params
    st.query_params.update(query_params_for_back)
    st.switch_page(back_button_target_page_path)


# Check if coming from a specific page and handle restoration
if from_page == "home":
    search_query = st.query_params.get("search_query", "")
    if search_query:
        st.session_state.search_query = search_query
elif from_page == "browse":
    # Restore filter states from query parameters
    categories_param = st.query_params.get("categories", "")
    if categories_param:
        st.session_state.selected_categories = [cat for cat in categories_param.split(",") if cat]
    approval_param = st.query_params.get("approval")
    if approval_param:
        st.session_state.selected_approval_label = approval_param
    proposing_party_param = st.query_params.get("proposing_party")
    if proposing_party_param:
        st.session_state.selected_proposing_party = proposing_party_param
    government_param = st.query_params.get("government")
    if government_param:
        st.session_state.selected_government = government_param

# --- Refined Get Topic ID Logic (from previous commit) ---
needs_url_update_for_session_id = False

# Try to get ID from query parameters first
query_param_id = st.query_params.get("issue_id")

if query_param_id:
    issue_id_param = str(query_param_id)
    # Sync session_state if URL is the source of truth and differs or if session state is not set
    if st.session_state.get("selected_issue_identifier") != issue_id_param:
        st.session_state.selected_issue_identifier = issue_id_param
else:
    # If not in query_params, try to get from session_state
    session_state_id = st.session_state.get("selected_issue_identifier")
    if session_state_id:
        issue_id_param = str(session_state_id)
        # Mark that the URL needs to be updated to reflect this ID
        needs_url_update_for_session_id = True

# If the ID came from session_state and URL needs updating, set query_params.
# This will trigger a rerun. The page will then load with issue_id in query_params.
if needs_url_update_for_session_id and issue_id_param:
    st.query_params.update({"issue_id": issue_id_param})
    # The script will rerun after this. For this current execution path,
    # issue_id_param is already set, so content can be displayed if not for the rerun.
    # Streamlit handles the rerun gracefully.

if issue_id_param and not data_df.empty:
    topic_details_df = data_df[data_df['issue_identifier'] == str(issue_id_param)]

    if not topic_details_df.empty:
        topic_info = topic_details_df.iloc[0]

        # Remove the navigation breadcrumb and back buttons - clean page design
        # The new back button is placed above this title
        st.title(f"🗳️ {topic_info['full_title']}")
        if pd.notna(topic_info['proposal_short_title']) and topic_info['proposal_short_title'] != 'N/A':
            st.subheader(f"{topic_info['proposal_short_title']}")
        st.markdown("---")

        # --- Prepare Data for Parliament Visualization (Moved Up) ---
        parliament_fig = None
        # Filter out N/A party if it exists and there are other parties for visualization.
        viz_parties_df = topic_details_df[topic_details_df['party'] != 'N/A']
        if viz_parties_df.empty and not topic_details_df.empty: # Only N/A party was found
            viz_parties_df = topic_details_df 
        
        if not viz_parties_df.empty:
            chart_data_for_viz = []
            for party_name_meta, meta_info in PARTY_METADATA.items():
                party_vote_info = viz_parties_df[viz_parties_df['party'] == party_name_meta]
                
                stance_for_viz = "neutral" 
                mps_for_viz = 0 # Initialize MPs for visualization to 0
                
                if not party_vote_info.empty:
                    party_row = party_vote_info.iloc[0]
                    favor = int(party_row.get('votes_favor', 0))
                    against = int(party_row.get('votes_against', 0))
                    abstention = int(party_row.get('votes_abstention', 0))
                    
                    total_explicit_votes = favor + against + abstention
                    mps_for_viz = total_explicit_votes # Use actual voting MPs for this proposal

                    if mps_for_viz > 0: # Determine stance only if there were votes
                        if favor > against and favor > abstention:
                            stance_for_viz = "favor"
                        elif against > favor and against > abstention:
                            stance_for_viz = "contra"
                        else: 
                            # Default to abstain if not clearly favor or contra (covers abstention-dominant or ties)
                            stance_for_viz = "abstain" 
                    # If mps_for_viz is 0, stance remains "neutral"
                # If party_vote_info was empty, mps_for_viz remains 0 and stance_for_viz remains "neutral"

                chart_data_for_viz.append({
                    "name": party_name_meta,
                    "mps": mps_for_viz, # Use dynamically calculated MPs for the visualization
                    "base_color": meta_info["color"],
                    "stance": stance_for_viz
                })
            
            ordered_chart_data = sorted(
                chart_data_for_viz,
                key=lambda x: ORDERED_PARTIES.index(x['name']) if x['name'] in ORDERED_PARTIES else float('inf')
            )
            ordered_chart_data = [p for p in ordered_chart_data if p['name'] in ORDERED_PARTIES]

            if ordered_chart_data:
                parliament_fig = generate_parliament_viz(ordered_chart_data)
        
        # --- Create Columns for Summary and Visualization ---
        col_summary, col_viz = st.columns([6, 4]) # Adjust ratio as needed, e.g., [3, 2] or [6,4]

        with col_summary:
            # --- Summary Section ---
            st.subheader("🏛️ Resumo da Proposta")
            with st.container(border=True): 
                # Show proposing party and short title prominently
                proposing_party_text = ""
                if pd.notna(topic_info.get('proposal_proposing_party')) and topic_info['proposal_proposing_party'] != 'N/A' and str(topic_info['proposal_proposing_party']).lower() != 'nan':
                    proposing_party_text = topic_info['proposal_proposing_party']
                
                # Add session date if available
                if pd.notna(topic_info.get('session_date')):
                    date_str = topic_info['session_date'].strftime("%Y-%m-%d")
                    if proposing_party_text:
                        st.markdown(f"**Proposta: {proposing_party_text} - {date_str}**")
                    else:
                        st.markdown(f"**Data: {date_str}**")
                else:
                    if proposing_party_text:
                        st.markdown(f"**Proposta: {proposing_party_text}**")
                
                # Show short title if available, otherwise use main title
                if pd.notna(topic_info.get('proposal_short_title')) and topic_info['proposal_short_title'] != 'N/A':
                    st.markdown(f"*{topic_info['proposal_short_title']}*")
                
                st.markdown("")  # Add some spacing

                parties_favor_summary = []
                parties_against_summary = []
                parties_abstention_summary = []

                for _, party_row in topic_details_df.iterrows():
                    if party_row['party'] == 'N/A': continue # Skip if no party data for this proposal
                    party_name = party_row['party']
                    favor_votes = int(party_row.get('votes_favor', 0))
                    against_votes = int(party_row.get('votes_against', 0))
                    abstention_votes = int(party_row.get('votes_abstention', 0))
                    
                    # Determine majority vote for the party
                    if favor_votes > against_votes and favor_votes > abstention_votes:
                        parties_favor_summary.append(party_name)
                    elif against_votes > favor_votes and against_votes > abstention_votes:
                        parties_against_summary.append(party_name)
                    elif abstention_votes > favor_votes and abstention_votes > against_votes: # Only count as abstention if it's the primary stance
                        parties_abstention_summary.append(party_name)
                    # If tied, or only non-voters, they won't appear in these lists.
                
                # Display voting results in clean format
                favor_text = ', '.join(sorted(list(set(parties_favor_summary)))) if parties_favor_summary else '-'
                st.markdown(f"**A Favor:** {favor_text}")
                
                contra_text = ', '.join(sorted(list(set(parties_against_summary)))) if parties_against_summary else '-'
                st.markdown(f"**Contra:** {contra_text}")

                abstention_text = ', '.join(sorted(list(set(parties_abstention_summary)))) if parties_abstention_summary else '-'
                st.markdown(f"**Abstenção:** {abstention_text}")
                
                st.markdown("")  # Add some spacing

                # Clean approval status display
                vote_outcome = topic_info['vote_outcome']
                if vote_outcome == "Aprovado":
                    st.markdown('<span style="font-size: 1.2em;">✅ **Aprovado**</span>', unsafe_allow_html=True)
                elif vote_outcome == "Rejeitado":
                    st.markdown('<span style="font-size: 1.2em;">❌ **Rejeitado**</span>', unsafe_allow_html=True)
                else:
                    st.markdown(f'<span style="font-size: 1.2em;">❓ **{vote_outcome}**</span>', unsafe_allow_html=True)
        
        with col_viz:
            # --- Parliament Visualization ---
            # st.subheader("🏛️ Votação no Parlamento")
            if parliament_fig:
                st.pyplot(parliament_fig)
            elif not viz_parties_df.empty: # If we had parties but still no fig (e.g. all neutral)
                st.markdown("Não foi possível gerar a visualização do parlamento (sem dados de votação para exibir ou todos os partidos neutros).")
            else: # No party data at all for viz_parties_df
                st.markdown("Não há dados de partidos para gerar a visualização do parlamento.")

        # --- Authors Section (Remains below the two columns) ---
        if pd.notna(topic_info['authors_json_str']) and topic_info['authors_json_str'] != '[]':
            try:
                authors_list = json.loads(topic_info['authors_json_str'])
                if authors_list: # Ensure it's not an empty list string like "[]" that becomes empty list
                    with st.expander("👥 **Autores/Proponentes da Iniciativa**", expanded=False):
                        for author in authors_list:
                            if isinstance(author, dict) and 'name' in author:
                                if 'link' in author and author['link']:
                                    st.markdown(f"- [{author['name']}]({author['link']})")
                                else:
                                    st.markdown(f"- {author['name']}")
                            elif isinstance(author, str): # Handle if authors_json is just a list of strings
                                st.markdown(f"- {author}")
            except json.JSONDecodeError:
                st.caption("Não foi possível carregar a lista de autores.")
        
        # --- Description and Summaries Section ---
        if pd.notna(topic_info['description']) and topic_info['description'].strip() and topic_info['description'] != 'Descrição não disponível.':
            with st.expander("📜 **Descrição Geral da Iniciativa**", expanded=True):
                st.markdown(topic_info['description'])
        
        if pd.notna(topic_info['proposal_summary_analysis']) and topic_info['proposal_summary_analysis'].strip():
            with st.expander("🔬 **Análise da Proposta**", expanded=False):
                st.markdown(topic_info['proposal_summary_analysis'])

        if pd.notna(topic_info['proposal_summary_fiscal_impact']) and topic_info['proposal_summary_fiscal_impact'].strip():
            with st.expander("💰 **Impacto Fiscal Estimado**", expanded=False):
                st.markdown(topic_info['proposal_summary_fiscal_impact'])

        if pd.notna(topic_info['proposal_summary_colloquial']) and topic_info['proposal_summary_colloquial'].strip():
            with st.expander("🗣️ **Sem precisar de dicionário**", expanded=False):
                st.markdown(topic_info['proposal_summary_colloquial'])
        
        if pd.notna(topic_info['hyperlink']) and topic_info['hyperlink'].strip():
            st.markdown(f"🔗 **Link para o documento/iniciativa:** [Aceder aqui]({topic_info['hyperlink']})", unsafe_allow_html=True)

        if pd.notna(topic_info['session_pdf_url']) and topic_info['session_pdf_url'].strip():
            st.markdown(f"📄 **Link para o PDF da sessão de votação:** [Aceder aqui]({topic_info['session_pdf_url']})", unsafe_allow_html=True)

        st.markdown("---")
        # --- Votação por Partido Político (Table - Remains below the two columns) ---
        st.subheader("📊 Votação por Partido Político")
        
        # Filter out N/A party if it exists and there are other parties.
        display_parties_df = topic_details_df[topic_details_df['party'] != 'N/A']
        if display_parties_df.empty and not topic_details_df.empty: # Only N/A party was found
            display_parties_df = topic_details_df 
        elif display_parties_df.empty and topic_details_df.empty: # No data at all
             st.markdown("Não há dados de votação por partido para exibir.")


        if not display_parties_df.empty:
            # Sort parties for consistent display (e.g., alphabetically)
            # No party_total_mps for sorting by size.
            sorted_parties_df = display_parties_df.sort_values(by='party') 
            
            # Option 1: Table display (concise)
            table_data_rows = []
            for _, party_vote_row in sorted_parties_df.iterrows():
                party_name = party_vote_row['party']
                if party_name == 'N/A' and len(sorted_parties_df) > 1: continue # Skip N/A if other parties exist

                favor = int(party_vote_row.get('votes_favor', 0))
                against = int(party_vote_row.get('votes_against', 0))
                abstention = int(party_vote_row.get('votes_abstention', 0))
                not_voted = int(party_vote_row.get('votes_not_voted', 0))

                # Determine party's primary stance for a simple tick
                main_stance = ""
                if favor > against and favor > abstention: main_stance = "A Favor ✅"
                elif against > favor and against > abstention: main_stance = "Contra ❌"
                elif abstention > favor and abstention > against: main_stance = "Abstenção 🤷"
                elif favor == 0 and against == 0 and abstention == 0 and not_voted > 0: main_stance = "Não Votou"
                elif favor == 0 and against == 0 and abstention == 0 and not_voted == 0: main_stance = "Sem registo"


                table_data_rows.append({
                    "Partido": party_name,
                    "Posição Principal": main_stance,
                    "A Favor": favor,
                    "Contra": against,
                    "Abstenção": abstention,
                    "Não Votaram": not_voted
                })
            
            if table_data_rows:
                party_votes_display_df = pd.DataFrame(table_data_rows)
                st.table(party_votes_display_df.set_index("Partido"))
            elif topic_details_df.iloc[0]['party'] == 'N/A':
                 st.markdown("Não há dados de votação por partido disponíveis para esta iniciativa.")
            else:
                 st.markdown("Processando dados de votação por partido...")

            # --- Parliament Visualization --- # This section is now moved to the top right column
            # st.subheader("🏛️ Visualização da Votação no Parlamento") # Original placement
            
            # chart_data_for_viz = [] # Logic moved up
            # ... (rest of the visualization data prep and display logic is now at the top) ...
            # --- End of Parliament Visualization ---

        else:
            st.markdown("Não há dados de votação por partido para exibir.")


    else:
        st.error(f"Não foram encontrados detalhes para a votação com o identificador: {issue_id_param}")
        # Simplified error message without navigation buttons

elif data_df.empty:
    st.warning("Não foi possível carregar os dados das votações. Verifique as mensagens de erro na consola ou na página principal.")
else:
    st.info("Selecione uma votação na página 'Todas as Votações' ou pesquise na página inicial para ver os detalhes.")

st.sidebar.page_link("streamlit_app.py", label="Página Inicial", icon="🏠")
st.sidebar.page_link("pages/1_Browse_Topics.py", label="Todas as Votações", icon="📜")