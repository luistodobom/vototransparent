import streamlit as st
import pandas as pd
import os
import json
import re

# --- Page Configuration ---
st.set_page_config(
    page_title="Detalhes da Votação - VotoTransparente PT",
    page_icon="🇵🇹",
    layout="wide"
)

# --- Data Loading ---
# [[REPLACE THE EXISTING load_data FUNCTION WITH THE ONE DEFINED ABOVE]]
# The load_data function is identical to the one in streamlit_app.py
# Path adjustment for pages is handled by the default path.
@st.cache_data  
def load_data(csv_path="data/parliament_data.csv"): # Adjusted default path for pages
    final_csv_path = csv_path
    # Path adjustment logic (same considerations as in 1_Browse_Topics.py)
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

            voting_breakdown_json = row.get('voting_details_json')
            current_proposal_overall_favor = 0
            current_proposal_overall_against = 0
            current_proposal_overall_abstention = 0
            proposal_party_votes_list = []
            parsed_voting_breakdown = {}
            valid_breakdown_found = False

            if pd.notna(voting_breakdown_json) and isinstance(voting_breakdown_json, str) and voting_breakdown_json.strip():
                try:
                    parsed_voting_breakdown = json.loads(voting_breakdown_json)
                    if isinstance(parsed_voting_breakdown, dict) and parsed_voting_breakdown:
                        valid_breakdown_found = True
                except json.JSONDecodeError:
                    parsed_voting_breakdown = {}

            if valid_breakdown_found:
                for party_name, party_votes_data in parsed_voting_breakdown.items():
                    if isinstance(party_votes_data, dict):
                        raw_favor_val = party_votes_data.get('Favor', party_votes_data.get('votes_favor', 0))
                        favor_numeric = pd.to_numeric(raw_favor_val, errors='coerce')
                        favor = 0 if pd.isna(favor_numeric) else int(favor_numeric)

                        raw_against_val = party_votes_data.get('Contra', party_votes_data.get('votes_against', 0))
                        against_numeric = pd.to_numeric(raw_against_val, errors='coerce')
                        against = 0 if pd.isna(against_numeric) else int(against_numeric)

                        raw_abstention_val = party_votes_data.get('Abstenção', party_votes_data.get('Abstencao', party_votes_data.get('votes_abstention', 0)))
                        abstention_numeric = pd.to_numeric(raw_abstention_val, errors='coerce')
                        abstention = 0 if pd.isna(abstention_numeric) else int(abstention_numeric)
                        
                        raw_not_voted_val = party_votes_data.get('Não Votaram', party_votes_data.get('Nao Votaram', 0))
                        not_voted_numeric = pd.to_numeric(raw_not_voted_val, errors='coerce')
                        not_voted = 0 if pd.isna(not_voted_numeric) else int(not_voted_numeric)

                        current_proposal_overall_favor += favor
                        current_proposal_overall_against += against
                        current_proposal_overall_abstention += abstention
                        proposal_party_votes_list.append({
                            'party': party_name, 'votes_favor': favor, 'votes_against': against,
                            'votes_abstention': abstention, 'votes_not_voted': not_voted,
                        })
            
            vote_outcome_str = "Dados de votação não disponíveis"
            is_unanimous_bool = False
            if proposal_party_votes_list:
                if current_proposal_overall_favor > 0 and current_proposal_overall_against == 0 and current_proposal_overall_abstention == 0:
                    vote_outcome_str = "Aprovado por unanimidade"; is_unanimous_bool = True
                elif current_proposal_overall_against > 0 and current_proposal_overall_favor == 0 and current_proposal_overall_abstention == 0:
                    vote_outcome_str = "Rejeitado por unanimidade"; is_unanimous_bool = True
                elif current_proposal_overall_favor > current_proposal_overall_against: vote_outcome_str = "Aprovado"
                elif current_proposal_overall_against > current_proposal_overall_favor: vote_outcome_str = "Rejeitado"
                elif current_proposal_overall_favor == current_proposal_overall_against and current_proposal_overall_favor > 0: vote_outcome_str = "Empate"
                elif current_proposal_overall_abstention > 0 and current_proposal_overall_favor == 0 and current_proposal_overall_against == 0:
                    all_abstained = all(p_vote['votes_favor'] == 0 and p_vote['votes_against'] == 0 for p_vote in proposal_party_votes_list)
                    if all_abstained: vote_outcome_str = "Abstenção Geral"; is_unanimous_bool = True # Unanimous abstention
                    else: vote_outcome_str = "Resultado misto" # Should not happen
                else:
                    if current_proposal_overall_favor == 0 and current_proposal_overall_against == 0 and current_proposal_overall_abstention == 0:
                        total_non_voters = sum(pvd.get('votes_not_voted',0) for pvd in proposal_party_votes_list)
                        if total_non_voters > 0 and not any(pvd.get('votes_favor',0) > 0 or pvd.get('votes_against',0) > 0 or pvd.get('votes_abstention',0) > 0 for pvd in proposal_party_votes_list):
                             vote_outcome_str = "Ausência de votação registada"
                        else:
                             vote_outcome_str = "Sem votos expressos (Favor, Contra, Abstenção)"
                    else: vote_outcome_str = "Resultado misto"
            elif not valid_breakdown_found and pd.notna(voting_breakdown_json) and voting_breakdown_json.strip():
                 vote_outcome_str = "Dados de votação malformados"

            if proposal_party_votes_list:
                for p_vote in proposal_party_votes_list:
                    all_vote_details.append({
                        'issue_identifier': issue_id_str, 'full_title': title, 'description': description_text,
                        'hyperlink': hyperlink_url, 'vote_outcome': vote_outcome_str, 'is_unanimous': is_unanimous_bool,
                        'issue_type': issue_type, 'party': p_vote['party'],
                        'votes_favor': p_vote['votes_favor'], 'votes_against': p_vote['votes_against'],
                        'votes_abstention': p_vote['votes_abstention'], 'votes_not_voted': p_vote['votes_not_voted'],
                        'authors_json_str': authors_json_str,
                        'proposal_summary_analysis': summary_analysis,
                        'proposal_summary_fiscal_impact': summary_fiscal,
                        'proposal_summary_colloquial': summary_colloquial,
                        'session_pdf_url': session_pdf_url_val,
                        'proposal_category_list': proposal_category_list,
                    })
            else:
                all_vote_details.append({
                    'issue_identifier': issue_id_str, 'full_title': title, 'description': description_text,
                    'hyperlink': hyperlink_url, 'vote_outcome': vote_outcome_str, 'is_unanimous': is_unanimous_bool,
                    'issue_type': issue_type, 'party': 'N/A',
                    'votes_favor': 0, 'votes_against': 0, 'votes_abstention': 0, 'votes_not_voted': 0,
                    'authors_json_str': authors_json_str,
                    'proposal_summary_analysis': summary_analysis,
                    'proposal_summary_fiscal_impact': summary_fiscal,
                    'proposal_summary_colloquial': summary_colloquial,
                    'session_pdf_url': session_pdf_url_val,
                    'proposal_category_list': proposal_category_list,
                })
        
        if not all_vote_details: st.info("No vote data could be processed."); return pd.DataFrame()
        df = pd.DataFrame(all_vote_details)
        expected_cols = [
            'issue_identifier', 'full_title', 'description', 'hyperlink', 'vote_outcome', 'is_unanimous', 
            'issue_type', 'party', 'votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted',
            'authors_json_str', 'proposal_summary_analysis', 'proposal_summary_fiscal_impact', 'proposal_summary_colloquial',
            'session_pdf_url', 'proposal_category_list'
        ]
        for col in expected_cols:
            if col not in df.columns:
                if col in ['votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted']: df[col] = 0
                elif col == 'is_unanimous': df[col] = False
                elif col in ['authors_json_str', 'proposal_summary_analysis', 'proposal_summary_fiscal_impact', 'proposal_summary_colloquial']:
                    df[col] = '' if col != 'authors_json_str' else '[]'
                elif col == 'proposal_category_list': df[col] = df[col].apply(lambda x: [] if pd.isna(x) else x)
                else: df[col] = 'N/A' if col not in ['hyperlink', 'session_pdf_url'] else ''
        
        for col_fill_na in ['full_title', 'description', 'vote_outcome', 'issue_type', 'party']: df[col_fill_na] = df[col_fill_na].fillna('N/A')
        df['hyperlink'] = df['hyperlink'].fillna('')
        df['session_pdf_url'] = df['session_pdf_url'].fillna('')
        df['is_unanimous'] = df['is_unanimous'].fillna(False).astype(bool)
        df['authors_json_str'] = df['authors_json_str'].fillna('[]')
        df['proposal_category_list'] = df['proposal_category_list'].fillna('').apply(lambda x: [] if x == '' else x)
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

# --- Get Topic ID from Session State ---
issue_id_param = st.session_state.get("selected_issue_identifier")

if issue_id_param and not data_df.empty:
    # 'issue_identifier' is already string from load_data
    topic_details_df = data_df[data_df['issue_identifier'] == str(issue_id_param)]

    if not topic_details_df.empty:
        # General Info (from the first row, should be consistent per issue_identifier)
        topic_info = topic_details_df.iloc[0]

        st.title(f"🗳️ {topic_info['full_title']}")
        st.markdown("---")

        # --- Summary Section ---
        with st.container(border=True): 
            st.subheader(f"Resultado Geral: {topic_info['vote_outcome'].upper()}")

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
            
            if parties_favor_summary:
                st.markdown(f"**A FAVOR (maioria do partido):** {', '.join(sorted(list(set(parties_favor_summary))))}")
            else:
                st.markdown("**A FAVOR (maioria do partido):** -")
            
            if parties_against_summary:
                st.markdown(f"**CONTRA (maioria do partido):** {', '.join(sorted(list(set(parties_against_summary))))}")
            else:
                st.markdown("**CONTRA (maioria do partido):** -")

            if parties_abstention_summary:
                st.markdown(f"**ABSTENÇÃO (maioria do partido):** {', '.join(sorted(list(set(parties_abstention_summary))))}")
            else:
                st.markdown("**ABSTENÇÃO (maioria do partido):** -")
            
            st.markdown(" ") 

            if pd.notna(topic_info['issue_type']):
                st.markdown(f"**Tipo de Iniciativa:** {topic_info['issue_type']}")
            if pd.notna(topic_info['issue_identifier']):
                st.markdown(f"**Identificador:** {topic_info['issue_identifier']}")
            
            # Display categories
            if 'proposal_category_list' in topic_info and topic_info['proposal_category_list']:
                category_names = [
                    CATEGORY_MAPPING.get(cat_id, f"Categoria {cat_id}") 
                    for cat_id in topic_info['proposal_category_list']
                ]
                if category_names:
                    st.markdown(f"**Categorias:** {', '.join(category_names)}")
            
            if 'is_unanimous' in topic_info and pd.notna(topic_info['is_unanimous']):
                 st.markdown(f"**Votação Unânime (geral):** {'Sim ✅' if topic_info['is_unanimous'] else 'Não ❌'}")
        
        # --- Authors Section ---
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
            with st.expander("🗣️ **Em Português Claro (Resumo Coloquial)**", expanded=False):
                st.markdown(topic_info['proposal_summary_colloquial'])
        
        if pd.notna(topic_info['hyperlink']) and topic_info['hyperlink'].strip():
            st.markdown(f"🔗 **Link para o documento/iniciativa:** [Aceder aqui]({topic_info['hyperlink']})", unsafe_allow_html=True)

        if pd.notna(topic_info['session_pdf_url']) and topic_info['session_pdf_url'].strip():
            st.markdown(f"📄 **Link para o PDF da sessão de votação:** [Aceder aqui]({topic_info['session_pdf_url']})", unsafe_allow_html=True)

        st.markdown("---")
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

        else:
            st.markdown("Não há dados de votação por partido para exibir.")


    else:
        st.error(f"Não foram encontrados detalhes para a votação com o identificador: {issue_id_param}")
        st.page_link("pages/1_Browse_Topics.py", label="Voltar à lista de votações", icon="⬅️")

elif data_df.empty:
    st.warning("Não foi possível carregar os dados das votações. Verifique as mensagens de erro na consola ou na página principal.")
else:
    st.info("Selecione uma votação na página 'Todas as Votações' ou pesquise na página inicial para ver os detalhes.")
    st.page_link("streamlit_app.py", label="Ir para a Página Inicial", icon="🏠")
    st.page_link("pages/1_Browse_Topics.py", label="Navegar por Todas as Votações", icon="📜")

st.sidebar.page_link("streamlit_app.py", label="Página Inicial", icon="🏠")
st.sidebar.page_link("pages/1_Browse_Topics.py", label="Todas as Votações", icon="📜")