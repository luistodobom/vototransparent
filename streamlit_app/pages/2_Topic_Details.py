import streamlit as st
import pandas as pd
import os
import json # Added for JSON processing

# --- Page Configuration ---
st.set_page_config(
    page_title="Detalhes da Votação - VotoTransparente PT",
    page_icon="🇵🇹",
    layout="wide"
)

# --- Data Loading ---
@st.cache_data
def load_data(json_path="portuguese_parliament_votes_json.json"): # Adjusted path for pages folder
    # Corrected path logic for files inside 'pages' directory, assuming json is at project root
    final_json_path = json_path 
    if not os.path.isabs(json_path) and os.path.basename(os.getcwd()) == "pages":
        final_json_path = os.path.join("..", json_path)
    elif not os.path.exists(json_path) and os.path.exists(os.path.join("..", json_path)) and "pages" in os.getcwd(): # Fallback for pages
        final_json_path = os.path.join("..", json_path)

    if not os.path.exists(final_json_path):
        st.error(f"Error: The data file '{os.path.abspath(final_json_path)}' was not found. "
                 f"Please ensure it's generated (e.g., by an extraction script).")
        return pd.DataFrame()
    try:
        with open(final_json_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        if 'votes' not in raw_data or 'party_composition' not in raw_data:
            st.error("Error: JSON data is missing essential 'votes' or 'party_composition' keys.")
            return pd.DataFrame()

        party_composition_data = raw_data.get('party_composition', {})
        all_vote_details = []

        for vote_event in raw_data.get('votes', []):
            if not isinstance(vote_event, dict) or not vote_event.get('id'):
                continue

            issue_id = vote_event.get('id')
            title = vote_event.get('title', 'Título não disponível.')
            vote_type = vote_event.get('type', 'N/A')
            url = vote_event.get('url', '')
            outcome = vote_event.get('result', 'N/A')
            is_unanimous_vote = "unanimidade" in outcome.lower()
            description_text = 'Descrição não disponível.'


            voting_breakdown = vote_event.get('voting_breakdown')

            if isinstance(voting_breakdown, dict) and voting_breakdown:
                for party_name, party_votes_data in voting_breakdown.items():
                    favor, against, abstention = 0, 0, 0
                    if isinstance(party_votes_data, dict):
                        favor = party_votes_data.get('favor', party_votes_data.get('votes_favor', 0))
                        against = party_votes_data.get('against', party_votes_data.get('votes_against', 0))
                        abstention = party_votes_data.get('abstention', party_votes_data.get('votes_abstention', 0))
                    
                    row = {
                        'issue_identifier': issue_id, 'full_title': title, 'description': description_text,
                        'hyperlink': url, 'vote_outcome': outcome, 'is_unanimous': is_unanimous_vote,
                        'issue_type': vote_type, 'party': party_name,
                        'party_total_mps': party_composition_data.get(party_name, 0),
                        'votes_favor': favor, 'votes_against': against, 'votes_abstention': abstention
                    }
                    all_vote_details.append(row)
            else:
                if not party_composition_data:
                     all_vote_details.append({
                        'issue_identifier': issue_id, 'full_title': title, 'description': description_text,
                        'hyperlink': url, 'vote_outcome': outcome, 'is_unanimous': is_unanimous_vote,
                        'issue_type': vote_type, 'party': 'N/A', 'party_total_mps': 0,
                        'votes_favor': 0, 'votes_against': 0, 'votes_abstention': 0
                    })
                else:
                    for party_name in party_composition_data.keys():
                        all_vote_details.append({
                            'issue_identifier': issue_id, 'full_title': title, 'description': description_text,
                            'hyperlink': url, 'vote_outcome': outcome, 'is_unanimous': is_unanimous_vote,
                            'issue_type': vote_type, 'party': party_name,
                            'party_total_mps': party_composition_data.get(party_name, 0),
                            'votes_favor': 0, 'votes_against': 0, 'votes_abstention': 0
                        })
        
        if not all_vote_details:
            st.info("No vote data could be processed from the JSON file.")
            return pd.DataFrame()

        df = pd.DataFrame(all_vote_details)
        expected_cols = [
            'issue_identifier', 'full_title', 'description', 'hyperlink',
            'vote_outcome', 'is_unanimous', 'issue_type', 'party', 'party_total_mps',
            'votes_favor', 'votes_against', 'votes_abstention'
        ]
        for col in expected_cols:
            if col not in df.columns:
                if col in ['votes_favor', 'votes_against', 'votes_abstention', 'party_total_mps']: df[col] = 0
                elif col == 'is_unanimous': df[col] = False
                else: df[col] = 'N/A' if col != 'hyperlink' else ''

        df['full_title'] = df['full_title'].fillna('Título não disponível.')
        df['description'] = df['description'].fillna('Descrição não disponível.')
        df['hyperlink'] = df['hyperlink'].fillna('')
        for col in ['party_total_mps', 'votes_favor', 'votes_against', 'votes_abstention']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int) # Corrected errors='fillna' to errors='coerce'
        return df

    except FileNotFoundError:
        st.error(f"Error: The data file '{os.path.abspath(final_json_path)}' was not found.")
        return pd.DataFrame()
    except json.JSONDecodeError:
        st.error(f"Error: Could not decode JSON from '{final_json_path}'. File might be corrupted.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred while loading data from '{final_json_path}': {e}")
        return pd.DataFrame()

data_df = load_data() # Uses default "portuguese_parliament_votes_json.json"

# --- Get Topic ID from Session State ---
issue_id_param = st.session_state.get("selected_issue_identifier")

if issue_id_param and not data_df.empty:
    # Convert to string for comparison
    data_df['issue_identifier_str'] = data_df['issue_identifier'].astype(str)
    topic_details_df = data_df[data_df['issue_identifier_str'] == str(issue_id_param)]

    if not topic_details_df.empty:
        # General Info (from the first row, should be consistent per issue_identifier)
        topic_info = topic_details_df.iloc[0]

        st.title(f"🗳️ {topic_info['full_title']}")
        st.markdown("---")

        # --- New Summary Section ---
        with st.container(border=True): # Visually group the summary
            st.subheader(f"Resultado Geral: {topic_info['vote_outcome'].upper()}")

            parties_favor_summary = []
            parties_against_summary = []
            parties_abstention_summary = []

            # Determine party stances for summary from topic_details_df
            for _, party_row in topic_details_df.iterrows():
                party_name = party_row['party']
                favor_votes = int(party_row.get('votes_favor', 0))
                against_votes = int(party_row.get('votes_against', 0))
                abstention_votes = int(party_row.get('votes_abstention', 0))

                if favor_votes > 0 and favor_votes > against_votes and favor_votes > abstention_votes:
                    parties_favor_summary.append(party_name)
                elif against_votes > 0 and against_votes > favor_votes and against_votes > abstention_votes:
                    parties_against_summary.append(party_name)
                elif abstention_votes > 0 and abstention_votes > favor_votes and abstention_votes > against_votes:
                    parties_abstention_summary.append(party_name)
            
            if parties_favor_summary:
                st.markdown(f"**A FAVOR:** {', '.join(sorted(parties_favor_summary))}")
            else:
                st.markdown("**A FAVOR:** -")
            
            if parties_against_summary:
                st.markdown(f"**CONTRA:** {', '.join(sorted(parties_against_summary))}")
            else:
                st.markdown("**CONTRA:** -")

            if parties_abstention_summary:
                st.markdown(f"**ABSTENÇÃO:** {', '.join(sorted(parties_abstention_summary))}")
            else:
                st.markdown("**ABSTENÇÃO:** -")
            
            st.markdown(" ") # Add a little space

            if pd.notna(topic_info['issue_type']):
                st.markdown(f"**Tipo de Iniciativa:** {topic_info['issue_type']}")
            if pd.notna(topic_info['issue_identifier']):
                st.markdown(f"**Identificador:** {topic_info['issue_identifier']}")
            if 'is_unanimous' in topic_info and pd.notna(topic_info['is_unanimous']):
                 st.markdown(f"**Votação Unânime:** {'Sim ✅' if topic_info['is_unanimous'] else 'Não ❌'}")
        # --- End of New Summary Section ---
        
        # The original col1, col2 section is now integrated into the summary above.

        if pd.notna(topic_info['description']) and topic_info['description'].strip():
            with st.expander("📜 **Descrição da Iniciativa**", expanded=True):
                st.markdown(topic_info['description'])
        
        if pd.notna(topic_info['hyperlink']) and topic_info['hyperlink'].strip():
            st.markdown(f"🔗 **Link para o documento oficial:** [Aceder aqui]({topic_info['hyperlink']})", unsafe_allow_html=True)

        st.markdown("---")
        st.subheader("📊 Votação por Partido Político")

        # Display votes per party in a table
        # Sort parties for consistent display, e.g., by name or by number of MPs
        sorted_parties_df = topic_details_df.sort_values(by='party_total_mps', ascending=False)
        
        table_data_rows = []
        for _, party_vote_row in sorted_parties_df.iterrows():
            party_name = party_vote_row['party']
            favor = int(party_vote_row.get('votes_favor', 0))
            against = int(party_vote_row.get('votes_against', 0))
            abstention = int(party_vote_row.get('votes_abstention', 0))

            favor_tick = "✅" if favor > 0 and favor > against and favor > abstention else ""
            against_tick = "✅" if against > 0 and against > favor and against > abstention else ""
            abstention_tick = "✅" if abstention > 0 and abstention > favor and abstention > against else ""
            
            table_data_rows.append({
                "Partido": party_name,
                "A Favor": favor_tick,
                "Contra": against_tick,
                "Abstenção": abstention_tick
            })

        if table_data_rows:
            party_votes_df = pd.DataFrame(table_data_rows)
            st.table(party_votes_df.set_index("Partido"))
        else:
            st.markdown("Não há dados de votação por partido para exibir nesta tabela.")

        # Remove old party voting display method
        # for _, party_vote_row in sorted_parties_df.iterrows():
        #     party_name = party_vote_row['party']
        #     total_mps = int(party_vote_row.get('party_total_mps', 0))
        #     favor = int(party_vote_row.get('votes_favor', 0))
        #     against = int(party_vote_row.get('votes_against', 0))
        #     abstention = int(party_vote_row.get('votes_abstention', 0))
            
        #     voted_mps = favor + against + abstention
        #     non_voters = total_mps - voted_mps if total_mps > 0 else 0

        #     with st.container(border=True):
        #         st.markdown(f"#### {party_name} ({total_mps} Deputados)")
                
        #         cols = st.columns(3 if non_voters <= 0 else 4)

        #         with cols[0]:
        #             st.metric(label="👍 A Favor", value=favor)
        #         with cols[1]:
        #             st.metric(label="👎 Contra", value=against)
        #         with cols[2]:
        #             st.metric(label="🤷 Abstenções", value=abstention)
        #         if non_voters > 0:
        #              with cols[3]:
        #                 st.metric(label="👤 Ausentes/Não Votaram", value=non_voters, help="Deputados ausentes ou que não exerceram o seu direito de voto.")

    else:
        st.error(f"Não foram encontrados detalhes para a votação com o identificador: {issue_id_param}")
        st.page_link("pages/1_Browse_Topics.py", label="Voltar à lista de votações", icon="⬅️")

elif data_df.empty:
    st.warning("Não foi possível carregar os dados das votações. Verifique as mensagens de erro na consola ou na página principal.")
else:
    st.info("Selecione uma votação na página 'Browse Topics' ou pesquise na página inicial para ver os detalhes.")
    st.page_link("streamlit_app.py", label="Ir para a Página Inicial", icon="🏠")
    st.page_link("pages/1_Browse_Topics.py", label="Navegar por Todas as Votações", icon="📜")

st.sidebar.page_link("streamlit_app.py", label="Página Inicial", icon="🏠")
st.sidebar.page_link("pages/1_Browse_Topics.py", label="Todas as Votações", icon="📜")