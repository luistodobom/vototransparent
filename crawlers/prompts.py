import os
import json
import time
from datetime import date
from google import genai


from config import *


genai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None


def create_prompt_for_session_pdf(hyperlink_table_pairs, unpaired_links, session_date):
    session_date = date.fromisoformat(session_date)
    response_schema = build_response_schema()
    pre_2020 = session_date is None or session_date < date(2020, 1, 1)
    structured_data_text = format_structured_data_for_llm(
        hyperlink_table_pairs, unpaired_links, pre_2020)
    mp_counts_text = build_mp_counts_text(session_date)

    if session_date is None:
        return create_prompt_for_session_pdf_post_2020(structured_data_text, mp_counts_text), response_schema
    else:
        return create_prompt_for_session_pdf_pre_2020(structured_data_text, mp_counts_text), response_schema


def create_prompt_for_session_pdf_pre_2020(structured_data_text, mp_counts_text):
    """
    Creates a prompt for the LLM to extract proposal voting data from pre-2020 session PDFs.
    These PDFs list parties that voted For/Against/Abstained, without detailed tables.
    MP counts are provided based on the session_date.
    """

    prompt = f"""Você está analisando um registro de votações parlamentares portuguesas de um período anterior a 2020. Os dados de votação neste formato não usam tabelas, mas sim listas textuais de partidos que votaram a favor, contra ou se abstiveram.

Os dados estruturados fornecidos (`structured_data_text`) contêm excertos de texto do PDF, cada um descrevendo uma ou mais propostas e como os partidos votaram. Um exemplo de como uma proposta pode ser descrita no texto:
"Projeto de Resolução n.º 958/XIII/2.ª (PCP) – Pela reabertura do Serviço de Urgência Básica no Hospital de Espinho; Favor – BE, PCP, PEV e PAN; Contra – Aprovado; Abstenção – PSD, PS e CDS-PP"
Note que "Contra – Aprovado" significa que a proposta foi aprovada, e a lista de partidos que votaram contra pode não estar explícita ou ser inferida.

{structured_data_text}

Com base nestes dados estruturados, crie um array JSON onde cada elemento representa UMA proposta que foi votada.

Para cada proposta, extraia:

    1. 'proposal_name': O identificador da proposta a partir do texto do hiperlink (por exemplo, "Projeto de Lei 404/XVI/1", "Proposta de Lei 39/XVI/1"). Isso vem do 'TEXTO' do hiperlink (para propostas agrupadas) ou 'TEXTO DA PROPOSTA' (para propostas não pareadas). O Identificador NUNCA será "Texto Final" ou similar, apesar do hyperlink poder ter esse texto.
    2. 'proposal_link': O URI/hiperlink para esta proposta. Isso vem do 'URI' do hiperlink.
    3.  'voting_summary': O detalhamento da votação por partido.
        -   Analise as menções "Favor –", "Contra –", "Abstenção –" para identificar os partidos.
        -   Use o formato: {{"NomeDoPartido": {{"Favor": X, "Contra": Y, "Abstenção": Z, "Não Votaram": W, "TotalDeputados": Total}}}}
        -   Utilize a contagem de deputados fornecida abaixo para o período da sessão para determinar X, Y, Z, W e Total.
        -   Se um partido está listado em "Favor", todos os seus deputados são contados como "Favor". O mesmo para "Contra" e "Abstenção".
        -   Se um partido não é mencionado em nenhuma lista de votação para uma proposta, ele não deve ser incluído no 'voting_summary' dessa proposta.
    4. 'proposal_approval_status': Um inteiro, 1 se a proposta foi aprovada, 0 se foi rejeitada. Se não estiver claro, defina como nulo. Isso é derivado do 'voting_summary'.

{mp_counts_text}

    Notas importantes:
    - Alguns dos hiperlinks podem não ser propostas, mas sim guias suplementares ou outros documentos. Normalmente, o primeiro hiperlink que aparece em um determinado parágrafo é a proposta principal, e pode não estar sempre vinculado ao identificador da proposta, às vezes o texto do hiperlink é apenas um genérico "Texto Final". Filtre itens não-proposta se identificáveis.
    - Algumas propostas podem ser aprovadas "por unanimidade" - estas ainda devem ser incluídas com o resumo da votação indicando aprovação unânime e proposal_approval_status como 1.
    - Múltiplas propostas podem compartilhar o mesmo resultado de votação se foram votadas juntas. **Conforme instruído acima, crie um objeto JSON separado para cada proposta nestes casos.**
    - Sempre forneça contagens numéricas no resumo da votação, não apenas marcas 'X'.

    Retorne apenas um array JSON válido. Cada objeto no array corresponde a um hiperlink/proposta.
    Se você não conseguir determinar as informações de votação para uma proposta, ainda a inclua com seu 'proposal_name' e 'proposal_link', mas defina 'voting_summary' como nulo e 'proposal_approval_status' como nulo.

Formato de exemplo de um objeto no array JSON (assumindo dados da XIII Legislatura para o exemplo de contagem):
    [
    {{ // Do grupo, primeiro hiperlink
        "proposal_name": "Projeto de Lei 123/XV/2",
        "proposal_link": "https://www.parlamento.pt/ActividadeParlamentar/Paginas/DetalheIniciativa.aspx?BID=XXXXX",
        "voting_summary": {{ 
        "PS": {{"Favor": 100, "Contra": 0, "Abstenção": 5, "Não Votaram": 2, "TotalDeputados": 107}},
        "PSD": {{"Favor": 0, "Contra": 65, "Abstenção": 0, "Não Votaram": 1, "TotalDeputados": 66}}
        }},
        "proposal_approval_status": 1
    }},
    {{ // Do mesmo grupo, segundo hiperlink (assumindo que é outra proposta válida relacionada à mesma conclusão)
        "proposal_name": "Alteração ao Projeto de Lei 123/XV/2",
        "proposal_link": "https://www.parlamento.pt/ActividadeParlamentar/Paginas/DetalheIniciativa.aspx?BID=YYYYY",
        "voting_summary": {{
        "PS": {{"Favor": 100, "Contra": 0, "Abstenção": 5, "Não Votaram": 2, "TotalDeputados": 107}},
        "PSD": {{"Favor": 0, "Contra": 65, "Abstenção": 0, "Não Votaram": 1, "TotalDeputados": 66}}
        }},
        "proposal_approval_status": 1
    }},
    {{ // Uma proposta não pareada
        "proposal_name": "Voto de Pesar XYZ",
        "proposal_link": "https://www.parlamento.pt/ActividadeParlamentar/Paginas/DetalheIniciativa.aspx?BID=ZZZZZ",
        "voting_summary": null, // Ou inferido se unânime, por exemplo, {{"PS": {{"Favor": 100, ...}}}}
        "proposal_approval_status": null // Ou inferido, por exemplo, 1 se aprovação unânime
    }}
    ]
"""
    return prompt


def create_prompt_for_session_pdf_post_2020(structured_data_text, mp_counts_text):

    prompt = f"""Você está analisando um registro de votações parlamentares portuguesas. Eu já extraí dados estruturados de propostas do PDF. Estes dados consistem em:
    1. Grupos de propostas: Cada grupo contém um ou mais hiperlinks (propostas) que *aparentam estar* associados a uma única tabela de votação encontrada após eles na mesma página. **A lista de hiperlinks fornecida para cada "grupo" é uma extração de melhor esforço de links encontrados textualmente acima de uma tabela. É possível que nem todos os hiperlinks listados sejam relevantes para essa tabela específica, e alguns podem não estar relacionados ou ser de contextos diferentes. Sua tarefa inclui discernir as propostas reais relacionadas à tabela a partir desta lista.**
    2. Propostas não pareadas: Estes são hiperlinks que não tinham uma tabela imediatamente a seguir.

    {structured_data_text}

    Com base nestes dados estruturados, crie um array JSON onde cada elemento representa UMA proposta (hiperlink) que foi votada.
    **A associação de hiperlinks a tabelas é uma tentativa baseada na proximidade no documento. Nem todos os hiperlinks listados acima de uma tabela pertencem necessariamente a essa votação; alguns podem ser de outros contextos. O modelo deve analisar criticamente para determinar a relevância.**

    - **Para "GRUPOS" de hiperlinks que parecem compartilhar uma única tabela (indicado como "TABELA DE VOTAÇÃO COMPARTILHADA POR ESTE GRUPO"):**
        - **Analise cuidadosamente cada hiperlink no grupo. É possível que múltiplos hiperlinks sejam propostas válidas que foram votadas em bloco, usando a mesma tabela de resultados.**
        - **Se este for o caso, você DEVE criar um objeto JSON separado para CADA UMA dessas propostas (hiperlinks) válidas. Cada um desses objetos JSON deve conter os detalhes da votação da tabela compartilhada.** Não agrupe várias propostas em um único objeto JSON nem ignore propostas válidas dentro do grupo. Filtre quaisquer hiperlinks que claramente não sejam propostas votadas (ex: links para páginas genéricas, documentos suplementares não votados).
    - Para propostas não pareadas (listadas sob "PROPOSTAS SEM TABELAS DE VOTAÇÃO INDIVIDUAIS"), tente inferir os detalhes da votação conforme descrito abaixo.

    Para cada proposta (hiperlink), extraia:

    1. 'proposal_name': O identificador da proposta a partir do texto do hiperlink (por exemplo, "Projeto de Lei 404/XVI/1", "Proposta de Lei 39/XVI/1"). Isso vem do 'TEXTO' do hiperlink (para propostas agrupadas) ou 'TEXTO DA PROPOSTA' (para propostas não pareadas). O Identificador NUNCA será "Texto Final" ou similar, apesar do hyperlink poder ter esse texto.
    2. 'proposal_link': O URI/hiperlink para esta proposta. Isso vem do 'URI' do hiperlink.
    3. 'voting_summary': O detalhamento da votação por partido.
        - Para propostas em um grupo com uma tabela compartilhada: Analise a tabela COMPARTILHADA para extrair as contagens de votos para cada partido (PS, PSD, CH, IL, PCP, BE, PAN, L, etc.)
        - Para propostas não pareadas: Se a proposta aparecer na seção "PROPOSTAS SEM TABELAS DE VOTAÇÃO INDIVIDUAIS", verifique se há algum indicador de texto no documento original (não fornecido aqui, então infira se possível a partir do contexto ou padrões comuns como aprovação unânime para certos tipos de propostas) sugerindo aprovação unânime ou votação em grupo. Se não houver informação, defina como nulo.
    4. 'proposal_approval_status': Um inteiro, 1 se a proposta foi aprovada, 0 se foi rejeitada. Se não estiver claro, defina como nulo. Isso é derivado do 'voting_summary'.


{mp_counts_text}


    Para o formato de voting_summary:
    - Se houver uma tabela de votação: Analise a tabela para extrair as contagens de votos para cada partido.
    - Use o formato: {{"NomeDoPartido": {{"Favor": X, "Contra": Y, "Abstenção": Z, "Não Votaram": W, "TotalDeputados": Total}}}}
    - Se a tabela usar marcas 'X': A marca 'X' indica que todos os MPs daquele partido votaram daquela maneira. Use o número total mostrado para aquele partido, se disponível, caso contrário, infira com base nos tamanhos típicos dos partidos, se necessário (menos ideal).
    - Se não houver tabela individual, mas for provavelmente unânime: Indique a votação unânime com as distribuições de partido apropriadas, se puder inferi-las, ou marque como unânime.

    Notas importantes:
    - Alguns dos hiperlinks podem não ser propostas, mas sim guias suplementares ou outros documentos. Normalmente, o primeiro hiperlink que aparece em um determinado parágrafo é a proposta principal, e pode não estar sempre vinculado ao identificador da proposta, às vezes o texto do hiperlink é apenas um genérico "Texto Final". Filtre itens não-proposta se identificáveis.
    - Algumas propostas podem ser aprovadas "por unanimidade" - estas ainda devem ser incluídas com o resumo da votação indicando aprovação unânime e proposal_approval_status como 1.
    - Múltiplas propostas podem compartilhar o mesmo resultado de votação se foram votadas juntas. **Conforme instruído acima, crie um objeto JSON separado para cada proposta nestes casos.**
    - Sempre forneça contagens numéricas no resumo da votação, não apenas marcas 'X'.

    Retorne apenas um array JSON válido. Cada objeto no array corresponde a um hiperlink/proposta.
    Se você não conseguir determinar as informações de votação para uma proposta, ainda a inclua com seu 'proposal_name' e 'proposal_link', mas defina 'voting_summary' como nulo e 'proposal_approval_status' como nulo.

    Formato de exemplo (ilustrando um grupo de duas propostas compartilhando uma tabela, e uma proposta não pareada):
    [
    {{ // Do grupo, primeiro hiperlink (assumindo que é uma proposta válida relacionada à tabela)
        "proposal_name": "Projeto de Lei 123/XV/2",
        "proposal_link": "https://www.parlamento.pt/ActividadeParlamentar/Paginas/DetalheIniciativa.aspx?BID=XXXXX",
        "voting_summary": {{ // Derivado da tabela compartilhada
        "PS": {{"Favor": 100, "Contra": 0, "Abstenção": 5, "Não Votaram": 2, "TotalDeputados": 107}},
        "PSD": {{"Favor": 0, "Contra": 65, "Abstenção": 0, "Não Votaram": 1, "TotalDeputados": 66}}
        }},
        "proposal_approval_status": 1
    }},
    {{ // Do mesmo grupo, segundo hiperlink (assumindo que é outra proposta válida relacionada à mesma tabela)
        "proposal_name": "Alteração ao Projeto de Lei 123/XV/2",
        "proposal_link": "https://www.parlamento.pt/ActividadeParlamentar/Paginas/DetalheIniciativa.aspx?BID=YYYYY",
        "voting_summary": {{ // Derivado DA MESMA tabela compartilhada que acima
        "PS": {{"Favor": 100, "Contra": 0, "Abstenção": 5, "Não Votaram": 2, "TotalDeputados": 107}},
        "PSD": {{"Favor": 0, "Contra": 65, "Abstenção": 0, "Não Votaram": 1, "TotalDeputados": 66}}
        }},
        "proposal_approval_status": 1
    }},
    {{ // Uma proposta não pareada
        "proposal_name": "Voto de Pesar XYZ",
        "proposal_link": "https://www.parlamento.pt/ActividadeParlamentar/Paginas/DetalheIniciativa.aspx?BID=ZZZZZ",
        "voting_summary": null, // Ou inferido se unânime, por exemplo, {{"PS": {{"Favor": 100, ...}}}}
        "proposal_approval_status": null // Ou inferido, por exemplo, 1 se aprovação unânime
    }}
    ]
    """
    return prompt


def create_prompt_for_proposal_pdf():
    prompt = """Analise este documento, que é uma proposta governamental votada no Parlamento português e, portanto, repleta de linguagem jurídica. Forneça uma resposta JSON estruturada. O idioma de todas as strings de texto na resposta JSON deve ser o português de Portugal."""

    response_schema = {
        "type": "OBJECT",
        "properties": {
            "general_summary": {
                "type": "STRING",
                "description": "Um resumo geral da proposta, evitando jargão jurídico e usando vocabulário normal."
            },
            "critical_analysis": {
                "type": "STRING",
                "description": "Pense criticamente sobre o documento e aponte inconsistências, se houver, e se não, mostre como os detalhes da implementação se alinham com o objetivo."
            },
            "fiscal_impact": {
                "type": "STRING",
                "description": "Uma estimativa educada se a proposta aumentará ou diminuirá os gastos do governo e aumentará ou diminuirá a receita do governo também, e qual pode ser o efeito líquido."
            },
            "colloquial_summary": {
                "type": "STRING",
                "description": "Outro resumo, mas em linguagem mais coloquial."
            },
            "categories": {
                "type": "ARRAY",
                "description": "Uma matriz de um ou mais índices de categorias em que esta proposta se enquadra. Escolha entre os seguintes índices de categorias, apenas produza o índice num formato de matriz, não produza o nome da categoria em si:\n   0 - \"Saude e Cuidados Sociais\"\n   1 - \"Educacao e Competências\"\n   2 - \"Defesa e Segurança Nacional\"\n   3 - \"Justica, Lei e Ordem\"\n   4 - \"Economia e Financas\"\n   5 - \"Bem-Estar e Seguranca Social\"\n   6 - \"Ambiente, Agricultura e Pescas\"\n   7 - \"Energia e Clima\"\n   8 - \"Transportes e Infraestruturas\"\n   9 - \"Habitacao, Comunidades e Administracao Local\"\n   10 - \"Negocios Estrangeiros e Cooperacao Internacional\"\n   11 - \"Ciencia, Tecnologia e Digital\"",
                "items": {
                    "type": "INTEGER"
                }
            },
            "short_title": {
                "type": "STRING",
                "description": "Um título conciso para a proposta, máximo de 10 palavras."
            },
            "proposing_party": {
                "type": "ARRAY",
                "description": "Uma lista dos partidos políticos ou entidades que propuseram esta iniciativa (por exemplo, [\"PCP\"], [\"PS\", \"PSD\"], [\"Governo\"]). Extraia isso do texto do documento, geralmente encontrado perto do título ou número da proposta. Se nenhum for claramente identificável, a lista pode ser nula ou vazia.",
                "items": {
                    "type": "STRING"
                },
                "nullable": True
            }
        },
        "required": [
            "general_summary",
            "critical_analysis",
            "fiscal_impact",
            "colloquial_summary",
            "categories",
            "short_title",
            "proposing_party"
        ]
    }

    return prompt, response_schema


def format_structured_data_for_llm(hyperlink_table_pairs, unpaired_links, pre_2020=False):
    """Format the structured data for the LLM, accommodating grouped hyperlinks."""
    structured_data_text = "STRUCTURED PROPOSAL DATA EXTRACTED FROM PDF:\n\n"
    has_data = False

    if hyperlink_table_pairs:
        has_data = True
        structured_data_text += "PROPOSALS WITH VOTING TABLES (a group of proposals may share one table):\n"
        for i, group in enumerate(hyperlink_table_pairs, 1):
            structured_data_text += f"\nGROUP {i} (Page: {group['page_num']}):\n"
            structured_data_text += f"  HYPERLINKS IN THIS GROUP (sharing the table below):\n"
            for link_info in group['hyperlinks']:
                structured_data_text += f"    - TEXT: {link_info['text']}, URI: {link_info['uri']}\n"
            structured_data_text += f"  SHARED VOTING TABLE FOR THIS GROUP:\n"
            table_str = group['table_data'].to_string(index=False, header=True)
            structured_data_text += f"    {table_str.replace(chr(10), chr(10) + '    ')}\n"
            structured_data_text += "  " + "-"*50 + "\n"

    if unpaired_links:
        has_data = True
        if pre_2020:
            structured_data_text += "PROPOSALS LINKS (voting should be below this but may be approved unanimously or in groups where 1 result is the result of all the proposals acima dele.):\n"
        else:
            structured_data_text += "\nPROPOSALS WITHOUT INDIVIDUAL VOTING TABLES (may be approved unanimously or in groups where 1 result is the result of all the proposals acima dele.):\n"
        for i, link in enumerate(unpaired_links, 1):
            structured_data_text += f"\n{i}. PROPOSAL TEXT: {link['hyperlink_text']}\n"
            structured_data_text += f"   LINK: {link['uri']}\n"
            structured_data_text += f"   PAGE: {link['page_num']}\n"

    if not has_data:
        return "NO DATA EXTRACTED FROM PDF"

    return structured_data_text


def build_mp_counts_text(session_date):
    mp_counts_text = "ERRO: Data da sessão fora dos períodos conhecidos para contagem de deputados."

    if not isinstance(session_date, date):
        try:
            session_date = date.fromisoformat(session_date)
        except (ValueError, TypeError):
            pass  # Will result in the "ERRO" message if session_date remains invalid

    if isinstance(session_date, date):
        selected_legislature = None
        for start_date, data in legislature_data.items():
            if start_date <= session_date <= data["end_date"]:
                selected_legislature = data
                break

        if selected_legislature:
            party_lines = []
            # Get the first party for example
            example_party_name = next(iter(selected_legislature["parties"]))
            example_party_count = selected_legislature["parties"][example_party_name]

            for party, count in selected_legislature["parties"].items():
                party_lines.append(
                    f"- {party} ({party_name_map.get(party, party)}): {count}")

            if selected_legislature.get("notes"):
                party_lines.append(f"- {selected_legislature['notes']}")

            party_details_str = "\n".join(party_lines)

            mp_counts_text = f"""Composição parlamentar e contagem de deputados para referência ({selected_legislature['name']}):
{party_details_str}
Total de deputados: {selected_legislature['total_mps']}.

Instrução para 'voting_summary': Para cada partido listado numa categoria de voto (Favor, Contra, Abstenção), atribua o número TOTAL de deputados desse partido a essa categoria. Por exemplo, se o {example_party_name} votou 'Favor', o 'voting_summary' para o {example_party_name} será `{{"Favor": {example_party_count}, "Contra": 0, "Abstenção": 0, "Não Votaram": 0, "TotalDeputados": {example_party_count}}}`. Os outros campos de voto (Contra, Abstenção, Não Votaram) serão 0 para essa entrada específica, a menos que o texto indique o contrário (improvável para este formato).
"""
    return mp_counts_text


def build_response_schema():
    response_schema = {
        "type": "array",
        "description": "Um array JSON onde cada elemento representa UMA proposta (hiperlink) que foi votada.",
        "items": {
            "type": "object",
            "description": "Representa uma proposta votada.",
            "properties": {
                "proposal_name": {
                    "type": "string",
                    "description": "O identificador da proposta (ex: 'Projeto de Lei 404/XVI/1', 'Proposta de Lei 39/XVI/1'). Extraído do texto do hiperlink ou perto do hiperlink pois o texto hiperlink pode ser abreviado. Nunca será 'Texto Final' ou similar."
                },
                "proposal_link": {
                    "type": "string",
                    "description": "O URI/hiperlink para a proposta."
                },
                "voting_summary": {
                    "type": "array",
                    "nullable": True,
                    "description": "Detalhe da votação por partido. Um array onde cada elemento representa um partido e seus votos. Definir como nulo se não houver informação.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "party_name": {"type": "string", "description": "Nome do partido (ex: 'PS', 'PSD')."},
                            "votes": {
                                "type": "object",
                                "properties": {
                                    "Favor": {"type": "integer"},
                                    "Contra": {"type": "integer"},
                                    "Abstenção": {"type": "integer"},
                                    "Não Votaram": {"type": "integer"},
                                    "TotalDeputados": {"type": "integer"}
                                },
                                "required": ["Favor", "Contra", "Abstenção", "Não Votaram", "TotalDeputados"]
                            }
                        },
                        "required": ["party_name", "votes"]
                    }
                },
                "proposal_approval_status": {
                    "type": "integer",
                    "nullable": True,
                    "description": "Um inteiro, 1 se a proposta foi aprovada, 0 se foi rejeitada. Se não estiver claro, defina como nulo. Isso é derivado do 'voting_summary'."
                }
            },
            "required": ["proposal_name", "proposal_link", "voting_summary", "proposal_approval_status"]
        }
    }
    return response_schema



def call_gemini_api(prompt_text, document_path=None, expect_json=False, responseSchema=None):
    """Calls the Gemini API with the given prompt and optional document file."""
    if not genai_client:
        return None, "GEMINI_API_KEY not configured"

    actual_prompt_text = prompt_text
    actual_response_schema = responseSchema

    # Handle cases where prompt_text might be a tuple (prompt_string, schema_dict)
    # This can happen if create_prompt_for_proposal_pdf()'s result is passed directly.
    if isinstance(prompt_text, tuple) and len(prompt_text) == 2:
        potential_prompt_str, potential_schema_dict = prompt_text
        if isinstance(potential_prompt_str, str) and isinstance(potential_schema_dict, dict):
            actual_prompt_text = potential_prompt_str
            if responseSchema is None: # Only use schema from tuple if no explicit schema was passed
                actual_response_schema = potential_schema_dict
            # If responseSchema was explicitly passed, it takes precedence.
            # actual_prompt_text is now correctly the string part.

    print(f"Calling Gemini API. Prompt length: {len(actual_prompt_text)}")

    # Prepare contents array
    contents = [actual_prompt_text]

    # If a document is provided, upload it using the File API
    if document_path and os.path.exists(document_path):
        try:
            print(f"Uploading file: {document_path}")
            uploaded_file = genai_client.files.upload(file=document_path)
            contents.append(uploaded_file)
            print(f"File uploaded successfully: {uploaded_file.name}")
        except Exception as e:
            return None, f"File upload failed: {e}"

    # Prepare generation config
    config = {}
    if expect_json:
        config = {
            "response_mime_type": "application/json",
            "temperature": 0,
            "responseSchema": actual_response_schema # Use the potentially corrected schema
        }

    for attempt in range(LLM_RETRY_ATTEMPTS):
        try:
            response = genai_client.models.generate_content(
                model="gemini-2.5-flash-preview-05-20",
                contents=contents,
                config=config if config else None
            )

            generated_text = response.text

            if not generated_text.strip():
                print(f"Gemini API Warning: Empty text response.")
                return None, "Empty text response from API"

            if expect_json:
                # Clean up JSON response if needed
                cleaned_text = generated_text.strip()
                if cleaned_text.startswith("```json"):
                    cleaned_text = cleaned_text[7:]
                if cleaned_text.endswith("```"):
                    cleaned_text = cleaned_text[:-3]

                try:
                    parsed_json = json.loads(cleaned_text)
                    print("Successfully parsed JSON response from Gemini API.")
                    return parsed_json, None
                except json.JSONDecodeError as e:
                    print(
                        f"Error decoding JSON from Gemini API response: {e}. Response text: {generated_text}")
                    return None, f"JSONDecodeError: {e}. Raw text: {generated_text[:500]}"

            print("Successfully received text response from Gemini API.")
            return generated_text, None

        except Exception as e:
            print(
                f"Error communicating with Gemini API (attempt {attempt + 1}/{LLM_RETRY_ATTEMPTS}): {e}")
            if attempt + 1 == LLM_RETRY_ATTEMPTS:
                return None, f"API error after {LLM_RETRY_ATTEMPTS} attempts: {e}"

        time.sleep(LLM_RETRY_DELAY)
    return None, f"Failed after {LLM_RETRY_ATTEMPTS} attempts."


def validate_llm_proposals_response(extracted_data):
    """Validate the LLM response and return valid proposals."""
    valid_proposals = []
    if not isinstance(extracted_data, list):
        print(
            f"Warning: LLM response was not a list, but {type(extracted_data)}. Data: {str(extracted_data)[:200]}")
        return []

    for item in extracted_data:
        if isinstance(item, dict) and 'proposal_name' in item and item['proposal_name'] is not None:
            valid_proposals.append(item)
        else:
            print(
                f"Warning: LLM returned an invalid item structure or missing proposal_name: {item}")
    return valid_proposals
