import streamlit as st
from openai import OpenAI
import pandas as pd

def create_chat_completion(prompt):
    client = OpenAI(
        base_url = "https://integrate.api.nvidia.com/v1",
        api_key= "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"
    )
    chat_completion = client.chat.completions.create(
                                                        messages=[ {"role": "user","content": prompt} ],
                                                        model="meta/llama-3.3-70b-instruct",
                                                        temperature=1,
                                                        top_p=1,
                                                        max_tokens=4096,
                                                        stream=True
                                                    )
    return chat_completion

def generate_prompt(etl_config):
    chunk_size = 5
    all_responses = []
    for i in range(0, len(etl_config), chunk_size):
        chunk = etl_config[i:i + chunk_size]
        rows = ''.join(
            f'target_column: {col}\n'
            f'target_type: {typ}\n'
            f'target_table: {tgt_tbl}\n'
            f'source_table: {src_tbl}\n'
            f'source_column: {src_col}\n'
            f'source_type: {src_typ}\n'
            f'transformation: {trans}\n\n'
            for col, typ, tgt_tbl, src_tbl, src_col, src_typ, trans in chunk
        )
        prompt = open("llama3.3_prompt.txt", "r").read().format(rows=rows)
        print("Processing chunk:", i, "to", i + chunk_size)
        response = create_chat_completion(prompt)
        final_response=""
        for chunk in response:
                    if chunk.choices[0].delta.content is not None:
                        final_response+= chunk.choices[0].delta.content
        final_response=final_response.split("```")
        all_responses.append("--"+final_response[1])
    combined_response='\n\n'.join(res for res in all_responses)
    return combined_response

def generate_query(df):
    etl_config = [tuple(row) for row in df.to_numpy()]
    formatted_response = generate_prompt(etl_config)
    print(formatted_response,"\n\n\n")
    final_prompt=formatted_response+"\n\nConvert the above query into single query in which it should create only one temporary table with combined select statements with appropriate JOINS"
    print(final_prompt,"\n\n\n")
    completion=create_chat_completion(final_prompt)
    final_response=""
    for chunk in completion:
        if chunk.choices[0].delta.content is not None:
            final_response+= chunk.choices[0].delta.content
    final_output="--"+final_response.split("```")[1]
    print(final_output)
    return final_output


st.set_page_config(
    page_title="Evolve",
    page_icon="🤖",
)

st.write("## SQL Bot: Your Data Query Assistant 🤖")
st.markdown(
"""
SQL Bot is an AI-driven tool that empowers users to analyze and interact with their data.  
By uploading a CSV file, users can effortlessly generate SQL-like queries using natural  
language inputs. With the help of Large Language Models (LLMs), SQL Bot interprets your  
questions and provides accurate queries to help you explore and understand your data.  

----  
"""
)
st.sidebar.write("# Setup")
# File uploader
uploaded_file = st.sidebar.file_uploader("Choose a CSV file", type=["csv"])

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    generate_button=st.sidebar.button("Generate Query")
    if  generate_button:
        text = generate_query(df)
        st.text_area("## Final Query:", value=text, height=500)
        st.button("Submit")
else:
    st.write("Please upload a CSV file to start.")
