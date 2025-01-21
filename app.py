import streamlit as st  # Import Streamlit
from snowflake.snowpark import Session  # Import Snowpark Session
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root
import pandas as pd
import json

# Access Snowflake credentials from Streamlit secrets
snowflake_user = st.secrets["user"]
snowflake_password = st.secrets["password"]
snowflake_account = st.secrets["account"]

# Access service parameters from Streamlit secrets
cortex_search_database = st.secrets["CORTEX_SEARCH_DATABASE"]
cortex_search_schema = st.secrets["CORTEX_SEARCH_SCHEMA"]
cortex_search_service = st.secrets["CORTEX_SEARCH_SERVICE"]

# Set pandas display options
pd.set_option("max_colwidth", None)

# Default values for context (adjust if needed)
NUM_CHUNKS = 3  # Play with this to check its impact on your results

# Snowflake session connection parameters
connection_parameters = {
    "account": snowflake_account,  # Snowflake account (no region needed if default)
    "user": snowflake_user,        # Snowflake username
    "password": snowflake_password,  # Snowflake password
    "database": cortex_search_database,
    "schema": cortex_search_schema,
    "service": cortex_search_service
    # Add other parameters if needed (role, warehouse, etc.)
}

# Create a Snowflake session using Snowpark
session = Session.builder.configs(connection_parameters).create()


# columns to query in the service
COLUMNS = [
    "chunk",
    "relative_path",
    "category"
]

#session = get_active_session()
root = Root(session)                         
   
### Functions
     
def config_options():

    st.sidebar.selectbox('Select your model:',(
                                    'mixtral-8x7b',
                                    'snowflake-arctic',
                                    'mistral-large',
                                    'llama3-8b',
                                    'llama3-70b',
                                    'reka-flash',
                                     'mistral-7b',
                                     'llama2-70b-chat',
                                     'gemma-7b'), key="model_name")

    categories = session.sql("select category from docs_chunks_table group by category").collect()

    cat_list = ['ALL']
    for cat in categories:
        cat_list.append(cat.CATEGORY)
            
    st.sidebar.selectbox('Select what products you are looking for', cat_list, key = "category_value")

    st.sidebar.expander("Session State").write(st.session_state)

def get_similar_chunks_search_service(query):

    if st.session_state.category_value == "ALL":
        response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)
    else: 
        filter_obj = {"@eq": {"category": st.session_state.category_value} }
        response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)

    st.sidebar.json(response.json())
    
    return response.json()  

def create_prompt (myquestion):

    if st.session_state.rag == 1:
        prompt_context = get_similar_chunks_search_service(myquestion)
  
        prompt = f"""
           You are an expert chat assistance that extracs information from the CONTEXT provided
           between <context> and </context> tags.
           When ansering the question contained between <question> and </question> tags
           be concise and do not hallucinate. 
           If you don´t have the information just say so.
           Only anwer the question if you can extract it from the CONTEXT provideed.
           
           Do not mention the CONTEXT used in your answer.
    
           <context>          
           {prompt_context}
           </context>
           <question>  
           {myquestion}
           </question>
           Answer: 
           """

        json_data = json.loads(prompt_context)

        relative_paths = set(item['relative_path'] for item in json_data['results'])
        
    else:     
        prompt = f"""[0]
         'Question:  
           {myquestion} 
           Answer: '
           """
        relative_paths = "None"
            
    return prompt, relative_paths

def complete(myquestion):

    prompt, relative_paths =create_prompt (myquestion)
    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """
    
    df_response = session.sql(cmd, params=[st.session_state.model_name, prompt]).collect()
    return df_response, relative_paths

def main():
    
    st.title(f":national_park: Parkopedia: Answers for adventurers, explorers, and dreamers.")
    st.write("These are the context documents that will be used to answer your questions:")
    docs_available = session.sql("ls @docs").collect()
    list_docs = []
    for doc in docs_available:
        list_docs.append(doc["name"])
    st.dataframe(list_docs)

    config_options()

    st.session_state.rag = st.sidebar.checkbox('Use your own documents as context?')

    question = st.text_input("Enter question", placeholder="What are some sites to visit in the Acadia National Park", label_visibility="collapsed")

    if question:
        response, relative_paths = complete(question)
        res_text = response[0].RESPONSE
        st.markdown(res_text)

        if relative_paths != "None":
            with st.sidebar.expander("Related Documents"):
                for path in relative_paths:
                    cmd2 = f"select GET_PRESIGNED_URL(@docs, '{path}', 360) as URL_LINK from directory(@docs)"
                    df_url_link = session.sql(cmd2).to_pandas()
                    url_link = df_url_link._get_value(0,'URL_LINK')
        
                    display_url = f"Doc: [{path}]({url_link})"
                    st.sidebar.markdown(display_url)
                
if __name__ == "__main__":
    main()