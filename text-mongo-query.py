import json
import requests

duckdb_nql_model = 'duckdb-nsql:7b' # TODO: update this for whatever model you wish to use
llm_model = 'gemma2'

system_prompt = """Here is the database schema that the SQL query will run on:
CREATE TABLE unit (
   id varchar(200),
   name varchar(120)
);
#type can have value meter
CREATE TABLE appliance (
   id varchar(200),
   name varchar(120),
   type varchar(120), 
   serialno varchar(32),
   balance double,
   grid_reading double,
   dg_reading double,
   unit_id varchar(200) REFERENCES unit(id),,
   current_source varchar(16)
);
"""

def generate_sql(prompt, context):
    r = requests.post('http://localhost:11434/api/generate',
                      json={
                          'model': duckdb_nql_model,
                          'prompt': prompt,
                          'context': context,
                          "system" : system_prompt
                      },
                      stream=False)
    r.raise_for_status()

    str=""
    for line in r.iter_lines():
        body = json.loads(line)
        response_part = body.get('response', '')
        # the response streams one token at a time, print that as we receive it
        str=str+response_part
        if 'error' in body:
            raise Exception(body['error'])

        if body.get('done', False):
            return str

def generate_mongo_query(prompt, context):
    r = requests.post('http://localhost:11434/api/generate',
                      json={
                          'model': llm_model,
                          'prompt': prompt,
                          'context': context
                      },
                      stream=False)
    r.raise_for_status()

    str=""
    for line in r.iter_lines():
        body = json.loads(line)
        response_part = body.get('response', '')
        # the response streams one token at a time, print that as we receive it
        str=str+response_part
        if 'error' in body:
            raise Exception(body['error'])

        if body.get('done', False):
            return str

def main():
    context = [] # the context stores a conversation history, you can use this to make the model more context aware
    text_input = "get meters where balnce geater than 300"
    print("input :" , text_input)
    sql_response = generate_sql(text_input, context)
    print("SQL :",sql_response)
    context = []
    mongo_text_input = "generate single line mongodb query only in response in plain text for sql " + sql_response
    mongo_query = generate_mongo_query(mongo_text_input, context)
    print("Mongo query :", mongo_query)

if __name__ == "__main__":
    main()
