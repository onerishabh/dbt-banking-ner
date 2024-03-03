
import duckdb
import pandas as pd
from transformers import (AutoModelForTokenClassification, AutoTokenizer,
                          pipeline)

TOKENIZER = AutoTokenizer.from_pretrained("dslim/bert-base-NER")
MODEL = AutoModelForTokenClassification.from_pretrained("dslim/bert-base-NER")
NLP = pipeline("ner", model=MODEL, tokenizer=TOKENIZER)

def ner_tags(description: str):
    ner_results = NLP(description)
    print(f"Processing: {description}")
    print(f"Results: {ner_results}")

    results = {'ORG': '', 'LOC': '', 'PER': ''}
    if not len(ner_results):
        return results

    df = pd.DataFrame(ner_results)

    df["entity"] = df["entity"].apply(lambda x: x.split("-")[1])
    print(df.head(10))

    # group by entity and get max score
    # select entity, max(score) from df group by 1
    entity_indexes = df.groupby("entity")["score"].idxmax()

    results = {}
    for entity in ["ORG", "LOC", "PER"]:
        if entity in entity_indexes:
            results[entity] = df.loc[entity_indexes[entity], "word"]
        else:
            results[entity] = ''
    return results

def generate_dim_ner(duck_db_file="dbt.duckdb"):
    conn = duckdb.connect(duck_db_file)
    df = conn.sql("select txn_key, \"desc\" from fact_txn").df()
    
    df["ner_tags"] = df["Desc"].apply(ner_tags)

    print(df.head())

    conn.sql("create or replace table dim_ner as select * from df")
    print("dim_ner created successfully")

generate_dim_ner()

