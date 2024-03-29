from elasticsearch import Elasticsearch,helpers
import pandas as pd
from pprint import pprint
import pickle
import os
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.schema.document import Document
import tqdm.autonotebook

embeddings = HuggingFaceEmbeddings(model_name= '/Users/toddzzr/MLOps/huggingface/m3e-base' , model_kwargs = {'device':'mps'})

index_name = "paperm3e"

# 参数定义
from langchain.text_splitter import CharacterTextSplitter
max_len = 400
text_splitter = CharacterTextSplitter(
    separator = "\n",
    chunk_size = max_len,
    chunk_overlap  = 50,
    length_function = len,
)

def parse_doci(doci):
    docs = []
    for pi in doci:
        pi_splitted = text_splitter.split_text(pi.page_content)
        metadata_list = [dict({'part':i},**pi.metadata) for i in range(len(pi_splitted))]
        pi_new = [Document(page_content=pi_splitted[i], metadata=metadata_list[i]) for i in range(len(pi_splitted))]
        docs.extend(pi_new)
    return docs

def get_text4embedding(pagei):
    text4embedding = f"""以下信息来源于科学论文{pagei.metadata.get('title','')}。论文发表在{pagei.metadata.get('Publisher','')}({pagei.metadata.get('journal','')}) 上。
    {pagei.page_content}
    """
    return text4embedding



files = os.listdir('./Data_paper/2database')
files = [fi for fi in files if fi[-3:]=='pkl']

with tqdm.tqdm(total=len(files)) as pbar:
    for fi in files:

        with open(f'./Data_paper/2database/{fi}','rb') as doc_file:
            doc_data = pickle.load(doc_file)

        ## split the doc to chunks
        new_doc_data = parse_doci(doc_data)
        num_records = len(new_doc_data)

        # embedding
        embeded_pages = [ get_text4embedding(recordi) for recordi in new_doc_data]
        embedded_docs =  embeddings.embed_documents(embeded_pages)

        try:
            with Elasticsearch("https://elastic:mg8RfGAgIIJ80ts5YtLO@58.37.114.191:7360",ca_certs="./ES_certs/http_ca.crt") as es:
                bulk_data = [
                        {
                            "_index": index_name,
                            "_source": {
                                "vector":embedded_docs[i],
                                "text": new_doc_data[i].page_content,
                                "metadata": new_doc_data[i].metadata
                            }
                        } for i in range(num_records)]

                helpers.bulk(es, bulk_data)
        except Exception as e:
            print(f'Error on file {fi}',e)

        pbar.update(1)