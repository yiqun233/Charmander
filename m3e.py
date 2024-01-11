import tqdm.autonotebook
from elasticsearch import Elasticsearch, helpers
from langchain.schema.document import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import XinferenceEmbeddings

import sql_excute

EMBEDDING_URL = "https://necboy.gicp.net"
EMBEDDING_MODEL_UID = "m3e"
# embedding
embeddings_M3E = XinferenceEmbeddings(server_url=EMBEDDING_URL, model_uid=EMBEDDING_MODEL_UID)

index_name = "paperm3e_test"

# 参数定义


max_len = 400
text_splitter = RecursiveCharacterTextSplitter(
    separators= ['\n','。'],
    chunk_size = max_len,
    chunk_overlap  = 50,
    length_function = len,
)

def get_new_doc_dart(html):
    docs = []
    # 正文
    chapters = sql_excute.check_chapters_list(html["id"])
    for chapter in chapters:
        if chapter["content"].strip() != chapter["chapter_title1"].strip():
            pi_splitted = text_splitter.split_text(chapter["content"])
            metadata_list = [
                dict(**{'part': i}, **{'title_cn': html["title_cn"]}, **{'title_en': html["title_en"]},
                     **{'authors_cn': html["authors_cn"]}, **{'authors_en': html["authors_en"]},
                     **{'doi': html["doi"]},
                     **{'keywords_cn': html["keywords_cn"]},
                     **{'keywords_en': html["keywords_en"]},
                     **{'publisher': '中国石油化工股份有限公司 石油勘探开发研究院 《石油与天然气地质》编辑部'},
                     **{'journal': '石油与天然气地质'}, **{'organization': ''},
                     **{'sequence': chapter["sequence"]},
                     **{'header1': chapter["chapter_title1"]},
                     **{'header2': chapter["chapter_title2"]},
                     **{'header3': chapter["chapter_title3"]}, **{'type': 1}) for i in range(len(pi_splitted))]
            pi_new = [Document(page_content=pi_splitted[i], metadata=metadata_list[i]) for i in range(len(pi_splitted))]
            docs.extend(pi_new)
    # 摘要 cn
    digest1 = html["digest_cn"]
    if digest1 is not None:
        pi_splitted = text_splitter.split_text(digest1)
        metadata_list = [
            dict(**{'part': i}, **{'title_cn': html["title_cn"]}, **{'title_en': html["title_en"]},
                 **{'authors_cn': html["authors_cn"]}, **{'authors_en': html["authors_en"]},
                 **{'doi': html["doi"]},
                 **{'keywords_cn': html["keywords_cn"]},
                 **{'keywords_en': html["keywords_en"]},
                 **{'publisher': '中国石油化工股份有限公司 石油勘探开发研究院 《石油与天然气地质》编辑部'},
                 **{'journal': '石油与天然气地质'}, **{'organization': ''},
                 **{'sequence': chapter["sequence"]}, **{'header1': chapter["chapter_title1"]},
                 **{'header2': chapter["chapter_title2"]},
                 **{'header3': chapter["chapter_title3"]}, **{'type': 3}) for i in range(len(pi_splitted))]
        pi_new = [Document(page_content=pi_splitted[i], metadata=metadata_list[i]) for i in range(len(pi_splitted))]
        docs.extend(pi_new)

        # 摘要 cn
        digest2 = html["digest_en"]
        if digest2 is not None:
            pi_splitted = text_splitter.split_text(digest2)
            metadata_list = [
                dict(**{'part': i}, **{'title_cn': html["title_cn"]}, **{'title_en': html["title_en"]},
                     **{'authors_cn': html["authors_cn"]}, **{'authors_en': html["authors_en"]},
                     **{'doi': html["doi"]},
                     **{'keywords_cn': html["keywords_cn"]},
                     **{'keywords_en': html["keywords_en"]},
                     **{'publisher': '中国石油化工股份有限公司 石油勘探开发研究院 《石油与天然气地质》编辑部'},
                     **{'journal': '石油与天然气地质'}, **{'organization': ''},
                     **{'sequence': chapter["sequence"]}, **{'header1': chapter["chapter_title1"]},
                     **{'header2': chapter["chapter_title2"]},
                     **{'header3': chapter["chapter_title3"]}, **{'type': 3}) for i in range(len(pi_splitted))]
            pi_new = [Document(page_content=pi_splitted[i], metadata=metadata_list[i]) for i in range(len(pi_splitted))]
            docs.extend(pi_new)
    return docs


def get_text4embedding(pagei):
    text4embedding = f"""以下信息来源于科学论文{pagei.metadata.get('filename', '')}。论文发表在{pagei.metadata.get('publisher', '')}({pagei.metadata.get('journal', '')}) 上。
    {pagei.page_content}
    """
    return text4embedding


def start():
    return


count = sql_excute.check_count()
htmls = sql_excute.check_list()
with tqdm.tqdm(total=count) as pbar:
    for html in htmls:

        ## split the doc to chunks

        # 每一段的内容集合
        new_doc_data = get_new_doc_dart(html)
        num_records = len(new_doc_data)

        # embedding
        embeded_pages = [get_text4embedding(recordi) for recordi in new_doc_data]
        embedded_docs = embeddings_M3E.embed_documents(embeded_pages)
        try:
            with Elasticsearch("https://elastic:mg8RfGAgIIJ80ts5YtLO@58.37.114.191:7360", verify_certs=False) as es:
                bulk_data = [
                    {
                        "_index": index_name,
                        "_source": {
                            "vector": embedded_docs[i],
                            "text": new_doc_data[i].page_content,
                            "metadata": new_doc_data[i].metadata
                        }
                    } for i in range(num_records)]

                helpers.bulk(es, bulk_data)
        except Exception as e:
            print(f'Error on file {html["id"]}', e)
