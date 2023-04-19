# ----------------------------------------------
# CHAT_WITH_DOC 
#  ------------
# This simple .py script uses Langchain to enhance an LLM (in this case OpenAI gpt) to allow the user to tune the model
# .. to be able to chat with (Q&A) any .txt file (in this case a proposed bill to be sent to congress from congress.gov, bill.txt)
#
# Just replace docs\bill.txt with the .txt file you wish to chat with & pass the query you wish to pass to the model
# ----------------------------------------------



from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.text_splitter import CharacterTextSplitter, RecursiveCharacterTextSplitter
from langchain.vectorstores import Pinecone
from langchain.document_loaders import TextLoader
from langchain.llms import OpenAI
from langchain.chains.question_answering import load_qa_chain
import pinecone
import os

import pinecone 


# os.environ["OPENAI_API_KEY"] = ''     # Insert your OpenAI API key
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

# os.environ["PINECONE_API_KEY"] = ''     # Insert your OpenAI API key
PINECONE_API_KEY = os.environ.get('PINECONE_API_KEY')


# Read in 
path = r"chat_with_doc\docs\bill.txt"     # Path to document that you want to chat with (stored in the chat_with_doc\docs\ path)
 
# LOAD DATA
# -----------------------------------------------

loader = TextLoader(path)
documents = loader.load()

print(f'There are {len(documents)} documents in this data source')
print(f'There are {len(documents[0].page_content)} characters in this document')

# SPLIT DATA INTO CHUNKS
# -----------------------------------------------

text_splitter = RecursiveCharacterTextSplitter(chunk_size=10000, chunk_overlap=10)
docs = text_splitter.split_documents(documents)

print(f'There are now {len(docs)} documents after chunking')


# CREATE EMBEDDINGS USING OPENAI
# -----------------------------------------------

embeddings = OpenAIEmbeddings()

# INITIALIZE PINECONE (vector store to store embeddings of the .txt document)
# -----------------------------------------------

pinecone.init(
    api_key=PINECONE_API_KEY,  # find at app.pinecone.io
    environment="northamerica-northeast1-gcp"  # next to api key in console
)

index_name = "testindex"

docsearch = Pinecone.from_texts([d.page_content for d in docs], embeddings, index_name=index_name)

# QUERY VECTORSTORE (Pinecone) TO LOOK FOR MOST SIMILAR DOCS 
# -----------------------------------------------

# Question to pass to LLM about the .txt file you've embedded

query = """

Who introduced this bill?

"""

docs = docsearch.similarity_search(query,include_metadata=True)    # default is to pull back top 5 docs

# INSTANTIATE LLM & LANGCHAIN CHAIN
# -----------------------------------------------

llm = OpenAI(temperature=0,openai_api_key=os.environ["OPENAI_API_KEY"])

chain = load_qa_chain(llm, chain_type="stuff")


# USE LLM TO QUERY THE RELEVANT SIMILAR DOCS GET AN ANSWER BACK
# -----------------------------------------------

print(chain.run(input_documents=docs, question=query))