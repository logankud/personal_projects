CHAT_WITH_DOC 
#  ------------

This simple .py script uses Langchain to enhance an LLM (in this case OpenAI gpt) to allow the user to tune the model to be able to chat with (Q&A) any .txt file (in this case a proposed bill to be sent to congress from congress.gov, bill.txt).   


To run: 
1) replace docs\bill.txt with the .txt file you wish to chat with
2) input an OPENAI_API_KEY
3) input a Pinecone (vector store) PINECONE_API_KEY
3) Update the 'query' value with a query that you wish to pass to the model
