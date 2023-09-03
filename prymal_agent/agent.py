from langchain.utilities import TextRequestsWrapper


requests = TextRequestsWrapper()


r = requests.get("https://www.google.com")
