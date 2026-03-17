from dotenv import load_dotenv
load_dotenv()

from langchain_google_vertexai import ChatVertexAI
import os

llm = ChatVertexAI(
    model="gemini-2.5-flash",
    project=os.environ["GOOGLE_CLOUD_PROJECT"],
    location=os.environ["GOOGLE_CLOUD_LOCATION"],
)
print(llm.invoke("Say hello in one sentence.").content)