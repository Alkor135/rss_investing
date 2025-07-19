from openai import OpenAI
import os
from dotenv import load_dotenv

load_dotenv(override=True)
openai_api_key = os.getenv('OPENAI_API_KEY')

if openai_api_key:
  print(f"OpenAI API Key exists and begins {openai_api_key[:8]}")
else:
  print("OpenAI API Key not set")

client = OpenAI(api_key=openai_api_key)

completion = client.chat.completions.create(
  model="gpt-4o-mini",
  store=True,
  messages=[{"role": "user", "content": "напишите хайку об искусственном интеллекте"}]
)

print(completion.choices[0].message)
