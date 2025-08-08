from google import genai

client = genai.Client()

print('My files:')
for f in client.files.list():
    print(' ', f.name)