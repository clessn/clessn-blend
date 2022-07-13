import os

# simple print
print("hello python")

# extraction de variables d'environnement
username = os.environ.get("clhub_username")
password = os.environ.get("clhub_password")
url = os.environ.get("clhub_url")
print(f"username: {username}")
print(f"password: {password}")
print(f"url: {url}")
