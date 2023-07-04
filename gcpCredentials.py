import os

# Specify GCP credentials
def specifyGoogleCredentials():
    key_path = os.path.dirname(__file__) + '/key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
