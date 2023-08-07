import requests
from datetime import datetime

from db_mufap import MFDatabase

db = MFDatabase()
# db.update_fundlist()
cutoff_date = datetime(2023, 7, 22)
db.update_attached(cutoff_date)



# upload
username = 'msaadat'
token = '##########################'

fl = db.path_db_attach.name
dest = f"https://www.pythonanywhere.com/api/v0/user/{username}/files/path/home/{username}/mysite/{fl}"

f = open(fl,'rb')
content = f.read()
f.close()
response = requests.post(
    dest,
    files={'content': content},
    headers={'Authorization': f'Token {token}'}
)

print(response)

# merge local 
db.merge_attached(cutoff_date)
db.path_db_attach.unlink()

# merge on server

response = requests.post(
    "http://msaadat.pythonanywhere.com/merge",
    # "http://localhost:8000/merge",
    json={'cutoff_date': cutoff_date.isoformat()},
)

print(response.content)

db.close()