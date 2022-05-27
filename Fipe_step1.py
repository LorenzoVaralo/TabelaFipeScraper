import requests
import os
from csv import writer

def path(file):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data', file)
 
output_file= path('step1.csv') 


headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'}
response = requests.get('https://parallelum.com.br/fipe/api/v1/carros/marcas', headers = headers)
resp = response.json()

with open(output_file, 'a+', newline='') as write_obj:
        csv_writer = writer(write_obj)
        for i in resp:
            csv_writer.writerow([f"https://parallelum.com.br/fipe/api/v1/carros/marcas/{i['codigo']}/modelos"])
