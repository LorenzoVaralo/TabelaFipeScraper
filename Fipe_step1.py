import requests
import ast
from csv import writer


headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'}
response = requests.get('https://parallelum.com.br/fipe/api/v1/carros/marcas', headers = headers)
resp = ast.literal_eval(response.text)
with open('Fipe3.csv', 'a+', newline='') as write_obj:
        csv_writer = writer(write_obj)
        for i in resp:
            csv_writer.writerow([f"https://parallelum.com.br/fipe/api/v1/carros/marcas/{i['codigo']}/modelos"])