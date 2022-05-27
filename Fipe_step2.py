import requests
import csv
import numpy as np
import os


def path(file):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data', file)


input_file = path('step1.csv')
output_file = path('step2.csv')


def treat_resp(url, resp):
    resp = resp['modelos']
    
    with open(output_file, 'a+', newline='') as write_obj:
        csv_writer = csv.writer(write_obj)
        for i in resp:
            csv_writer.writerow([f"{url}/{i['codigo']}/anos"])
    
with open(input_file) as f:
    lista = np.ravel(list(csv.reader(f)))

for url in lista:
    response = requests.get(url, headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'})
    treat_resp(url, response.json())