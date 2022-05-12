import sqlite3
import requests
import json
import ast
import os
import multiprocessing
import queue
from threading import Thread

DIRECTORY_PATH = os.path.abspath(os.path.dirname(__file__))

def treat_resp(url, resp, suffix):
    lista = []
    if 'modelos' in resp:
        resp = resp['modelos']
        
    if suffix == 'GET_PRICES':#when link is at the last level
        assert len(resp) == 9
        return [tuple(resp.values())]
    
    for i in resp:
        lista.append(f"{url}/{i['codigo']}{suffix}")
    return lista

# Source: https://towardsdatascience.com/parallel-web-requests-in-python-4d30cc7b8989

def perform_web_requests(addresses, suffix):
    class Worker(Thread):
        def __init__(self, request_queue, suffix):
            Thread.__init__(self)
            self.suffix = suffix
            self.queue = request_queue
            self.results = []

        def run(self):
            while True:
                content = self.queue.get()
                if content == "":
                    break
                response = requests.get(content, headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'})
                self.results.extend(treat_resp(content, ast.literal_eval(response.text), self.suffix))
                self.queue.task_done()

    num_workers = multiprocessing.cpu_count()
            
    # Create queue and add addresses
    q = queue.Queue()
    for url in addresses:
        q.put(url)

    # Workers keep working till they receive an empty string
    for _ in range(num_workers):
        q.put("")

    # Create workers and add tot the queue
    workers = []
    for _ in range(num_workers):
        worker = Worker(q, suffix)
        worker.start()
        workers.append(worker)
    # Join workers to wait till they finished
    for worker in workers:
        worker.join()

    # Combine results from all workers
    resu = []
    for worker in workers:
        resu.extend(worker.results)
    return resu

base_url = 'https://parallelum.com.br/fipe/api/v1/carros/marcas'

response = perform_web_requests([base_url], '/modelos')
print('RESPONSE 1 ✔️')

response2 = perform_web_requests(response, '/anos')
print('RESPONSE 2 ✔️')

response3 = perform_web_requests(response2, '')
print('RESPONSE 3 ✔️')

response4 = perform_web_requests(response3, 'GET_PRICES')
print('RESPONSE 4 ✔️')

con = sqlite3.connect(os.path.join(DIRECTORY_PATH, 'database.db'))

curr = con.cursor()

try:
    curr.executemany('INSERT INTO fipe VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', response4)
except:
    curr.execute('''CREATE TABLE fipe(
    Valor TEXT, 
    Marca TEXT, 
    Modelo TEXT, 
    AnoModelo INTEGER, 
    Combustivel TEXT, 
    CodigoFipe TEXT, 
    MesReferencia TEXT, 
    TipoVeiculo INTEGER, 
    SiglaCombustivel TEXT);''')
    curr.executemany('INSERT INTO fipe VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', response4)

con.commit()
con.close()
