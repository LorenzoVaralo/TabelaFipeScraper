import requests
import multiprocessing
import queue
from threading import Thread
import csv
import os
from tqdm import tqdm
from numpy import ravel
from time import sleep


def path(file):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), file)

input_file = path('data/step3.csv')
output_file = path('data/fipe.csv')


def perform_web_requests(addresses):
    class Worker(Thread):
        def __init__(self, request_queue):
            Thread.__init__(self)
            self.queue = request_queue

        def run(self):
            while True:
                content = self.queue.get()
                if content == "":
                    out.put("")
                    break
                try:
                    response = requests.get(content, headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'})
                    out.put(response.json().values())
                except:
                    with open('FipeCrawlerLog.txt', 'a') as log:
                        log.write(content)
                        log.write('\n')
                self.queue.task_done()
                
                
    class CSVWorker(Thread):
        def __init__(self, request_queue):
            Thread.__init__(self)
            self.queue = request_queue
            self.count_finishes = 0

        def run(self):
            with open(output_file, 'a+', newline='') as write_obj:
                csv_writer = csv.writer(write_obj)
                
                with tqdm(total=q.qsize(), ncols=100) as pbar:
                    while True:
                        content = self.queue.get()
                        
                        if content == "":
                            self.count_finishes +=1
                            if self.count_finishes == (num_workers-1):
                                #Se todos os workers terminaram, sair de while loop
                                break
                        
                        csv_writer.writerow(content)
                        pbar.update(1)
                        self.queue.task_done()

    num_workers = multiprocessing.cpu_count()
            
    out = queue.Queue()
    # Create queue and add addresses
    q = queue.Queue()
    for url in addresses:
        q.put(url)

    # Workers keep working till they receive an empty string
    for _ in range(num_workers):
        q.put("")

    # Create workers and add tot the queue
    workers = []
    for _ in range(num_workers-1):
        worker = Worker(q)
        worker.start()
        workers.append(worker)
        print(worker.name, '✔️')
        sleep(1)
    
    worker = CSVWorker(out)
    worker.start()
    workers.append(worker)
    print('Thread CSVWorker ✔️')
    
    # Esperar acabar o processo dos trabalhadores
    for worker in workers:
        worker.join()



with open(input_file) as f:
    lista = ravel(list(csv.reader(f)))

perform_web_requests(lista) 


with open('FipeCrawlerLog.txt', 'r+') as f:
    correct_errors = f.read().split('\n')
    f.truncate(0)

perform_web_requests(correct_errors)