import requests
import multiprocessing
import queue
from threading import Thread
import csv
import os
import numpy as np
from time import sleep


def path(file):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data', file)

input_file = path('step2.csv')
output_file = path('step3.csv')

def treat_resp(url, resp):
    with open(output_file, 'a+', newline='') as write_obj:
        csv_writer = csv.writer(write_obj)
        for i in resp:
            csv_writer.writerow([f"{url}/{i['codigo']}"])


def perform_web_requests(addresses):
    class Worker(Thread):
        def __init__(self, request_queue):
            Thread.__init__(self)
            self.queue = request_queue

        def run(self):
            while True:
                content = self.queue.get()
                if content == "":
                    break
                try:
                    response = requests.get(content, headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'})
                    treat_resp(content, response.json())
                except:
                    with open('FipeCrawlerLog.txt', 'a') as log:
                        log.write(content)
                        log.write('\n')
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
        worker = Worker(q)
        worker.start()
        workers.append(worker)
        print(worker.name)
        sleep(3)
    # Join workers to wait till they finished
    for worker in workers:
        worker.join()

    with q.mutex:
        q.queue.clear()


with open(input_file) as f:
    lista = np.ravel(list(csv.reader(f)))

perform_web_requests(lista)

with open('FipeCrawlerLog.txt', 'r+') as f:
    correct_errors = f.read().split('\n')
    f.truncate(0)#erase content of file

perform_web_requests(correct_errors)