# import multiprocessing
# import time
# import random

# def producer(queue):
#     while True:
#         # Produce an item and put it in the queue
#         item = random.randint(1, 10)
#         queue.put(item)
#         print(f"Produced item {item}")
#         time.sleep(random.random())

# def consumer(queue):
#     while True:
#         # Get an item from the queue and consume it
#         item = queue.get()
#         print(f"Consumed item {item}")
#         time.sleep(random.random())

# if __name__ == '__main__':
#     # Create a shared queue
#     queue = multiprocessing.Queue()

#     # Start the producer and consumer processes
#     producer_process = multiprocessing.Process(target=producer, args=(queue,))
#     consumer_process = multiprocessing.Process(target=consumer, args=(queue,))
#     producer_process.start()
#     consumer_process.start()

#     # Wait for the processes to finish
#     producer_process.join()
#     consumer_process.join()
# from queue import Queue


from multiprocessing import JoinableQueue, Process
from time import sleep
import os

import abc
class IWorker(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def run(self):
        pass

class CrawlerWorker(IWorker):
    def __init__(self, path_to_explore: str, folder_queue: JoinableQueue, file_queue: JoinableQueue) -> None:
        ''' always start with a dir '''
        self.path_to_explore = path_to_explore
        self.folder_queue = folder_queue
        self.file_queue = file_queue

        self.folder_queue.put(path_to_explore)


    def run(self):
        while not self.folder_queue.empty():
            current_folder = self.folder_queue.get()
            print("current_dir_for_processing: ", current_folder)
            try:
                files = os.listdir(current_folder)
            except PermissionError:
                print(f"Cannot list contents of {current_folder} due to permission error")
            else:
                files_list = []
                for file in files:
                    current_item = current_folder + "/" + file
                    if os.path.isdir(current_item):
                        self.folder_queue.put(current_item)
                    else:
                        # self.file_queue.put(current_item)
                        files_list.append(current_item)

                if len(files_list) > 0: self.file_queue.put(files_list)
                self.folder_queue.task_done()

class ProcessWorker(IWorker):
    def __init__(self,name: str, folder_queue: JoinableQueue, file_queue: JoinableQueue) -> None:
        self.folder_queue = folder_queue
        self.file_queue = file_queue
        self.name = name

    def run(self):
        while not self.file_queue.empty():
            # while self.file_queue.empty(): pass -> semaphore way of jamming need to be replaced.
            try:
                print(f"{self.name} ", end="")
                file_for_processing = self.file_queue.get(True, 1) # hard code the 1 sec limit
                # print(self.name, "file_for_processing: ", file_for_processing)
                if not file_for_processing: 
                    self.file_queue.task_done()
                    break
            except Exception:
                print("Queue Empty Exception")
            else:
                if not isinstance(file_for_processing, list):
                    file_for_processing = [file_for_processing]

                for file in file_for_processing:
                    stat_data = os.stat(file)

                # print("Stat: ", stat_data)
                self.file_queue.task_done()
        else:
            self.run()

if __name__ == '__main__':
    """
        Shared queues for folder and file separation
    """
    from time import time
    start = time()

    folder_queue = JoinableQueue(maxsize=5000)
    file_queue = JoinableQueue()


    
    crawl_worker = CrawlerWorker('/Users/inno/Documents', folder_queue, file_queue)
    # crawl_worker = CrawlerWorker('/Users/inno/Documents/elastic-demo/src', folder_queue, file_queue)
    worker_process1 = Process(target=crawl_worker.run)
    worker_process1.start()



    # multiple process for files processing.
    file_processor_worker1 = ProcessWorker("P1",folder_queue, file_queue)
    worker_process2 = Process(target=file_processor_worker1.run)
    worker_process2.start()


    file_processor_worker2 = ProcessWorker("P2",folder_queue, file_queue)
    worker_process3 = Process(target=file_processor_worker2.run)
    worker_process3.start()



    # Wait for all items to be processed
    # folder_queue.join()
    # file_queue.join()

    # All items have been processed


    worker_process1.join()
    worker_process2.join()
    worker_process3.join()



    elapsed = time() - start
    
    print ("took {} time to finish".format(elapsed))
