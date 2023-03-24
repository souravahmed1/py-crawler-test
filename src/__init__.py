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

# from multiprocessing import JoinableQueue, Process
# import os

# import abc


# class IWorker(metaclass=abc.ABCMeta):
#     @abc.abstractmethod
#     def run(self):
#         pass


# class CrawlerWorker(IWorker):
#     def __init__(self, path_to_explore: str, folder_queue: JoinableQueue, file_queue: JoinableQueue) -> None:
#         self.path_to_explore = path_to_explore
#         self.folder_queue = folder_queue
#         self.file_queue = file_queue

#         self.folder_queue.put(path_to_explore)

#     def run(self):
#         while True:
#             current_folder = self.folder_queue.get()
#             if current_folder is None:
#                 # sentinel value, no more folders to process
#                 self.folder_queue.task_done()
#                 break

#             try:
#                 files = os.listdir(current_folder)
#             except PermissionError:
#                 print(f"Cannot list contents of {current_folder} due to permission error")
#             else:
#                 files_list = []
#                 for file in files:
#                     current_item = os.path.join(current_folder, file)
#                     if os.path.isdir(current_item):
#                         self.folder_queue.put(current_item)
#                     else:
#                         files_list.append(current_item)

#                 if files_list:
#                     self.file_queue.put(files_list)

#                 self.folder_queue.task_done()


# class ProcessWorker(IWorker):
#     def __init__(self, name: str, file_queue: JoinableQueue) -> None:
#         self.file_queue = file_queue
#         self.name = name

#     def run(self):
#         while True:
#             files_for_processing = self.file_queue.get()
#             if files_for_processing is None:
#                 # sentinel value, no more files to process
#                 self.file_queue.task_done()
#                 break

#             for file in files_for_processing:
#                 stat_data = os.stat(file)

#             print(f"{self.name} processed {len(files_for_processing)} files")
#             self.file_queue.task_done()


# if __name__ == '__main__':
#     """
#     Shared queues for folder and file separation
#     """
#     from time import time

#     start = time()

#     folder_queue = JoinableQueue(maxsize=5000)
#     file_queue = JoinableQueue()

#     # start multiple crawler worker processes
#     num_crawler_workers = 4
#     for _ in range(num_crawler_workers):
#         crawl_worker = CrawlerWorker('/Users/inno/Documents', folder_queue, file_queue)
#         worker_process = Process(target=crawl_worker.run)
#         worker_process.start()

#     # start multiple file processing worker processes
#     num_file_workers = 2
#     for i in range(num_file_workers):
#         file_processor_worker = ProcessWorker(f"P{i+1}", file_queue)
#         worker_process = Process(target=file_processor_worker.run)
#         worker_process.start()

#     # wait for all folders to be processed
#     folder_queue.join()

#     # add sentinel values to file queue to indicate no more files to process
#     for _ in range(num_file_workers):
#         file_queue.put(None)

#     # wait for all files to be processed
#     file_queue.join()

#     elapsed = time() - start

#     print(f"Took {elapsed} seconds to finish")



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
        self.path_to_explore = path_to_explore
        self.folder_queue = folder_queue
        self.file_queue = file_queue

        self.folder_queue.put(path_to_explore)

    def run(self):
        while True:
            current_folder = self.folder_queue.get()
            if current_folder is None:
                self.folder_queue.task_done()
                break

            print("current_dir_for_processing: ", current_folder)

            try:
                files = os.listdir(current_folder)
            except PermissionError:
                print(f"Cannot list contents of {current_folder} due to permission error")
            else:
                files_list = []
                for file in files:
                    current_item = os.path.join(current_folder, file)
                    if os.path.isdir(current_item):
                        self.folder_queue.put(current_item)
                    else:
                        files_list.append(current_item)

                if files_list:
                    self.file_queue.put(files_list)

            self.folder_queue.task_done()

class ProcessWorker(IWorker):
    def __init__(self, name: str, file_queue: JoinableQueue) -> None:
        self.file_queue = file_queue
        self.name = name

    def run(self):
        while True:
            try:
                files_for_processing = self.file_queue.get(True, 1)
                if not files_for_processing:
                    self.file_queue.task_done()
                    break
            except Exception:
                print(f"Queue Empty Exception for worker {self.name}")
            else:
                if not isinstance(files_for_processing, list):
                    files_for_processing = [files_for_processing]

                for file in files_for_processing:
                    stat_data = os.stat(file)

                print(f"Worker {self.name} processed {len(files_for_processing)} files")
                self.file_queue.task_done()

class ThreadPool:
    def __init__(self, num_workers: int) -> None:
        self.num_workers = num_workers
        self.folder_queue = JoinableQueue(maxsize=5000)
        self.file_queue = JoinableQueue()
        self.workers = []

    def start(self):
        self.start_crawler_worker()
        self.start_file_processor_workers()

    def start_crawler_worker(self):
        crawl_worker = CrawlerWorker('/Users/inno/Documents', self.folder_queue, self.file_queue)
        worker_process = Process(target=crawl_worker.run)
        worker_process.start()
        self.workers.append(worker_process)

    def start_file_processor_workers(self):
        for i in range(self.num_workers):
            worker = ProcessWorker(f"P{i+1}", self.file_queue)
            worker_process = Process(target=worker.run)
            worker_process.start()
            self.workers.append(worker_process)

    def stop(self):
        self.join_queues()
        self.join_workers()

    def join_queues(self):
        self.folder_queue.put(None)
        self.folder_queue.join()
        self.file_queue.join()

    def join_workers(self):
        for worker_process in self.workers:
            worker_process.join()

if __name__ == '__main__':
    num_workers = 2
    thread_pool = ThreadPool(num_workers)
    thread_pool.start()
    thread_pool.stop()