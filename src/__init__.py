import queue
import os
import time
import threading

folder_queue = queue.Queue()
folder_queue_size = 100
files_queue = queue.Queue()
files_queue_size = 100

folder_queue.put('/Users/inno/Documents')
folder_queue_size -= 1

def crawler():
    global folder_queue
    global folder_queue_size
    global files_queue
    global files_queue_size

    while not folder_queue.empty():
        current_folder = folder_queue.get()
        folder_queue_size += 1
        print(f"================== {current_folder} =======================")

        try:
            files = os.listdir(current_folder)
        except PermissionError:
            print(f"Cannot list contents of {current_folder} due to permission error")
        else:
            for file in files:
                current_item = current_folder + "/" + file
                print(f"currently checking {current_item}")
                if os.path.isdir(current_item):
                    print("folder")
                    folder_queue.put(current_item)
                    folder_queue_size -= 1
                else:
                    print("file")
                    files_queue.put(current_item)
                    files_queue_size -= 1
                
                # wait for processing to complete for files in queue
                if( files_queue_size <= 0 ):
                    print("Locking as file size is 100")
                while(files_queue_size <= 0):
                    pass 

            print("folder_queue :" , list(folder_queue.queue))
            print("files_queue :" , list(files_queue.queue))

            print(f"==================// {current_folder} //=======================")


def processor():
    global folder_queue
    global folder_queue_size
    global files_queue
    global files_queue_size

    while True:
        while not files_queue.empty():
            file_for_processing = files_queue.get()
            files_queue_size += 1
            print(f"processing: {file_for_processing}")
            time.sleep(3)
            print(f"processed {file_for_processing}")
        else:
            print("Queue is Empty")


thread1 = threading.Thread(
   target=crawler, args=())
   
thread2 = threading.Thread(
   target=processor, args=())

thread1.start()
thread2.start()

thread1.join()
thread2.join()
   