class QuikFyndWorker(Process):
    """ Task worker is a process that gets tasks to execute in a queue. All tasks
    are executable functions, so the task worker doesn't really care what these tasks
    do. It just executes the task and fetches next task from the queue. If a None is
    posted in queue, it's a signal that caller wants to gracefully exit the process, so
    we stop waiting and break out of while loop """

    def __init__(self, worker_queue, parser_queue, task_queue, ocr_queue, 
                 image_proc_queue, clustering_queue, update_queue, backup_queue, shared_dict):
        Process.__init__(self)
        self.worker_queue = worker_queue            # For posting jobs for workers
        self.parser_queue = parser_queue            # For posting jobs for parsing
        self.task_queue = task_queue                # For fetching jobs/tasks
        self.ocr_queue = ocr_queue                  # For fetching OCR related tasks
        self.image_proc_queue = image_proc_queue    # For fetching image helper tasks
        self.clustering_queue = clustering_queue    # For fetching clustering tasks
        self.update_queue = update_queue            # For posting stats to master
        self.backup_queue = backup_queue            # For posting updates for remote backup/synch
        self.idle_timeout = 1                       # if we are idle, we check for DEFERS
        self.shared_dict = shared_dict

    def per_process_init(self, task_queue=None):
        # Initializations to be made once per process
        get_config()
        from audioutil import init as audio_init
        audio_init()
        if task_queue:
            get_async_executor(task_queue)

    def run(self):
        proc_name = self.name
        self.per_process_init(self.task_queue)           # initialization of process state

        # Set up the logger
        setup_logger(get_config().loglevel, get_config().logtarget, get_config().logfile,
                             get_config().logdir, get_config().logmaxbytes, get_config().logbackupcount)

        # Every worker process gets its own instance of indexhelper
        # we need to ensure that minimal state is kept in indexhelper and
        # any shared state is pulled out in a different class
        # The call below is not needed if we didn't have a circular import - see the call for details
        taskhandlers.init(self.worker_queue, self.parser_queue, self.ocr_queue,
                          self.image_proc_queue, self.clustering_queue, self.update_queue, 
                          self.backup_queue, self.shared_dict)

        while True:
            try:
                # we want to get an item within idle_timeout
                # and if we don't get then we consider ourselves
                # as idle and check for any DEFER tasks
                next_tasks = self.task_queue.get(True, self.idle_timeout)

                if next_tasks is None:
                    # Poison pill means shutdown
                    print('%s: Exiting' % proc_name)
                    self.task_queue.task_done()
                    break

                # Most of the time, a task is singleton but we can also have an
                # array of tasks. To make code simple, we convert to array if it
                # was singleton
                if not isinstance(next_tasks, list):
                    next_tasks = [next_tasks]

                len_next_tasks = len(next_tasks)
                while len_next_tasks > 0:

                    next_task = next_tasks.pop(0)
                    len_next_tasks -= 1
                    
                    task_status_dict = self.shared_dict['task_status']

                    # We need to update the global dictionary whenever a new task
                    # has started, so that the health checker can monitor it
                    task_status_dict[self.pid] = {'start_time' : time.time(),
                                                  'task_name' : next_task.name,
                                                  'pid' : self.pid}

                    # There is a task request, so we should execute it
                    # we look up the method to execute from TASKMAP and execute that method
                    # in try/catch so that if throws up an exception, process keeps on going to
                    # next task. task is passed as an argument to method

                    try:
                        taskname = next_task.name
                        # Get the function we want to execute and pass
                        # dbtoken and args. As a convention this method
                        # can pass args back that should be used for next stage
                        # in pipeline. In most cases, args returned is a task itself
                        # but in some cases, it can also be a list of tasks
                        fn = taskmap.get_handler(taskname)
                        if fn is not None:
                            new_tasks = fn(next_task)

                            # Once the task is completed (exception or otherwise), we need
                            # to remove it from the status dict for health checker
                            del task_status_dict[self.pid]

                            # since we have executed the function, check if the
                            # function returned a list of tasks, a single task,
                            # a dictionary of tasks or nothing at all
                            # If a list was returned, our convention is that list of
                            # tasks must have task names already specifed
                            # if a single task was returned, we check if name is same
                            # as current task and if true we get the name from taskmap
                            # if false, we assume that function has created a new task and
                            # knows what it is doing, so we pick up task name form returned task
                            # if a dict was returned, we assume that this is a dict of completed
                            # tasks and we will pick next task from task map
                            # if None was returned, we get the next task name from taskmap
                            if isinstance(new_tasks, list):
                                next_tasks = next_tasks + new_tasks
                                len_next_tasks = len(next_tasks)
                                continue
                            elif isinstance(new_tasks, tasks.Task):
                                if new_tasks.name == taskname:
                                    new_taskname = taskmap.get_next(taskname)
                                else:
                                    new_taskname = new_tasks.name
                            elif isinstance(new_tasks, dict):
                                for t in list(new_tasks.values()):
                                    new_taskname = taskmap.get_next(t.name)
                                    if new_taskname:
                                        new_task = t._replace(name=new_taskname)
                                        next_tasks.append(new_task)
                                        len_next_tasks = len(next_tasks)
                                continue
                            else:
                                new_taskname = taskmap.get_next(taskname)

                            if new_taskname is not None:
                                new_tasks = new_tasks._replace(name=new_taskname)
                                next_tasks.append(new_tasks)
                                len_next_tasks = len(next_tasks)

                        else:

                            # This task is done because fn was None, so we need
                            # to remove it from the status dict for health checker
                            del task_status_dict[self.pid]

                            # handler for this task was none, but may be
                            # it is just a dummy task and we have a next task
                            # in chain. See if that's the case
                            new_taskname = taskmap.get_next(taskname)
                            if new_taskname is not None:
                                new_tasks = next_task._replace(name=new_taskname)
                                next_tasks.append(new_tasks)
                                len_next_tasks = len(next_tasks)
                    
                    except FileNotFoundError as ex:
                        # If we encounter file not found error, we update the 
                        # scan_status with message ERROR_FILE_NOT_FOUND.
                        logger.error('\n')
                        logger.error('======================================')
                        logger.exception(f'Main Worker exception {ex}\n')
                        logger.error('Setting scan status as ERROR_FILE_NOT_FOUND')
                        logger.error('=======================================\n')
                        update_scanstatus_task = tasks.create("update_scanstatus",
                                                              userid=next_task.userid,
                                                              dbtoken=next_task.dbtoken,
                                                              context=next_task.context,
                                                              result='ERROR_FILE_NOT_FOUND')
                        self.worker_queue.put(update_scanstatus_task)

                    except Exception as ex:
                        # we need to figure out how to use logger in multiprocess
                        logger.error('\n')
                        logger.error('=======================================')
                        logger.exception('Main Worker exception {} for task {}'.format(str(ex), 
                                                                                       next_task))
                        logger.error(next_task)
                        logger.error('=======================================\n')

                        # Once the task is completed (exception or otherwise), we need
                        # to remove it from the status dict for health checker
                        del task_status_dict[self.pid]
                        
                        # Call exception handling
                        exception_handling_task = tasks.create("exception_handling",
                                                              userid=next_task.userid,
                                                              dbtoken=next_task.dbtoken)
                        self.worker_queue.put(exception_handling_task)

                    # Update the task status dict
                    self.shared_dict['task_status'] = task_status_dict
                
                self.task_queue.task_done()

            except queue.Empty:
                # we come here because of idle timeout exception
                taskhandlers.handle_housekeeping_work()

            except Exception as ex:
                # Any other exception, we dont care for now
                pass

class TaskManager:
    """Task Manager manages workers for executing various tasks. It has
    two kind of workers. One is a set of workers for things like image thumb,
    text parsing and so on and other is a pool of servers processes that support
    text extraction. While workers can use a threadpool but server processes are
    used in a shared manner because they consume good memory and each worker shouldn't
    have to start its own server. If worker needs any work done via server processes,
    they have to post a request in server_queue """

    def __init__(self, sqlhelp, control_queue, shared_dict):
        """ Initializes Task Manager."""

        # Processes managed by TaskManager
        self.workers = []
        self.next_worker = 0
        self.max_queue_length = 4

        # We maintain a single worker queue for anyone to post new work
        # If calling process owns the TaskManager, it can directly call
        # submit_task_request function, but if work needs to be submitted
        # from other processes, they have to post it to queue
        self.worker_queue = JoinableQueue()
        self.worker_queue_thread = None

        self.cpu_count = multiprocessing.cpu_count()

        # This is for parallelism. We have separate parallel processes which
        # process docs, images and do OCR.
        # For a typical sytem, two insatnces of tika is good enough, considering
        # memory vs parallelism tradeoff
        # NOTE: Can we dynamically assign it, after scanning the filesystem ?
        cfg_doc_parsers = self.config.max_document_parsers
        self.max_document_parsers =  get_num_doc_parsers(self.max_ocr_workers,
                                                         self.cpu_count) \
                if cfg_doc_parsers == -1 else cfg_doc_parsers

        # How long a task is permitted to execute before killing the worker
        self.task_timeout = 300  # 5 minutes


        """ Since workers are running, we can start thread to listen on worker_queue
        Note that worker queue was created already, so we don't miss on any work that is
        queued up in worker queue. We want to start processing that work after all workers
        have started """
        self.worker_queue_thread = threading.Thread(target=self.handle_worker_requests)
        self.worker_queue_thread.setDaemon(True)
        self.worker_queue_thread.start()

        self.health_checker = threading.Thread(target=self._health_check)
        self.health_checker.setDaemon(True)
        self.health_checker.start()

    def _health_check(self):
        """
        Checks for tasks which takes too much time to execute.
        """

        while True:
            # Timestamp for checking hung process
            check_time = time.time()
            restart_process = False
            
            task_status_dict = self.shared_dict['task_status']
            for task_name, task_details in list(task_status_dict.items()):

                # A task took more than 'timeout' period to execute
                if check_time - task_details['start_time'] > self.task_timeout:

                    # Set the restart flag
                    restart_process = True
                    pid = task_details['pid']
                    break

            if restart_process:

                logger.debug('Task {} hangs for more than {} seconds.'\
                             'Restarting the worker process ...'.\
                             format(task_details['task_name'], self.task_timeout))

                try:
                    # Clear the task status dict for the tasks spawned by that
                    # particular worker
                    for task_name, task_details in list(task_status_dict.items()):
                        if task_details['pid'] == pid:
                            del task_status_dict[task_name]
                            self.shared_dict['task_status'] = task_status_dict

                    # Once the hung process is killed, we need to call the taskmanager
                    # to remove that worker and create a new one. Once the new
                    # process is restarted, it will create a new worker process
                    self._restart_worker(pid)

                except Exception as ex:
                    pass

            # Check for update
            if time.time() - self.last_app_update_check > self.app_update_interval:
                try:
                    self.phelper.update_versions_info()
                except:
                    # Update mechanism is not supported for this platform. Ignore
                    pass
                self.last_app_update_check = time.time()

            # Sleep
            time.sleep(30)

    def _restart_worker(self, pid):
        """
        This method iterartes through the worker list to find a process
        with a matching pid, kill the process, remove all the references of
        that process, creates a new one and update the map
        """

        # Iterate through all the worker processes and find the matching process

        for worker in self.workers:
            try:
                if worker['p'].pid == pid:

                    # Kill the worker process
                    worker['p'].terminate()

                    # Wait until child process is terminated
                    worker['p'].join(timeout=10)

                    # pop it from the workers list
                    self.workers.remove(worker)

            except Exception as ex:
                pass

    def submit_task_request(self, task):

        userid = task.userid

        # we need either userid or dbtoken to submit a task
        if userid is None:
            dbtoken = task.dbtoken
        elif userid not in self.userid_map:
            return None
        else:
            dbtoken = self.userid_map[userid]["dbtoken"]

        if dbtoken is None:
            return None

        worker = self.dbtoken_worker_map[dbtoken]

        if worker["l"] >= self.max_queue_length:
            return False
        queue = worker["q"]

        # Before submitting task, grab dbtoken
        task = task._replace(dbtoken=dbtoken)

        # Submit request
        queue.put(task)

    def handle_worker_requests(self):
        while not self.exiting:
            task = self.worker_queue.get(True)
            if task is not None:
                self.submit_task_request(task)
            self.worker_queue.task_done()

    def shutdown(self):
        for worker in self.workers:
            worker['q'].put(None)
            worker['q'].join()

if __name__ == '__main__':
    import sys

    # Main module used for module testing
    TASKMANAGER = TaskManager(SQLHELPER, None)

    folder = "/Users/bakalanest/testdoc"

    task = tasks.create("document_cluster_model", userid=1, eventtime=time.time(), context=cluster_context)
    TASKMANAGER.submit_task_request(task)

    time.sleep(1000)
    #TASK