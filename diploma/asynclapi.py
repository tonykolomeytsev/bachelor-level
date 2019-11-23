"""
Addition to the Clapi library adding asynchronous requests to devices.
The appearance of the API is as close as possible to the HTTP request API.
Direct call of functions and classes from this file is provided only for debug cases,
for all other cases it is better to use only clapy.py functions.
"""

from threading import Thread, Lock
import json
import time



class TaskPool():

    def __init__(self, serial_wrapper):
        self.serial_wrapper = serial_wrapper
        self.running = True
        self.main_thread = None # thread that processes incoming messages and tasks
        self.tasks = list() # tasks queue
        self.subscribers = dict() # (CODE -> LISTENER) where CODE is code of the Task for which the response is expected
        self.task_lock = Lock() 
        self.inbox = dict() # for messages which nobody waited

    def push_task(self, task):
        """ Add task to perform and run main thread """
        self.task_lock.acquire()
        self.tasks.append(task)
        # run thread if it is not alive
        if not self.main_thread or not self.main_thread.isAlive():
            self.main_thread = Thread(target=self.main_loop, daemon=False) # daemon=True for force terminating
            self.main_thread.start()
        self.task_lock.release()

    def push_subscriber(self, s):
        """ Add subscriber for incoming messages """
        self.subscribers[s._code] = s
    
    def main_loop(self):
        """
        Main loop for task performing.
        First, check incoming messages, then perform ONE task
        """
        while self.running:
            self.task_lock.acquire()
            # accept incoming messages
            self.process_input()
            if len(self.tasks):
                self.process_output()
            else:
                time.sleep(0) # like thread.yield() in other langs
            self.task_lock.release()
    
    def process_input(self):
        # TRY TO PRESERVE MESSAGES
        for code, message in self.inbox.items():
            target = self.subscribers.get(code, None)
            if target:
                if target._callback:
                    target._callback(message)
                self.subscribers.pop(code, None)
                self.inbox.pop(code, None)
                if isinstance(target, LongPoll):
                    self.push_task(target)

        while self.serial_wrapper.inWaiting():
            response = json.loads(self.serial_wrapper.pull())
            code = response.get('code', -1)
            if code == -1:
                print('Response to the void (response without CODE):', response) # You cannot reply to asynchronous messages without the "code" field.
            else:
                target = self.subscribers.get(code, None)
                if target:
                    if target._callback:
                        target._callback(response)
                    self.subscribers.pop(code, None)
                    if isinstance(target, LongPoll): # add performed task again to the task queue if the task is long-poll
                        self.push_task(target)
                else: # if the message has no recipient
                    self.inbox[code] = response

    def process_output(self):
        """ Perform ONE task """
        cur_task = self.tasks.pop(0) # take task from queue start

        if isinstance(cur_task, Push):
            self.serial_wrapper.push(cur_task._code, list(cur_task._args))
        
        if isinstance(cur_task, Request) or isinstance(cur_task, LongPoll):
            self.push_subscriber(cur_task) # add subscriber if task is request or long-poll
            self.serial_wrapper.push(cur_task._code, list(cur_task._args))

    def reset(self):
        self.subscribers = dict()
        self.tasks = list()
        if (self.task_lock.locked()): self.task_lock.release()
    
    def __str__(self):
        response =  '\nmain_thread: '
        response += 'active' if self.main_thread and self.main_thread.isAlive() else 'stopped'
        if len(self.tasks):
            for t in self.tasks:
                response += "\n[task] {}".format(t)
        else:
            response += "\nno tasks"
        if len(self.subscribers):
            for s in self.subscribers.values():
                response += "\n[subscriber] {}".format(s)
        else:
            response += "\nno subscribers"
        return response



class Task():

    def __init__(self, control_code, *control_args):
        self._code = control_code
        self._args = control_args
        self._executor = None

    def code(self, control_code:int):
        self._code = control_code
        return self

    def args(self, *control_args):
        self._args = control_args
        return self
    
    def execute(self):
        if self._executor:
            self._executor(self)



class Push(Task):

    def __str__(self):
        return "Push(code={}, args={})".format(self._code, str(self._args))



class CallbackTask(Task):
    
    def __init__(self, control_code, *control_args):
        self._code = control_code
        self._args = control_args
        self._executor = None
        self._callback = None

    def callback(self, control_callback):
        self._callback = control_callback
        return self



class Request(CallbackTask):
    def __str__(self):
        return "Request(code={}, args={}, callback={})".format(self._code, str(self._args), str(self._callback))



class LongPoll(CallbackTask):
    def __str__(self):
        return "LongPoll(code={}, args={}, callback={})".format(self._code, str(self._args), str(self._callback))
        
