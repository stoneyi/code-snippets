'''
Created on Apr 10, 2018

@author: root
'''
import multiprocessing
from multiprocessing import Pipe, Process
import threading
from queue import Queue
from spark_computation.spark_setting import getSparkContext
from constants import TaskStatus
# from tornado.ioloop import IOLoop
from tornado.websocket import WebSocketClosedError
from tornado import gen
from constants import q, POOL, CONCURRENT_WORKER, INITIAL_PORT
from taskParser import TaskParser
from zmq.eventloop.future import Context
from zmq.eventloop.ioloop import IOLoop as zmq_IOLoop
import os, time, sys, zmq, json
from datetime import datetime
# import time
# import sys
# import zmq
# import json

def worker_for_child(port):
    Ctx = Context()
    Url = 'tcp://127.0.0.1:' + port
    print("**********worker process: ", os.getpid(), "listening on port: ", port)
    getSparkContext()
    @gen.coroutine
    def run():
        socket = Ctx.socket(zmq.REP)
        print("binding ",Url)
        socket.bind(Url)
        while True:
            #  Wait for next request from client
            task_id = yield socket.recv()
            # print("Received task : {}".format(task_id))
            cur = yield POOL.execute("SELECT params FROM task where id=%s", (int(task_id),))
            params = cur.fetchall()[0][0]
            params = json.loads(params)
            try:
                # time.sleep(30)
                res = TaskParser.parse(**params).execute()
                # print(res)
                yield socket.send(bytes(json.dumps([1, res]), 'utf-8'))
            except Exception as err:
                # computation error
                yield socket.send(bytes(json.dumps([-1, {}]), 'utf-8'))

    def main():
        loop = zmq_IOLoop.current()
        loop.add_callback(lambda: run())
        loop.start()
        
    main()

@gen.coroutine
def consume_task(pid, port):
    print("***********workerpool processes: ", WorkerPool.processes)
    print("******************entering-task-consumer-worker", pid, port)
    while True:
        task = yield q.get()
        print("**************get task ", task.id, " from queue ", pid, port)
        if task.status == TaskStatus.STOPPED:
            continue
        else:
            try:
                if pid not in WorkerPool.processes:
                    print("**************raise key error")
                    raise KeyError
                Url = 'tcp://127.0.0.1:' + port
                Ctx = Context()
                socket = Ctx.socket(zmq.REQ)
                socket.connect(Url)
                task.socket = socket
                yield socket.send_string(str(task.id))
                # update task status
                task.status = TaskStatus.RUNNING
                WorkerPool.running_tasks[task.id] = [task, pid]
                yield POOL.execute("UPDATE task SET status=%s WHERE id=%s", (task.status, task.id,))
                print("**************waiting for result********")
                res = yield socket.recv()
                # print(json.loads(res.decode('utf-8')))
                # task.f.set_result(json.loads(res.decode('utf-8')))
                print("**************come back .....")
            except Exception as err:   # proc is killed 
                print("**************catch exception ....", err, "**************")              
                new_proc = Process(target=worker_for_child, args=(port,))
                new_proc.start()
                while not new_proc.pid:
                    continue
                WorkerPool.processes[new_proc.pid] = new_proc, port
                WorkerPool.io_loop.add_callback(lambda: consume_task(new_proc.pid, port))
                return
            else:  # finish with success
                yield update(json.loads(res.decode('utf-8')), task)
                q.task_done()

@gen.coroutine
def update(result, task):
    try:
        # todo: result is an array with the first element being the code indicating success, error, or...
        if result[0] == TaskStatus.ERR:
            raise RuntimeError("computation error")
        result = result[1]
        print("***************result is ", result)
        finish_time = datetime.now()
        print("task_", task.id, "finished at ", finish_time)
        # front-end would time out, so connection would probably get lost
        try:
            # connection not lost
            task.waiter.write_message(result)
        except WebSocketClosedError:
            # connection lost
            print("socket closed")
        finally:
            # update task status
            if task.id in WorkerPool.running_tasks:
                del WorkerPool.running_tasks[task.id]
            task.status = TaskStatus.DONE
            yield POOL.execute("UPDATE task SET status=%s WHERE id=%s", (task.status, task.id,))
            # update finish time to db
            yield POOL.execute("UPDATE task SET finishTime=%s WHERE id=%s",
                                (finish_time.strftime('%Y-%m-%d %H:%M:%S'), task.id,))
            res_path_cur = yield POOL.execute("SELECT resultPATH FROM task where id=%s", (task.id,))
            res_path = res_path_cur.fetchall()[0][0]
            # write task result along with task params to the DB
            f_result = {"result": result, "params": task.params}
            with open(res_path, "w+") as f:
                f.write(json.dumps(f_result))
    except RuntimeError as e:
        # catch execution error
        try:
            # connection not lost
            task.waiter.write_message(str(e))
        except WebSocketClosedError:
            # connection lost
            print("socket closed")
        finally:
            if task.id in WorkerPool.running_tasks:
                del WorkerPool.running_tasks[task.id]
            task.status = TaskStatus.ERR
            yield POOL.execute("UPDATE task SET status=%s WHERE id=%s", (task.status, task.id,))


@gen.coroutine
def initial_start():    
    @gen.coroutine
    def worker(pid, port):
        yield consume_task(pid, port)
            
    for pid in WorkerPool.processes:
        worker(pid, WorkerPool.processes[pid][1])
    
    # yield q.join()


class WorkerPool(object):
    _instance = None
    io_loop = zmq_IOLoop.current()
    running_tasks = {}
    processes = {}

    def __new__(cls, workers_num):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            cls._instance._max_workers = workers_num
            cls._instance._monitor_daemon_thread = None
            multiprocessing.set_start_method('spawn')
        return cls._instance

    # def start(self):
    #     if self._monitor_daemon_thread is not None:
    #         return
    #     monitor = threading.Thread(target=self._monitor)
    #     self._monitor_daemon_thread = monitor
    #     monitor.setDaemon(True)
    #     monitor.start()
    
    def start(self, g_ioloop):
        assert self.io_loop is g_ioloop
        for incr in range(0, CONCURRENT_WORKER):
            port = str(INITIAL_PORT+incr)
            proc = Process(target=worker_for_child, args=(port,))
            proc.start()
            while not proc.pid:
                continue
            self.processes[proc.pid] = proc, port
        # proc = Process(target=worker_for_child, args=('5555',))
        # proc.start()
        # proc2 = Process(target=worker_for_child, args=('5556',))
        # proc2.start()
        # self.processes[proc.pid] = proc, '5555'
        # self.processes[proc2.pid] = proc2, '5556'
        self.io_loop.add_callback(lambda: initial_start())


    # def _monitor(self):
    #     i = 0
    #     while True:
    #         for _ in range(len(self.processes), self._max_workers):
    #             i += 1
    #             port = str(6666+i) 
    #             proc = Process(target=worker_for_child, args=(port,))
    #             proc.start()
    #             while not proc.pid:
    #                 continue
    #             print(proc.pid)
    #             self.processes[proc.pid] = proc, port
    #             self.io_loop.add_callback(lambda: consume_task(proc.pid, port))

    # def stop(self):
    #     self.terminated = True
    #     self.workers_not_full.notify()

    def join(self):
        '''
        等待所有 worker 都退出
        '''
        self._monitor.join()


