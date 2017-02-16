#!/usr/bin/python
# Python 3.5.2
# clock.py
# for scheduling a task

from apscheduler.schedulers.blocking import BlockingScheduler
from rq import Queue
from worker import conn
from run import stream_and_process_trends

from run import run_gather_threads, run_gather_comments

import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

sched = BlockingScheduler()

q = Queue(connection=conn)

def trend_writer():
	q.enqueue(stream_and_process_trends)

sched.add_job(trend_writer) # enqueue upon deploy
sched.add_job(trend_writer, day_of_week='mon-fri', hour=17) 
# sched.add_job(gather_threads, 'interval', minutes=30) # every 30 minutes
sched.start()