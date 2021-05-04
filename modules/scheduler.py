from pytz import utc
import time
import os
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from datetime import datetime, timedelta
import random
from modules.connectors import connectors_map
from modules.task_generator import TaskGenerator
from environment import rule_path
import logging

logger = logging.getLogger('unicon')


class Scheduler:
    def __init__(self):
        # self.__jobstores = {'default': RedisJobStore(jobs_key="unicon_tasks", host='localhost', port=6379)}
        self.__executors = {'default': ThreadPoolExecutor(20)}
        self.scheduler = BackgroundScheduler(
            # jobstores=self.__jobstores,
            executors=self.__executors,
            timezone=utc)

    @staticmethod
    def handle_rule_execution(**rule):
        if rule['connector_id'] in connectors_map:
            connector = connectors_map[rule['connector_id']]()
        else:
            logger.warning("Not exist connector %s", rule['connector_id'])
            return
        tg = TaskGenerator()
        while True:
            task = tg.generate_task(rule)
            if tg.its_time_to_start():
                logger.info("[%s] start from %s to %s", task['rule_name'], task['start_time'], task['end_time'])
                connector.start(task)
                tg.new_checkpoint()
                logger.info("[%s] end work", task['rule_name'])
            else:
                break

    def add_job(self, rule):
        interval_time = timedelta(**rule["scheduler_params"]["interval"]).total_seconds()
        job = self.scheduler.add_job(self.handle_rule_execution, 'interval',
                                     kwargs=rule,
                                     seconds=interval_time,
                                     id=rule['rule_name'],
                                     name=rule['rule_name'],
                                     max_instances=1,
                                     # jitter=5,
                                     replace_existing=True,
                                     # **rule["scheduler_params"]["interval"]
                                     )
        job.modify(next_run_time=datetime.utcnow() + timedelta(seconds=random.randint(1, 15)))

    def make_tasks(self):
        logger.debug("Loading scheduler rules from %s", rule_path)
        for rule_file in os.listdir(rule_path):
            with open(f'{rule_path}/{rule_file}') as f:
                rule = yaml.load(f.read(), Loader=yaml.FullLoader)
            if rule:
                self.add_job(rule)

        self.scheduler.print_jobs()


def start_scheduler():
    logger.info("Start scheduler")
    sch = Scheduler()
    sch.make_tasks()
    sch.scheduler.start()
