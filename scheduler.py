from asyncore import file_dispatcher
import sched
import script as script
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.cron import CronTrigger
import logging
import time
import sys

logging.basicConfig(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO,
                    handlers=[
                        logging.StreamHandler(),
                        logging.FileHandler("jobs.log", mode="a")
                    ])
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

jobstores = {"default": SQLAlchemyJobStore("sqlite:///jobs.sqlite")}

job_defaults = {
    'coalesce': True,
    'max_instances': 1
}

cron_trigger = CronTrigger(
        year="*", month="*", day="*", hour="*", minute="15", second="*"
    )

scheduler = BackgroundScheduler(jobstores=jobstores, job_defaults=job_defaults)

@scheduler.scheduled_job(id="run_backup",trigger="cron", year="*", month="*", day="*", hour="8", minute="0", second="0")
def run_backup():
    custom_fields_pd = script.get_all_users()
    logging.info("Exported all users")
    enrolled_users_pd_32 = script.process_enrolled_users_of_course(32, "Herkese Lazım Dersler")
    logging.info("Exported Herkese Lazım Dersler")
    enrolled_users_pd_33 = script.process_enrolled_users_of_course(33, "Herkes Plan Sever")
    logging.info("Exported Herkes Plan Sever")
    enrolled_users_pd_34 = script.process_enrolled_users_of_course(34, "Herkes Dijital Sever")
    logging.info("Exported Herkes Dijital Sever")
    completed_time = script.mark_completed_time()
    logging.info("Time entered.")
    message_text = script.prepare_message(completed_time, custom_fields_pd, enrolled_users_pd_32, enrolled_users_pd_33, enrolled_users_pd_34)
    script.send_message_to_google_chat(message_text)

# @scheduler.scheduled_job(id="deneme", trigger="cron", second="15")
# def print_periodical(): 
#     print("DENEME")
#     logging.info("Deneme")

if __name__ == '__main__':
    if "--now" in sys.argv:
        print("Task running now.")
        run_backup()

    logging.info("Scheduler starting")
    scheduler.start()
    scheduler.print_jobs()
    while True:
        pass