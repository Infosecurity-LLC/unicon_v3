import logging
import threading
from queue import Queue, Empty
import environment as env
from modules.exception_thread import ExceptionThread
from modules.metrics import get_rabbitmq_metric

logger = logging.getLogger('unicon')


def main():
    logger.info("Application start")

    from modules.metrics import run_metrics_webserver
    app_metrics_thread = threading.Thread(target=run_metrics_webserver, daemon=True)
    app_metrics_thread.name = "app_metrics_thread"
    app_metrics_thread.start()
    logger.info("Start %s", app_metrics_thread.name)

    rabbimq_metrics_thread = threading.Thread(
        target=get_rabbitmq_metric,
        args=(env.settings['rabbitmq'],),
        daemon=True
    )
    rabbimq_metrics_thread.name = "rabbimq_metrics_thread"
    rabbimq_metrics_thread.start()
    logger.info("Start %s", rabbimq_metrics_thread.name)

    from modules import scheduler
    scheduler.start_scheduler()

    from modules.buffering.configuration_parser import TaskConfigurationParser
    tasks_handler = TaskConfigurationParser.parse(env.target_config)
    logger.info("Parse task config")

    from modules.buffering.target import target_consuming_executor
    from modules.buffering.amqp import prepare_consumer_channel

    exception_bucket = Queue()
    kill_event = threading.Event()

    target_tasks_threads = []
    target_rmq_channels = []
    for target_task in tasks_handler.target_tasks:
        channel = prepare_consumer_channel(
            settings=env.settings['rabbitmq']['connection'],
            exchange_name=tasks_handler.exchange_label,
            queue_name=target_task.queue_label,
            routing_key=target_task.connector_id
        )
        target_tasks_threads.append(ExceptionThread(
            name=f"{target_task.connector_id}_reporter_thread",
            target=target_consuming_executor,
            args=(target_task, channel),
            exception_bucket=exception_bucket
        ))
        target_rmq_channels.append(channel)

    logger.info("Create %s export target threads", len(target_tasks_threads))

    for thread in target_tasks_threads:
        thread.start()
        logger.info("Start %s executor thread", thread.name)
    from time import sleep
    while True:
        sleep(0.1)
        try:
            exc = exception_bucket.get(block=False)
        except Empty:
            pass
        else:
            kill_event.set()
            for channel in target_rmq_channels:
                try:
                    channel.stop_consuming()
                except Exception:
                    pass
                finally:
                    logger.info("Stop consuming for channel %s", channel)
                    try:
                        channel.connection.close()
                    except Exception:
                        pass
            for thread in target_tasks_threads:
                thread.join()
            logger.error("Everything should be stopped by this exception: %s", str(exc))
            raise exc


if __name__ == '__main__':
    main()
