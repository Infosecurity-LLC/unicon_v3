from datetime import datetime, timedelta
from environment import redis
import croniter
import logging

logger = logging.getLogger('unicon')


class TaskGenerator:
    rule_name: str = None  # имя правила (должно быть уникальным)
    organization: str = None  # организация
    connector_id: str = None  # id коннектора
    scheduler_params: dict = None  # как запускать таску
    config: str = None  # ссылка на файл конфига
    start_time: datetime = None  # с какого времени собирать данные
    end_time: datetime = None  # до какого времени собирать данные

    @staticmethod
    def rn_format_rule_name(rule_name: str):
        rule_name = rule_name.replace(" ", "-")
        rule_name = rule_name.lower()
        return rule_name

    @staticmethod
    def get_checkpoint(rule_name: str, fts_offset: dict = None, rts_offset: dict = False):
        """
        Получаем время, от которого необходимо осуществить сбор данных
            Если в хранилище нет временной метки, генерируем новую и записываем в хранилище

        :param rule_name: имя правиила после преобразования в формат task_rule_name (replace(" ","-"))
        :param fts_offset: отступ по времени назад от полученной временной метки при первом запуске
        :param rts_offset: отступ по времени назад от полученной временной метки
        :return: datetime
        """

        checkpoint_name = f"checkpoint_{rule_name}"
        checkpoint = redis.get(checkpoint_name)
        if not checkpoint:
            checkpoint = datetime.utcnow()
            if not fts_offset:
                fts_offset = {"minutes": 5}
            logger.info(f"It is the first start for [{rule_name}], make time offset [{timedelta(**fts_offset)}]")
            checkpoint = checkpoint - timedelta(**fts_offset)
            redis.set(checkpoint_name, checkpoint.isoformat())
        else:
            checkpoint = datetime.fromisoformat(checkpoint.decode("utf-8"))
            if rts_offset:
                checkpoint = checkpoint - timedelta(**rts_offset)
        return checkpoint

    def its_time_to_start(self):
        """
        Можно ли запускать таску.
        Если время начала и окончания сбоа данных меньше времени "сейчас"
        :return: bool
        """
        now = datetime.utcnow()
        if self.start_time < self.end_time <= now:
            return True
        return False

    def calculate_number_of_task(self, start_time: datetime):
        """
        Сколько тасок до конца сбора данных
        :param start_time:
        :return: int
        """
        # TODO
        pass

    @staticmethod
    def calculate_end_time(start_time: datetime, scheduler_params: dict):
        """
        Рассчёт end_time от start_time по параметрам запуска для шедулера
        :param start_time: время запуска
        :param scheduler_params: параметры запуска
            'interval' - простой тип для apscheduler, прибавляем к start_time заданный интервал
            'cron' - при указании в правиле этого типа, метод вернёт время следующего запуска, согласно правилу крона
                при несоответствии start_time интервальности правила cron, получаемое временное окно не будет иметь
                характеристику строгой интервальности как в случае с типом 'interval'

        :return: datetime
        """
        for schedule_type, params in scheduler_params.items():
            if schedule_type == "interval":
                return start_time + timedelta(**params)
            elif schedule_type == "cron":
                cron = croniter.croniter(params, start_time)
                return cron.get_next(datetime)

    def new_checkpoint(self):
        """
        Записываем в хранилище новую временную метку из таски
        :return:
        """
        try:
            redis.set(f"checkpoint_{self.rule_name}", self.end_time.isoformat())
        except Exception as err:
            logger.error(err)
            return False
        return True

    def generate_task(self, rule: dict):
        """
        Из правила формируем таску, которую с которой уже может работать коннектор
        rule_name переводится в нижний регистр и пробелы заменяются на "-"
        Добавляется start_time и end_time
            start_time подтягивается из redis или генерируется новый
            end_time рассчитывается исходя из значения start_time и параметров периодичности запуска из правила
        :param rule:
        :return: dict
        """
        self.rule_name = self.rn_format_rule_name(rule['rule_name'])
        self.organization = rule['organization']
        self.connector_id = rule['connector_id']
        self.scheduler_params = rule['scheduler_params']
        self.config = rule['connector_config']
        self.start_time = self.get_checkpoint(self.rule_name,
                                              rule.get('first_start_time_offset', {}),
                                              rule.get('regular_start_time_offset', {}))
        self.end_time = self.calculate_end_time(self.start_time, self.scheduler_params)
        gtask = {}
        for ik, iv in self.__dict__.items():
            if "__" in ik:
                continue
            gtask[ik] = iv
        return gtask
