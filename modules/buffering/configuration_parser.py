from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from .target import TargetTask


class TaskConfigurationParser:
    @staticmethod
    def parse(config: Dict) -> TaskHandler:
        exchange_label = config['exchange_label']
        target_tasks = [TaskConfigurationParser.parse_target_task(task) for task in config['targets']]
        return TaskHandler(exchange_label, target_tasks)

    @staticmethod
    def parse_target_task(task: Dict) -> TargetTask:
        return TargetTask(**task)


@dataclass
class TaskHandler:
    exchange_label: str
    target_tasks: List[TargetTask] = field(default_factory=list)
