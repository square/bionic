""" Data model for AI platform hyperparameter tuning

https://cloud.google.com/ai-platform/training/docs/reference/rest/v1/projects.jobs#ParameterSpec
"""
from enum import Enum, auto
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List


class Param(ABC):
    @abstractmethod
    def spec(self):
        pass


class Scale(Enum):
    UNIT_LINEAR_SCALE = auto()
    UNIT_LOG_SCALE = auto()
    UNIT_REVERSE_LOG_SCALE = auto()


@dataclass
class ParamDiscrete(Param):
    name: str
    values: List[float]

    def spec(self):
        return {
            "name": self.name,
            "type": "DISCRETE",
            "discreteValues": self.values,
        }


@dataclass
class ParamCategorical:
    name: str
    values: List[str]

    def spec(self):
        return {
            "parameterName": self.name,
            "type": "CATEGORICAL",
            "categoricalValues": self.values,
        }


@dataclass
class ParamDouble:
    name: str
    min: float
    max: float
    scale: Optional[Scale] = None

    def spec(self):
        s = {
            "parameterName": self.name,
            "type": "DOUBLE",
            "minValue": self.min,
            "maxValue": self.max,
        }
        if self.scale is not None:
            s["scaleType"] = self.scale.name
        return s


@dataclass
class ParamInteger:
    name: str
    min: float
    max: float
    scale: Optional[Scale] = None

    def spec(self):
        s = {
            "parameterName": self.name,
            "type": "INTEGER",
            "minValue": self.min,
            "maxValue": self.max,
        }
        if self.scale is not None:
            s["scaleType"] = self.scale.name
        return s


@dataclass
class Tune:
    metric: str
    params: List[Param]
    goal: str = "MAXIMIZE"
    trials: int = 1
    parallel: int = 1
    resume_previous_job_id: Optional[str] = None
    algorithm: Optional[str] = None

    def spec(self):
        return {
            "hyperparameterMetricTag": self.metric,
            "params": [p.spec() for p in self.params],
            "goal": self.goal,
            "maxTrials": self.trials,
            "maxParallelTrials": self.parallel,
            "resumePreviousJobId": self.resume_previous_job_id,
            "algorithm": self.algorithm,
        }
