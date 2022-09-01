#  Copyright 2022-2023 The FormS Authors.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from abc import abstractmethod, ABC
from enum import Enum, auto
from dask.distributed import Client
from forms.core.config import FormSConfig
from forms.runtime.remoteobject import RemoteObject, DaskObject
from forms.utils.exceptions import RuntimeNotSupportedException


class BaseRuntime(ABC):
    def __init__(self, forms_config: FormSConfig):
        self.forms_config = forms_config

    @abstractmethod
    def distribute_data(self, data) -> RemoteObject:
        pass

    @abstractmethod
    def submit_one_func(self, func, *args) -> RemoteObject:
        pass

    @abstractmethod
    def shut_down(self):
        pass


class DaskRuntime(BaseRuntime):
    def __init__(self, forms_config: FormSConfig):
        super().__init__(forms_config)
        self.client = Client(processes=True, n_workers=self.forms_config.cores)
        self.broadcast = False

    def distribute_data(self, data) -> RemoteObject:
        return DaskObject(self.client.scatter([data], broadcast=self.broadcast)[0])

    def submit_one_func(self, func, *args) -> RemoteObject:
        return DaskObject(self.client.submit(func, *args))

    def shut_down(self):
        self.client.close()


class Runtime(Enum):
    DASK = auto()


runtime_class_dict = {Runtime.DASK.name.lower(): DaskRuntime}


def create_runtime_by_name(runtime_name: str, forms_config: FormSConfig):
    if runtime_name.lower() in runtime_class_dict.keys():
        return runtime_class_dict[runtime_name](forms_config)
    raise RuntimeNotSupportedException(f"Runtime {runtime_name} is not supported")
