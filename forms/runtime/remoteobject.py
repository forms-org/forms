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


class RemoteObject(ABC):
    def __init__(self, object_ref):
        self.object_ref = object_ref

    @abstractmethod
    def get_content(self):
        pass

    @abstractmethod
    def is_object_computed(self):
        pass


class DaskObject(RemoteObject):

    def get_content(self):
        return self.object_ref.result()

    def is_object_computed(self):
        return self.object_ref.done()
