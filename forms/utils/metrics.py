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

PARSING_TIME = "parsing_time"
REWRITE_TIME = "rewrite_time"
TRANSLATION_TIME = "translation_time"
EXECUTION_TIME = "execution_time"
TOTAL_TIME = "total_time"
NUM_SUBPLANS = "num_subplans"
MICROS_PER_SEC = 1000000


class MetricsTracker:
    def __init__(self):
        self.metrics = {}

    def put_one_metric(self, key, value):
        self.metrics[key] = value

    def get_metrics(self):
        return self.metrics

    def reset_metrics(self):
        self.metrics = {}
