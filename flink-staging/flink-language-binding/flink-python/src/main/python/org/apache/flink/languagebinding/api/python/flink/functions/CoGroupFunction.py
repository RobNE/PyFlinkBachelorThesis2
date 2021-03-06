# ###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from flink.functions import Function, RuntimeContext
from flink.connection import Iterator, Connection, Collector


class CoGroupFunction(Function.Function):
    def __init__(self):
        super(CoGroupFunction, self).__init__()
        self._keys1 = None
        self._keys2 = None
        self._discard_key1 = False
        self._discard_key2 = False

    def _configure(self, input_file, output_file, port):
        self._connection = Connection.TwinBufferingUDPMappedFileConnection(input_file, output_file, port)
        self._iterator = Iterator.Iterator(self._connection, 0)
        self._iterator2 = Iterator.Iterator(self._connection, 1)
        self._cgiter = Iterator.CoGroupIterator(self._iterator, self._iterator2, self._keys1, self._keys2, self._discard_key1, self._discard_key2)
        self.context = RuntimeContext.RuntimeContext(self._iterator, self._collector)
        self._configure_chain(Collector.Collector(self._connection))

    def _run(self):
        collector = self._collector
        iterator = self._cgiter
        function = self.co_group
        iterator._init()
        while iterator.next():
            result = function(iterator.p1, iterator.p2, collector)
            if result is not None:
                for res in result:
                    collector.collect(res)
            while iterator.p1.has_next():
                iterator.p1.next()
            while iterator.p2.has_next():
                iterator.p2.next()
        collector._close()

    def co_group(self, iterator1, iterator2, collector):
        pass