#!/usr/bin/env python3

from .dataStorage import DataStorage


class InMemoryStorage(DataStorage):
    """
    Storage that stores results in memory without persistency
    """
    def __init__(self):
        super(InMemoryStorage, self).__init__()
        self.database = {}

    def is_connected(self):
        return True

    def connect(self):
        pass

    def disconnect(self):
        pass

    def retrieve(self, task_name, task_id):
        try:
            return self.database[task_id]['result']
        except KeyError:
            raise FileNotFoundError("Record not found in database")

    def store(self, node_args, flow_name, task_name, task_id, result):
        assert task_id not in self.database

        record = {
            'node_args': node_args,
            'flow_name': flow_name,
            'task_name': task_name,
            'task_id': task_id,
            'result': result
        }
        self.database[task_id] = record

        # task_id is unique for the record
        return task_id
