import json

class MapReduceTwoStep:
    """
    Class for
    """
    def __init__(self):
        self.intermediate_map = {}
        self.intermediate_reduce = {}
        self.result = []

    def emit_intermediate_map(self, key, value):
        self.intermediate_map.setdefault(key, [])
        self.intermediate_map[key].append(value)

    def emit_intermediate_reduce(self, key, value):
        self.intermediate_reduce.setdefault(key, [])
        self.intermediate_reduce[key].append(value)

    def emit(self, value):
        self.result.append(value) 

    def execute(self, data, mapper, reducer1, reducer2):
        for line in data:
            record = json.loads(line)
            mapper(record)
        # emits into intermediate_map

        for key in self.intermediate_map:
            reducer1(key, self.intermediate_map[key])
        # emits into intermediate_reduce

        for key in self.intermediate_reduce:
            reducer2(key, self.intermediate_reduce[key])
        # appends to result

        #jenc = json.JSONEncoder(encoding='latin-1')
        jenc = json.JSONEncoder()
        for item in self.result:
            print jenc.encode(item)
