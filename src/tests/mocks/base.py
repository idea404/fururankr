class MockSession:
    objects_in_db = set()

    class MockQuery:
        query_results = {}

        def __init__(self, class_name):
            self.class_name = class_name

        def all(self):
            return self.query_results.get(self.class_name, [])

    def __init__(self):
        self.query = self.MockQuery

    def set_return_queries(self, obj_dict_result):
        self.MockQuery.query_results = obj_dict_result

    def commit(self):
        pass

    def add(self, obj):
        self.objects_in_db.add(obj)

    def query(self, *args):
        return self.query(*args)

    def __repr__(self):
        return "MockSession"
