import os


class GlobalConfig:
    def __init__(self):
        self.log = True
        self.log_path = f"{os.getcwd()}/IDWT_log.txt"
        self.db_type = "nosql"

    @staticmethod
    def _type_assertation(given: object,
                          expected: type):
        assert type(given) == expected, f"Expected type {expected}, got {type(given)}"

    def set_log(self, x: bool):
        self._type_assertation(x, bool)
        self.log = x

    def set_log_path(self, path: str):
        self._type_assertation(path, str)
        assert os.path.exists(os.path.dirname(os.path.abspath(path))), "Given path contains a directory that does not " \
                                                                       "exist, create directory before continuing"
        self.log_path = path

    def set_db_type(self, db_type: str):
        self._type_assertation(db_type, str)
        assert db_type in ["nosql", "sql"], "Valid inputs for db_type are: 'nosql' or 'sql'"

