class NoMatchFoundException(Exception):
    def __init__(self, keyword: str):
        self.keyword = keyword
