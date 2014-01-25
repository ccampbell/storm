class Collection(list):
    def __init__(self):
        self.total_count = None
        self.page = None
        self.page_size = None
        super(Collection, self).__init__(self)
