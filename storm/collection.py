class Collection(list):
    def __init__(self):
        self.total_count = None
        self.page = None
        self.page_size = None
        super(Collection, self).__init__(self)

    def has_previous(self):
        return self.page is not None and self.page > 1

    def has_next(self):
        return self.page is not None and self.total_count > (self.page * self.page_size)

    def to_dict(self):
        data = self.__dict__
        data['objects'] = self[:]
        data['has_next'] = self.has_next()
        data['has_previous'] = self.has_previous()
        return data
