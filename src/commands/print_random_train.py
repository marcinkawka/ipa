class RandomLongTrain:
    name = 'random_long_train'
    desc = 'prints major stations of a random long distance train (>300km)'
    args = []

    def run(self, db, args):
      train = (db.get_random_train())

      print(train.__next__())
      