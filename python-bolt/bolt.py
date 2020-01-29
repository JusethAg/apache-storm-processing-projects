import storm

class bolt(storm.BasicBolt):
  def initialize(self, conf, context):
    self._conf = conf
    self._context = context

  def process(self, tup):
    word = tup.values[0]
    storm.emit([word, len(word)])

bolt().run()