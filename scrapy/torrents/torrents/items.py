# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/topics/items.html

from scrapy.item import Item, Field

class TorrentsItem(Item):
    fid = Field()
    filename = Field()
    size = Field()
    origins = Field()
    schema = Field()
    meta = Field()
    errorType = Field()
