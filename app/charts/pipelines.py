# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from .model import db, HistoricalEntry
from .settings import add_to_items_buffer, get_items_buffer_len, empty_items_buffer, get_items_buffer


class ChartsPipeline:

    def process_item(self, item, spider):
        add_to_items_buffer(item)
        if get_items_buffer_len() > 10000:
            with db.atomic():
                HistoricalEntry.insert_many(get_items_buffer()).execute()
            empty_items_buffer()
        return item
