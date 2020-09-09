from charts.model import *

entries = HistoricalEntry().select(HistoricalEntry.artist).distinct()

print(len(entries))