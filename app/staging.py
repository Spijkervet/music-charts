from charts.model import *

## add distinct tracks
# query = """
#     INSERT INTO `track` (`name`, `spotify_id`)
#     SELECT DISTINCT `name`, `spotify_id`
#     FROM `historicalentry`
#     WHERE NOT EXISTS 
#     (
#     SELECT 1 
#         FROM `track`
#         WHERE `track`.`spotify_id`=`historicalentry`.`spotify_id`
#     );
# """
# cursor = db.execute_sql(query)


# add chart entries
query = """
    INSERT INTO `chartentry` (`date`, `position`, `streams`, `chart_id`, `region_id`, `track_id`)
    SELECT `historicalentry`.date, `historicalentry`.`position`, `historicalentry`.streams, `historicalentry`.`chart_id`, `historicalentry`.`region_id`, `track`.id FROM `historicalentry`
    LEFT JOIN `track` ON `track`.`spotify_id`=`historicalentry`.`spotify_id`
    WHERE `track`.`id` IS NOT NULL
"""
cursor = db.execute_sql(query)

