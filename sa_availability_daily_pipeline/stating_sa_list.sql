SELECT DISTINCT(sa.id)
FROM saved_auctions sa 
WHERE sa.id NOT IN (SELECT tl.tableid
FROM total_log tl 
WHERE tl.table_name_id = 495 
	AND tl.field_name_id = 747
	AND tl.updated < '2022-01-01 00:00:00'
	AND new_value = 1 )
	# AND sa.id BETWEEN 1000 AND 5000
