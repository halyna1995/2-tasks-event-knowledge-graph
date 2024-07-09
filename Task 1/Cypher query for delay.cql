MATCH (e:Event)
RETURN e.timestamp, e LIMIT 10
MATCH (e:Event)
RETURN e.timestamp, datetime(e.timestamp) AS datetimeFormat
LIMIT 10
MATCH (e:Event)
RETURN e.timestamp, datetime(e.timestamp).epochMillis AS numericTimestamp
LIMIT 10
MATCH (e1:Event)-[r:PRECEDES]->(e2:Event)
WHERE e1.timestamp IS NOT NULL AND e2.timestamp IS NOT NULL
RETURN e1, e2, duration.between(datetime(e1.timestamp), datetime(e2.timestamp)).seconds AS delay
ORDER BY delay DESC
LIMIT 10
MATCH path = (start:Event)-[:PRECEDES*1..5]->(end:Event)
WHERE start.timestamp IS NOT NULL AND end.timestamp IS NOT NULL
  AND start.timestamp >= '2021-01-01' AND end.timestamp <= '2021-12-31'
WITH start, end, relationships(path) AS rels
WITH start, end, rels, 
     duration.between(datetime(start.timestamp), datetime(end.timestamp)).seconds AS totalDelay
RETURN start, end, totalDelay
ORDER BY totalDelay DESC
LIMIT 100
