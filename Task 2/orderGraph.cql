MATCH (n)
DETACH DELETE n
LOAD CSV WITH HEADERS FROM 'file:///order_process_event_table_orderhandling_prepared.csv' AS row
CREATE (e:Event)
SET e = row,
    e.Order = row.Order,
    e.SupplierOrder = row.SupplierOrder,
    e.Item = row.Item,
    e.Tray = row.Tray,
    e.Payment = row.Payment,
    e.Invoice = row.Invoice,
    e.Order_Details = row.Order_Details,
    e.Actor = row.Actor,
    e.EventID = row.EventID,
    e.Activity = row.Activity,
    e.timestamp = row.timestamp
MATCH (e:Event)
RETURN e
LIMIT 10
MATCH (e:Event)
OPTIONAL MATCH (e)-[r:RELATED]->(e2:Event)
RETURN e, r, e2
