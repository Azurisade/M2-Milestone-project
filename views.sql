SOURCE /home/cfrierson/etl.sql;

USE POS;

DROP VIEW IF EXISTS v_ProductBuyers;

CREATE VIEW v_ProductBuyers AS
SELECT
    p.productID,
    p.name AS productName,
    GROUP_CONCAT(
        DISTINCT CONCAT(c.customerID, ' ', c.firstName, ' ', c.lastName)
        ORDER BY c.customerID
        SEPARATOR ', '
    ) AS customers
FROM Product p
LEFT JOIN Orderline ol
    ON p.productID = ol.productID
LEFT JOIN Orders o
    ON ol.orderID = o.orderID
LEFT JOIN Customer c
    ON o.customerID = c.customerID
GROUP BY p.productID;
