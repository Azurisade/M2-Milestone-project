SOURCE /home/cfrierson/etl.sql;

USE POS;

DROP TRIGGER IF EXISTS trg_Orderline_Insert_mv_ProductBuyers;
DROP TRIGGER IF EXISTS trg_Orderline_Delete_mv_ProductBuyers;
DROP TRIGGER IF EXISTS trg_Product_PriceHistory;

DROP TABLE IF EXISTS mv_ProductBuyers;
DROP VIEW IF EXISTS v_ProductBuyers;

-- ============================================================
-- 1. Regular View
-- ============================================================
CREATE VIEW v_ProductBuyers AS
SELECT
    p.id AS productID,
    p.name AS productName,
    IFNULL(
        GROUP_CONCAT(
            DISTINCT CONCAT(c.id, ' ', c.firstName, ' ', c.lastName)
            ORDER BY c.id
            SEPARATOR ', '
        ),
        ''
    ) AS customers
FROM Product p
LEFT JOIN Orderline ol
    ON p.id = ol.product_id
LEFT JOIN `Order` o
    ON ol.order_id = o.id
LEFT JOIN Customer c
    ON o.customer_id = c.id
GROUP BY p.id, p.name
ORDER BY p.id;

-- ============================================================
-- 2. Materialized View Simulation
-- ============================================================
CREATE TABLE mv_ProductBuyers AS
SELECT *
FROM v_ProductBuyers;

CREATE INDEX idx_mv_ProductBuyers_productID
ON mv_ProductBuyers(productID);

-- ============================================================
-- 3. Triggers to keep mv_ProductBuyers current
-- ============================================================
DELIMITER //

CREATE TRIGGER trg_Orderline_Insert_mv_ProductBuyers
AFTER INSERT ON Orderline
FOR EACH ROW
BEGIN
    UPDATE mv_ProductBuyers mv
    SET mv.customers = IFNULL((
        SELECT GROUP_CONCAT(
            DISTINCT CONCAT(c.id, ' ', c.firstName, ' ', c.lastName)
            ORDER BY c.id
            SEPARATOR ', '
        )
        FROM Orderline ol
        JOIN `Order` o
            ON ol.order_id = o.id
        JOIN Customer c
            ON o.customer_id = c.id
        WHERE ol.product_id = NEW.product_id
    ), '')
    WHERE mv.productID = NEW.product_id;
END//

CREATE TRIGGER trg_Orderline_Delete_mv_ProductBuyers
AFTER DELETE ON Orderline
FOR EACH ROW
BEGIN
    UPDATE mv_ProductBuyers mv
    SET mv.customers = IFNULL((
        SELECT GROUP_CONCAT(
            DISTINCT CONCAT(c.id, ' ', c.firstName, ' ', c.lastName)
            ORDER BY c.id
            SEPARATOR ', '
        )
        FROM Orderline ol
        JOIN `Order` o
            ON ol.order_id = o.id
        JOIN Customer c
            ON o.customer_id = c.id
        WHERE ol.product_id = OLD.product_id
    ), '')
    WHERE mv.productID = OLD.product_id;
END//

-- ============================================================
-- 4. Trigger for Price History
-- ============================================================
CREATE TRIGGER trg_Product_PriceHistory
AFTER UPDATE ON Product
FOR EACH ROW
BEGIN
    IF OLD.currentPrice <> NEW.currentPrice THEN
        INSERT INTO PriceHistory (oldPrice, newPrice, product_id)
        VALUES (OLD.currentPrice, NEW.currentPrice, NEW.id);
    END IF;
END//

DELIMITER ;
