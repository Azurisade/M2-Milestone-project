USE POS;

-- ============================================
-- CASE 1: PRODUCT DETAILS VIEW (prod.json)
-- Root: Product
-- Nested: customers who purchased that product
-- ============================================
SELECT JSON_OBJECT(
    'ProductID', p.id,
    'currentPrice', p.currentPrice,
    'productName', p.name,
    'customers',
        COALESCE(
            (
                SELECT JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'CustomerID', c.id,
                        'CustomerName', CONCAT(c.firstName, ' ', c.lastName)
                    )
                )
                FROM Orderline ol
                JOIN `Order` o
                  ON o.id = ol.order_id
                JOIN Customer c
                  ON c.id = o.customer_id
                WHERE ol.product_id = p.id
            ),
            JSON_ARRAY()
        )
)
FROM Product p
INTO OUTFILE '/var/lib/mysql-files/prod.json'
LINES TERMINATED BY '\n';

-- ============================================
-- CASE 2: CUSTOMER DASHBOARD (cust.json)
-- Root: customer
-- Nested: orders
-- Nested inside orders: items
-- Includes formatted addresses and calculated totals
-- ============================================
SELECT JSON_OBJECT(
    'customer_id', c.id,
    'customer_name', CONCAT(c.firstName, ' ', c.lastName),

    'printed_address_1',
        CASE
            WHEN c.address2 IS NULL OR TRIM(c.address2) = '' THEN c.address1
            ELSE CONCAT(c.address1, ' #', c.address2)
        END,

    'printed_address_2',
        CONCAT(ci.city, ', ', ci.state, '   ', ci.zip),

    'email', c.email,
    'birthdate', c.birthdate,

    'orders',
        COALESCE(
            (
                SELECT JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'order_id', o.id,
                        'order_total',
                            (
                                SELECT ROUND(SUM(p2.currentPrice * ol2.quantity), 2)
                                FROM Orderline ol2
                                JOIN Product p2
                                  ON p2.id = ol2.product_id
                                WHERE ol2.order_id = o.id
                            ),
                        'order_date', o.datePlaced,
                        'shipping_date', o.dateShipped,
                        'items',
                            COALESCE(
                                (
                                    SELECT JSON_ARRAYAGG(
                                        JSON_OBJECT(
                                            'ProductID', p.id,
                                            'Quantity', ol.quantity,
                                            'ProductName', p.name
                                        )
                                    )
                                    FROM Orderline ol
                                    JOIN Product p
                                      ON p.id = ol.product_id
                                    WHERE ol.order_id = o.id
                                ),
                                JSON_ARRAY()
                            )
                    )
                )
                FROM `Order` o
                WHERE o.customer_id = c.id
            ),
            JSON_ARRAY()
        )
)
FROM Customer c
JOIN City ci
  ON ci.zip = c.zip
INTO OUTFILE '/var/lib/mysql-files/cust.json'
LINES TERMINATED BY '\n';

-- ============================================
-- CASE 3: REGIONAL CUSTOMER DELIVERY VIEW (custom1.json)
-- Business case:
-- Customer service / logistics can view customers by city with nested customers
-- and each customer's order history for delivery planning
-- ============================================
SELECT JSON_OBJECT(
    'zip', ci.zip,
    'city', ci.city,
    'state', ci.state,
    'customers',
        COALESCE(
            (
                SELECT JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'customer_id', c.id,
                        'customer_name', CONCAT(c.firstName, ' ', c.lastName),
                        'email', c.email,
                        'printed_address_1',
                            CASE
                                WHEN c.address2 IS NULL OR TRIM(c.address2) = '' THEN c.address1
                                ELSE CONCAT(c.address1, ' #', c.address2)
                            END,
                        'orders',
                            COALESCE(
                                (
                                    SELECT JSON_ARRAYAGG(
                                        JSON_OBJECT(
                                            'order_id', o.id,
                                            'datePlaced', o.datePlaced,
                                            'dateShipped', o.dateShipped,
                                            'items',
                                                COALESCE(
                                                    (
                                                        SELECT JSON_ARRAYAGG(
                                                            JSON_OBJECT(
                                                                'product_id', p.id,
                                                                'product_name', p.name,
                                                                'quantity', ol.quantity
                                                            )
                                                        )
                                                        FROM Orderline ol
                                                        JOIN Product p
                                                          ON p.id = ol.product_id
                                                        WHERE ol.order_id = o.id
                                                    ),
                                                    JSON_ARRAY()
                                                )
                                        )
                                    )
                                    FROM `Order` o
                                    WHERE o.customer_id = c.id
                                ),
                                JSON_ARRAY()
                            )
                    )
                )
                FROM Customer c
                WHERE c.zip = ci.zip
            ),
            JSON_ARRAY()
        )
)
FROM City ci
INTO OUTFILE '/var/lib/mysql-files/custom1.json'
LINES TERMINATED BY '\n';

-- ============================================
-- CASE 4: PRODUCT PURCHASE INTELLIGENCE VIEW (custom2.json)
-- Business case:
-- Marketing can see a product with all orders and customer details
-- to support promotions and social proof / targeting
-- ============================================
SELECT JSON_OBJECT(
    'product_id', p.id,
    'product_name', p.name,
    'current_price', p.currentPrice,
    'available_quantity', p.availableQuantity,
    'orders',
        COALESCE(
            (
                SELECT JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'order_id', o.id,
                        'datePlaced', o.datePlaced,
                        'dateShipped', o.dateShipped,
                        'quantity', ol.quantity,
                        'customer',
                            JSON_OBJECT(
                                'customer_id', c.id,
                                'customer_name', CONCAT(c.firstName, ' ', c.lastName),
                                'email', c.email,
                                'city', ci.city,
                                'state', ci.state
                            )
                    )
                )
                FROM Orderline ol
                JOIN `Order` o
                  ON o.id = ol.order_id
                JOIN Customer c
                  ON c.id = o.customer_id
                JOIN City ci
                  ON ci.zip = c.zip
                WHERE ol.product_id = p.id
            ),
            JSON_ARRAY()
        )
)
FROM Product p
INTO OUTFILE '/var/lib/mysql-files/custom2.json'
LINES TERMINATED BY '\n';
