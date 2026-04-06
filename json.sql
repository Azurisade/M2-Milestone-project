USE POS;

-- ============================================
-- CASE 1: PRODUCT VIEW (prod.json)
-- ============================================
SELECT JSON_OBJECT(
    'ProductID', p.id,
    'productName', p.name,
    'currentPrice', p.currentPrice,

    'customers', (
        SELECT JSON_ARRAYAGG(
            JSON_OBJECT(
                'CustomerID', c.id,
                'CustomerName', CONCAT(c.firstName, ' ', c.lastName)
            )
        )
        FROM orders o
        JOIN orderline ol ON o.id = ol.order_id
        JOIN customer c ON o.customer_id = c.id
        WHERE ol.product_id = p.id
    )
)
FROM product p
INTO OUTFILE '/var/lib/mysql-files/prod.json';


-- ============================================
-- CASE 2: CUSTOMER DASHBOARD (cust.json)
-- ============================================
SELECT JSON_OBJECT(

    -- Customer Info
    'customer_name', CONCAT(c.firstName, ' ', c.lastName),

    'printed_address_1',
    IF(c.address2 IS NULL,
       c.address1,
       CONCAT(c.address1, ' #', c.address2)
    ),

    'printed_address_2',
    CONCAT(c.city, ', ', c.state, '   ', c.zip),

    -- Orders Array
    'orders', (
        SELECT JSON_ARRAYAGG(
            JSON_OBJECT(

                'order_id', o.id,
                'order_date', o.datePlaced,
                'ship_date', o.dateShipped,

                -- Order Total Calculation
                'order_total', (
                    SELECT SUM(p.currentPrice * ol.quantity)
                    FROM orderline ol
                    JOIN product p ON ol.product_id = p.id
                    WHERE ol.order_id = o.id
                ),

                -- Items Array
                'items', (
                    SELECT JSON_ARRAYAGG(
                        JSON_OBJECT(
                            'product_id', p.id,
                            'product_name', p.name,
                            'quantity', ol.quantity
                        )
                    )
                    FROM orderline ol
                    JOIN product p ON ol.product_id = p.id
                    WHERE ol.order_id = o.id
                )

            )
        )
        FROM orders o
        WHERE o.customer_id = c.id
    )

)
FROM customer c
INTO OUTFILE '/var/lib/mysql-files/cust.json';


-- ============================================
-- CASE 3: PRODUCT SALES ANALYTICS (custom1.json)
-- ============================================
SELECT JSON_OBJECT(

    'product_id', p.id,
    'product_name', p.name,

    'orders', (
        SELECT JSON_ARRAYAGG(
            JSON_OBJECT(
                'order_id', o.id,
                'customer_id', c.id,
                'customer_name', CONCAT(c.firstName, ' ', c.lastName),
                'quantity', ol.quantity
            )
        )
        FROM orders o
        JOIN orderline ol ON o.id = ol.order_id
        JOIN customer c ON o.customer_id = c.id
        WHERE ol.product_id = p.id
    )

)
FROM product p
INTO OUTFILE '/var/lib/mysql-files/custom1.json';


-- ============================================
-- CASE 4: CUSTOMER SPENDING PROFILE (custom2.json)
-- ============================================
SELECT JSON_OBJECT(

    'customer_id', c.id,
    'customer_name', CONCAT(c.firstName, ' ', c.lastName),

    -- Total Spending
    'total_spent', (
        SELECT SUM(p.currentPrice * ol.quantity)
        FROM orders o
        JOIN orderline ol ON o.id = ol.order_id
        JOIN product p ON ol.product_id = p.id
        WHERE o.customer_id = c.id
    ),

    -- Products Purchased
    'products', (
        SELECT JSON_ARRAYAGG(
            JSON_OBJECT(
                'product_id', p.id,
                'product_name', p.name
            )
        )
        FROM orders o
        JOIN orderline ol ON o.id = ol.order_id
        JOIN product p ON ol.product_id = p.id
        WHERE o.customer_id = c.id
    )

)
FROM customer c
INTO OUTFILE '/var/lib/mysql-files/custom2.json';
