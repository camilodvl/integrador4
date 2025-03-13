-- TODO: Esta consulta devolverá una tabla con las 10 categorías con menores ingresos
-- (en inglés), el número de pedidos y sus ingresos totales. La primera columna será
-- Category, que contendrá las 10 categorías con menores ingresos; la segunda será
-- Num_order, con el total de pedidos de cada categoría; y la última será Revenue,
-- con el ingreso total de cada categoría.
-- PISTA: Todos los pedidos deben tener un estado 'delivered' y tanto la categoría
-- como la fecha real de entrega no deben ser nulas.#
--falta--
SELECT
    pt.product_category_name_english AS Category,
    COUNT(DISTINCT o.order_id) AS Num_order,
    SUM(py.payment_value) AS Revenue
FROM
    olist_orders o
    JOIN olist_order_payments py ON o.order_id = py.order_id
    JOIN olist_order_items oi ON o.order_id = oi.order_id
    JOIN olist_products p ON oi.product_id = p.product_id
    JOIN product_category_name_translation pt ON p.product_category_name = pt.product_category_name
WHERE
    o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
    AND p.product_category_name IS NOT NULL
GROUP BY
    p.product_category_name
ORDER BY
    Revenue ASC
LIMIT
    10;