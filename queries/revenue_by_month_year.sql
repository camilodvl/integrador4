-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes (ej. Ene, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).
--falta--
SELECT
    strftime ('%m', oo.order_delivered_customer_date) AS month_no,
    CASE
        WHEN strftime ('%m', oo.order_delivered_customer_date) = '01' THEN 'Jan'
        WHEN strftime ('%m', oo.order_delivered_customer_date) = '02' THEN 'Feb'
        WHEN strftime ('%m', oo.order_delivered_customer_date) = '03' THEN 'Mar'
        WHEN strftime ('%m', oo.order_delivered_customer_date) = '04' THEN 'Apr'
        WHEN strftime ('%m', oo.order_delivered_customer_date) = '05' THEN 'May'
        WHEN strftime ('%m', oo.order_delivered_customer_date) = '06' THEN 'Jun'
        WHEN strftime ('%m', oo.order_delivered_customer_date) = '07' THEN 'Jul'
        WHEN strftime ('%m', oo.order_delivered_customer_date) = '08' THEN 'Aug'
        WHEN strftime ('%m', oo.order_delivered_customer_date) = '09' THEN 'Sep'
        WHEN strftime ('%m', oo.order_delivered_customer_date) = '10' THEN 'Oct'
        WHEN strftime ('%m', oo.order_delivered_customer_date) = '11' THEN 'Nov'
        WHEN strftime ('%m', oo.order_delivered_customer_date) = '12' THEN 'Dec'
    END AS month,
    COALESCE(
        SUM(
            CASE
                WHEN strftime ('%Y', oo.order_delivered_customer_date) = '2016' THEN oop.payment_value
            END
        ),
        0.00
    ) AS Year2016,
    COALESCE(
        SUM(
            CASE
                WHEN strftime ('%Y', oo.order_delivered_customer_date) = '2017' THEN oop.payment_value
            END
        ),
        0.00
    ) AS Year2017,
    COALESCE(
        SUM(
            CASE
                WHEN strftime ('%Y', oo.order_delivered_customer_date) = '2018' THEN oop.payment_value
            END
        ),
        0.00
    ) AS Year2018
FROM
    olist_orders oo
    JOIN olist_order_payments oop ON oo.order_id = oop.order_id
WHERE
    oo.order_delivered_customer_date IS NOT NULL
    AND oo.order_status = 'delivered'
GROUP BY
    month_no
ORDER BY
    month_no;