DROP TABLE IF EXISTS fact_cuentas_suscripcion;

CREATE TABLE fact_cuentas_suscripcion (
    id_dim_cuenta VARCHAR NOT NULL,
    id_dim_suscripcion VARCHAR NOT NULL,
    id_dim_tiempo_dia INT NOT NULL,
    PRIMARY KEY (id_dim_cuenta, id_dim_suscripcion, id_dim_tiempo_dia),
    FOREIGN KEY (id_dim_cuenta) REFERENCES dim_cuentas(id_dim_cuenta),
    FOREIGN KEY (id_dim_suscripcion) REFERENCES dim_suscripciones(id_dim_suscripcion),
    FOREIGN KEY (id_dim_tiempo_dia) REFERENCES dim_tiempo_dia(id_dim_tiempo_dia)
);

COMMENT ON COLUMN fact_cuentas_suscripcion.id_dim_cuenta IS 'Clave foránea hacia dim_cuentas';
COMMENT ON COLUMN fact_cuentas_suscripcion.id_dim_suscripcion IS 'Clave foránea hacia dim_suscripciones';
COMMENT ON COLUMN fact_cuentas_suscripcion.id_dim_tiempo_dia IS 'Clave foránea hacia dim_tiempo_dia';

-- Utilizaremos esta tabla de hechos para rastrear cambios de suscripciones en cuentas.
-- Esta tabla de hechos será de tipo snapshot, es decir, tendremos una foto diaria de las suscripciones por cuenta por día.
-- Dado que los datos históricos de accounts_subscription_historical no cuentan con esa historia completa,
-- utilizaremos esta tabla para hacer una carga inicial correspondiente a la fecha más reciente de esa tabla.
-- Es decir, esta tabla de hechos comenzará a partir del 2024-12-31.
INSERT INTO fact_cuentas_suscripcion (
    id_dim_cuenta,
    id_dim_suscripcion,
    id_dim_tiempo_dia
)
SELECT
    COALESCE(dcu.id_dim_cuenta, 'NA') AS id_dim_cuenta,
    COALESCE(ds.id_dim_suscripcion, 'NA') AS id_dim_suscripcion,
    CAST(strftime((select max(start_date) from accounts_subscription_historical), '%Y%m%d') AS INT) AS id_dim_tiempo_dia
FROM
    accounts_subscription_historical ash
LEFT JOIN
    dim_cuentas dcu
    ON dcu.id_cuenta = ash.account_id
    AND ash.start_date::date BETWEEN dcu.valido_desde AND dcu.valido_hasta -- Calce en periodo de validez
LEFT JOIN
    dim_suscripciones ds
    ON ds.id_suscripcion = ash.subscription_id
;

-- Carga diaria
INSERT INTO fact_cuentas_suscripcion (
    id_dim_cuenta,
    id_dim_suscripcion,
    id_dim_tiempo_dia
)
SELECT
    COALESCE(dcu.id_dim_cuenta, 'NA') AS id_dim_cuenta,
    COALESCE(ds.id_dim_suscripcion, 'NA') AS id_dim_suscripcion,
    CAST(strftime(asd.start_date, '%Y%m%d') AS INT) AS id_dim_tiempo_dia
FROM
    accounts_subscription_daily asd
LEFT JOIN
    dim_cuentas dcu
    ON dcu.id_cuenta = asd.account_id
    AND asd.start_date::date BETWEEN dcu.valido_desde AND dcu.valido_hasta -- Calce en periodo de validez
LEFT JOIN
    dim_suscripciones ds
    ON ds.id_suscripcion = asd.subscription_id
WHERE
    CAST(strftime(asd.start_date, '%Y%m%d') AS INT) > (
        SELECT COALESCE(MAX(id_dim_tiempo_dia), 0) -- Obtener el máximo id_dim_tiempo_dia existente
        FROM fact_cuentas_suscripcion
    );

-- Podriamos también plantear la tabla de hechos para registrar solo las creaciones de cuentas y la suscripción correspondiente.
-- En este caso, si nos sirven los datos de accounts_subscription_historical.
-- Y cambia la forma de hacer la inserción daily.

CREATE TABLE fact_cuentas_suscripcion_creacion (
    id_dim_cuenta VARCHAR NOT NULL,
    id_dim_suscripcion VARCHAR NOT NULL,
    id_dim_tiempo_dia INT NOT NULL,
    PRIMARY KEY (id_dim_cuenta, id_dim_suscripcion, id_dim_tiempo_dia),
    FOREIGN KEY (id_dim_cuenta) REFERENCES dim_cuentas(id_dim_cuenta),
    FOREIGN KEY (id_dim_suscripcion) REFERENCES dim_suscripciones(id_dim_suscripcion),
    FOREIGN KEY (id_dim_tiempo_dia) REFERENCES dim_tiempo_dia(id_dim_tiempo_dia)
);

-- Carga histórica
INSERT INTO fact_cuentas_suscripcion_creacion (
    id_dim_cuenta,
    id_dim_suscripcion,
    id_dim_tiempo_dia
)
SELECT
    COALESCE(dcu.id_dim_cuenta, 'NA') AS id_dim_cuenta,
    COALESCE(ds.id_dim_suscripcion, 'NA') AS id_dim_suscripcion,
    CAST(strftime(ash.start_date, '%Y%m%d') AS INT) AS id_dim_tiempo_dia -- Fecha de creación de la suscripción
FROM
    accounts_subscription_historical ash
LEFT JOIN
    dim_cuentas dcu
    ON dcu.id_cuenta = ash.account_id
    AND ash.start_date::date BETWEEN dcu.valido_desde AND dcu.valido_hasta -- Calce en periodo de validez
LEFT JOIN
    dim_suscripciones ds
    ON ds.id_suscripcion = ash.subscription_id
;

-- Carga diaria
-- Para nuestro caso, no hay cuentas nuevas en la tabla accounts_subscription_daily, por lo que no se insertarán registros.
INSERT INTO fact_cuentas_suscripcion_creacion (
    id_dim_cuenta,
    id_dim_suscripcion,
    id_dim_tiempo_dia
)
SELECT
    COALESCE(dcu.id_dim_cuenta, 'NA') AS id_dim_cuenta,
    COALESCE(ds.id_dim_suscripcion, 'NA') AS id_dim_suscripcion,
    CAST(strftime(asd.start_date, '%Y%m%d') AS INT) AS id_dim_tiempo_dia
FROM
    accounts_subscription_daily asd
LEFT JOIN
    dim_cuentas dcu
    ON dcu.id_cuenta = asd.account_id
    AND asd.start_date::date BETWEEN dcu.valido_desde AND dcu.valido_hasta -- Calce en periodo de validez
LEFT JOIN
    dim_suscripciones ds
    ON ds.id_suscripcion = asd.subscription_id
WHERE NOT EXISTS (
        SELECT 1
        FROM main.fact_cuentas_suscripcion_creacion fc
        WHERE fc.id_dim_cuenta = dcu.id_dim_cuenta
    )
;

-- Calcular la cantidad de cuentas que cambian de suscripción usando la tabla de hechos snapshot
WITH cambios_suscripcion AS (
    SELECT
        id_dim_cuenta,
        COUNT(DISTINCT id_dim_suscripcion) AS cantidad_cambios
    FROM
        fact_cuentas_suscripcion
    GROUP BY
        id_dim_cuenta
    HAVING
        COUNT(DISTINCT id_dim_suscripcion) > 1
)
SELECT
    COUNT(*) AS cantidad_cuentas_con_cambios
FROM
    cambios_suscripcion;