drop table IF EXISTS dim_cuentas;

CREATE TABLE dim_cuentas (
    id_dim_cuenta VARCHAR NOT NULL PRIMARY KEY,
    id_cuenta BIGINT,
    nombre_cuenta VARCHAR NOT NULL,
    correo VARCHAR NOT NULL,
    fecha_creacion TIMESTAMP,
    fecha_actualizacion TIMESTAMP,
    valido_desde date NOT NULL,
    valido_hasta date,
    es_actual BOOLEAN NOT NULL
);

COMMENT ON COLUMN dim_cuentas.id_dim_cuenta IS 'Identificador único generado mediante hash para la cuenta';
COMMENT ON COLUMN dim_cuentas.id_cuenta IS 'Identificador natural o de negocio de la cuenta';
COMMENT ON COLUMN dim_cuentas.nombre_cuenta IS 'Nombre descriptivo de la cuenta';
COMMENT ON COLUMN dim_cuentas.correo IS 'Dirección de correo electrónico asociada a la cuenta';
COMMENT ON COLUMN dim_cuentas.fecha_creacion IS 'Fecha y hora en que la cuenta fue creada en el sistema';
COMMENT ON COLUMN dim_cuentas.fecha_actualizacion IS 'Fecha y hora de la última actualización de la cuenta';
COMMENT ON COLUMN dim_cuentas.valido_desde IS 'Fecha de inicio de validez del registro de la cuenta';
COMMENT ON COLUMN dim_cuentas.valido_hasta IS 'Fecha de fin de validez del registro de la cuenta';
COMMENT ON COLUMN dim_cuentas.es_actual IS 'Indicador booleano que señala si el registro es el más reciente';

INSERT INTO dim_cuentas (
    id_dim_cuenta, id_cuenta, nombre_cuenta, correo, fecha_creacion, fecha_actualizacion, valido_desde, valido_hasta, es_actual
)
SELECT
    'NA' AS id_dim_cuenta, NULL AS id_cuenta, 'No Aplica' AS nombre_cuenta, 'na@example.com' AS correo,
    NULL AS fecha_creacion, NULL AS fecha_actualizacion, TIMESTAMP '1900-01-01' AS valido_desde, TIMESTAMP '9999-12-31' AS valido_hasta, TRUE AS es_actual
UNION ALL
SELECT
    md5(CAST(account_id AS VARCHAR) || account_name || email || CAST(created_at AS VARCHAR) || CAST(updated_at AS VARCHAR)) AS id_dim_cuenta,
    account_id AS id_cuenta,
    account_name AS nombre_cuenta,
    email AS correo,
    created_at AS fecha_creacion,
    updated_at AS fecha_actualizacion,
    '1900-01-01' AS valido_desde, -- Puede ocurrir que la fecha de creacion de una cuenta en la base sea posterior a algún hecho que se registre en las fact tables (aunque no debería ocurrir), por eso no se usa created_at
    '9999-12-31' AS valido_hasta,
    TRUE AS es_actual
FROM
    accounts_historical;

   
BEGIN TRANSACTION;

-- Cerrar registros actuales si hay cambios
UPDATE dim_cuentas
SET
    valido_hasta = cast(daily.updated_at - INTERVAL '1 day' as date),  -- aquí generalmente se usa current_date en vez de updated_at, depende que tan confiable es ese campo
    es_actual = FALSE
FROM
    accounts_daily AS daily
WHERE
    dim_cuentas.id_cuenta = daily.account_id
    AND (
        dim_cuentas.nombre_cuenta <> daily.account_name OR
        dim_cuentas.correo <> daily.email
    )
    AND dim_cuentas.es_actual = TRUE;

-- Insertar nuevos registros para los cambios detectados
INSERT INTO dim_cuentas (
    id_dim_cuenta, id_cuenta, nombre_cuenta, correo, fecha_creacion, fecha_actualizacion, valido_desde, valido_hasta, es_actual
)
SELECT
    md5(CAST(daily.account_id AS VARCHAR) || daily.account_name || daily.email || CAST(daily.created_at AS VARCHAR) || CAST(daily.updated_at AS VARCHAR)) AS id_dim_cuenta,
    daily.account_id AS id_cuenta,
    daily.account_name AS nombre_cuenta,
    daily.email AS correo,
    daily.created_at AS fecha_creacion,
    daily.updated_at AS fecha_actualizacion,
    daily.updated_at::date AS valido_desde,
    '9999-12-31' AS valido_hasta,
    TRUE AS es_actual
FROM
    accounts_daily AS daily
LEFT JOIN
    dim_cuentas AS dim
ON
    daily.account_id = dim.id_cuenta
    AND dim.es_actual = TRUE
WHERE
    dim.nombre_cuenta <> daily.account_name
    OR dim.correo <> daily.email
    OR dim.id_cuenta IS NULL;

COMMIT;


-- Primero, agrupa las cuentas por id_cuenta y selecciona aquellas que tienen más de una ocurrencia.
-- Es decir las que sufrieron cambios en el tiempo.
SELECT
id_cuenta,
id_dim_cuenta,
nombre_cuenta,
correo,
valido_desde,
valido_hasta,
es_actual
FROM
dim_cuentas
WHERE
id_cuenta IN (
    SELECT id_cuenta
    FROM dim_cuentas
    GROUP BY id_cuenta
    HAVING COUNT(*) > 1
    LIMIT 5
)
ORDER BY
id_cuenta, valido_desde;

-- Validar que los registros con es_actual = TRUE tengan valido_hasta = '9999-12-31'
SELECT
    id_cuenta,
    id_dim_cuenta,
    nombre_cuenta,
    correo,
    valido_desde,
    valido_hasta,
    es_actual
FROM
    dim_cuentas
WHERE
    es_actual = TRUE
    AND valido_hasta <> '9999-12-31';

-- Validar que no haya solapamiento en las fechas de validez de un mismo registro
SELECT
    id_cuenta,
    nombre_cuenta,
    correo,
    valido_desde,
    valido_hasta,
    es_actual
FROM
    dim_cuentas AS a
WHERE
    EXISTS (
        SELECT 1
        FROM dim_cuentas AS b
        WHERE
            a.id_cuenta = b.id_cuenta
            AND a.id_dim_cuenta <> b.id_dim_cuenta
            AND a.valido_desde <= b.valido_hasta
            AND a.valido_hasta >= b.valido_desde
    );