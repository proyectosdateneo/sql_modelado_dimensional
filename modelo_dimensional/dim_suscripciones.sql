DROP TABLE IF EXISTS dim_suscripciones;

CREATE TABLE dim_suscripciones (
    id_dim_suscripcion VARCHAR NOT NULL PRIMARY KEY,
    id_suscripcion BIGINT NOT NULL,
    nombre_suscripcion VARCHAR NOT NULL,
    max_contenidos_mensuales INT NOT NULL,
    fecha_creacion TIMESTAMP NOT NULL,
    fecha_actualizacion TIMESTAMP NOT NULL
);

COMMENT ON COLUMN dim_suscripciones.id_dim_suscripcion IS 'Clave sustituta generada como un hash de los atributos de la suscripción';
COMMENT ON COLUMN dim_suscripciones.id_suscripcion IS 'Clave natural o de negocio que representa el ID de la suscripción';
COMMENT ON COLUMN dim_suscripciones.nombre_suscripcion IS 'Nombre de la suscripción';
COMMENT ON COLUMN dim_suscripciones.max_contenidos_mensuales IS 'Número máximo de contenidos permitidos por mes para la suscripción';
COMMENT ON COLUMN dim_suscripciones.fecha_creacion IS 'Marca de tiempo cuando se creó la suscripción';
COMMENT ON COLUMN dim_suscripciones.fecha_actualizacion IS 'Marca de tiempo cuando se actualizó por última vez la suscripción';

-- Insertar valores iniciales de "No Aplica"
INSERT INTO dim_suscripciones (
    id_dim_suscripcion, id_suscripcion, nombre_suscripcion, max_contenidos_mensuales, fecha_creacion, fecha_actualizacion
)
VALUES (
    'NA',
    -1,
    'No Aplica',
    0,
    TIMESTAMP '1900-01-01',
    TIMESTAMP '1900-01-01'
);

-- Cargar datos iniciales de subscriptions_historical
INSERT INTO dim_suscripciones (
    id_dim_suscripcion, id_suscripcion, nombre_suscripcion, max_contenidos_mensuales, fecha_creacion, fecha_actualizacion
)
SELECT
    md5(CAST(subscription_id AS VARCHAR) || subscription_name || CAST(max_contents_per_month AS VARCHAR) || CAST(created_at AS VARCHAR) || CAST(updated_at AS VARCHAR)) AS id_dim_suscripcion,
    subscription_id AS id_suscripcion,
    subscription_name AS nombre_suscripcion,
    max_contents_per_month AS max_contenidos_mensuales,
    created_at AS fecha_creacion,
    updated_at AS fecha_actualizacion
FROM
    subscriptions_historical;

BEGIN TRANSACTION;

-- Actualizar registros existentes en dim_suscripciones
UPDATE dim_suscripciones
SET
    id_dim_suscripcion = md5(CAST(daily.subscription_id AS VARCHAR) || daily.subscription_name || CAST(daily.max_contents_per_month AS VARCHAR) || CAST(daily.created_at AS VARCHAR) || CAST(daily.updated_at AS VARCHAR)),
    nombre_suscripcion = daily.subscription_name,
    max_contenidos_mensuales = daily.max_contents_per_month,
    fecha_creacion = daily.created_at,
    fecha_actualizacion = daily.updated_at
FROM
    subscriptions_daily AS daily
WHERE
    dim_suscripciones.id_suscripcion = daily.subscription_id
    AND (
        dim_suscripciones.nombre_suscripcion <> daily.subscription_name OR
        dim_suscripciones.max_contenidos_mensuales <> daily.max_contents_per_month OR
        dim_suscripciones.fecha_actualizacion <> daily.updated_at
    );

-- Insertar nuevos registros que no existen en dim_suscripciones
INSERT INTO dim_suscripciones (
    id_dim_suscripcion, id_suscripcion, nombre_suscripcion, max_contenidos_mensuales, fecha_creacion, fecha_actualizacion
)
SELECT
    md5(CAST(daily.subscription_id AS VARCHAR) || daily.subscription_name || CAST(daily.max_contents_per_month AS VARCHAR) || CAST(daily.created_at AS VARCHAR) || CAST(daily.updated_at AS VARCHAR)) AS id_dim_suscripcion,
    daily.subscription_id AS id_suscripcion,
    daily.subscription_name AS nombre_suscripcion,
    daily.max_contents_per_month AS max_contenidos_mensuales,
    daily.created_at AS fecha_creacion,
    daily.updated_at AS fecha_actualizacion
FROM
    subscriptions_daily AS daily
LEFT JOIN
    dim_suscripciones AS dim
ON
    daily.subscription_id = dim.id_suscripcion
WHERE
    dim.id_suscripcion IS NULL;

COMMIT;

-- Validar datos en la dimensión
SELECT
    id_suscripcion,
    id_dim_suscripcion,
    nombre_suscripcion,
    max_contenidos_mensuales,
    fecha_creacion,
    fecha_actualizacion
FROM
    dim_suscripciones;
