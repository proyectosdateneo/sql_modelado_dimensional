-- Crear tabla dim_contenidos
DROP TABLE IF EXISTS dim_contenidos;

CREATE TABLE dim_contenidos (
    id_dim_contenido VARCHAR NOT NULL PRIMARY KEY,
    id_contenido BIGINT,
    titulo VARCHAR NOT NULL,
    descripcion TEXT,
    tipo_contenido VARCHAR NOT NULL,
    categoria VARCHAR,
    duracion INT,
    fecha_creacion TIMESTAMP,
    fecha_actualizacion TIMESTAMP,
    valido_desde DATE NOT NULL,
    valido_hasta DATE,
    es_actual BOOLEAN NOT NULL
);

COMMENT ON COLUMN dim_contenidos.id_dim_contenido IS 'Clave subrogada generada como un hash único para identificar de manera única cada registro en la dimensión, basada en múltiples atributos del contenido. Permite la identificación y vinculación con tablas de hechos';
COMMENT ON COLUMN dim_contenidos.id_contenido IS 'Clave natural del contenido, proveniente del sistema fuente.';
COMMENT ON COLUMN dim_contenidos.titulo IS 'Título del contenido, obtenido directamente de la tabla contents, que puede ser utilizado para búsquedas y visualización en reportes (SCD Tipo 1)';
COMMENT ON COLUMN dim_contenidos.descripcion IS 'Descripción detallada del contenido, que proporciona contexto adicional y puede ser utilizado en análisis de texto o para enriquecer la información presentada (SCD Tipo 1)';
COMMENT ON COLUMN dim_contenidos.tipo_contenido IS 'Tipo de contenido, como video, artículo, etc., que puede cambiar con el tiempo y requiere un seguimiento histórico (SCD Tipo 2)';
COMMENT ON COLUMN dim_contenidos.categoria IS 'Categoría del contenido, que clasifica el contenido en diferentes grupos temáticos y puede cambiar con el tiempo, requiriendo un seguimiento histórico (SCD Tipo 2). Proviene de la tabla content_attributes';
COMMENT ON COLUMN dim_contenidos.duracion IS 'Duración del contenido en minutos, que puede ser relevante para análisis de consumo y planificación de tiempo, y puede cambiar con el tiempo (SCD Tipo 2). Proviene de la tabla content_attributes';
COMMENT ON COLUMN dim_contenidos.fecha_creacion IS 'Fecha en la que el contenido fue creado originalmente en el sistema fuente, utilizada para análisis históricos y auditorías';
COMMENT ON COLUMN dim_contenidos.fecha_actualizacion IS 'Última fecha en la que el contenido fue actualizado en el sistema fuente, utilizada para identificar cambios recientes y mantener la información actualizada';
COMMENT ON COLUMN dim_contenidos.valido_desde IS 'Fecha de inicio de validez del registro en la dimensión, utilizada para gestionar la historia de cambios en los atributos del contenido (SCD Tipo 2)';
COMMENT ON COLUMN dim_contenidos.valido_hasta IS 'Fecha de fin de validez del registro en la dimensión, utilizada para gestionar la historia de cambios en los atributos del contenido (SCD Tipo 2)';
COMMENT ON COLUMN dim_contenidos.es_actual IS 'Indicador booleano que señala si el registro es la versión actual del contenido, utilizado para facilitar consultas y análisis de la versión vigente';

-- Insertar registros iniciales usando contents_historical y content_attributes_historical
INSERT INTO dim_contenidos (
    id_dim_contenido, id_contenido, titulo, descripcion, tipo_contenido, categoria, duracion, fecha_creacion, fecha_actualizacion, valido_desde, valido_hasta, es_actual
)
SELECT
    md5(CAST(c.content_id AS VARCHAR) || c.title || c.description || c.content_type || COALESCE(ca1.string_value, '') || CAST(COALESCE(ca2.decimal_value, -1) AS VARCHAR) || CAST(c.created_at AS VARCHAR) || CAST(c.updated_at AS VARCHAR)) AS id_dim_contenido,
    c.content_id AS id_contenido,
    c.title AS titulo,
    c.description AS descripcion,
    c.content_type AS tipo_contenido,
    ca1.string_value AS categoria, -- Atributo "Categoría"
    ca2.decimal_value AS duracion, -- Atributo "Duración"
    c.created_at AS fecha_creacion,
    c.updated_at AS fecha_actualizacion,
    '1900-01-01' AS valido_desde,
    '9999-12-31' AS valido_hasta,
    TRUE AS es_actual
FROM
    contents_historical c
LEFT JOIN
    content_attributes_historical ca1
ON
    c.content_id = ca1.content_id AND ca1.attribute_name = 'Categoría'
LEFT JOIN
    content_attributes_historical ca2
ON
    c.content_id = ca2.content_id AND ca2.attribute_name = 'Duración'
UNION ALL
SELECT
    'NA', NULL, 'No Aplica', NULL, 'NA', NULL, NULL, '1900-01-01', '1900-01-01', '1900-01-01', '9999-12-31', TRUE
;

-- Crear tabla temporal para detectar cambios en la dimensión
DROP TABLE IF EXISTS tmp_dim_contenidos_stg;
CREATE TABLE tmp_dim_contenidos_stg AS
SELECT
    COALESCE(cd.content_id, ca.content_id) AS content_id, -- Unión de cambios en contenido y atributos
    COALESCE(cd.title, d.titulo) AS title,               -- Título (priorizar contenido diario si existe)
    COALESCE(cd.description, d.descripcion) AS description, -- Descripción
    COALESCE(cd.content_type, d.tipo_contenido) AS content_type, -- Tipo de contenido
    MAX(CASE WHEN ca.attribute_name = 'Categoría' THEN ca.string_value ELSE d.categoria END) AS categoria, -- Categoría
    MAX(CASE WHEN ca.attribute_name = 'Duración' THEN ca.decimal_value ELSE d.duracion END) AS duracion, -- Duración
    md5(CAST(COALESCE(cd.content_id, ca.content_id) AS VARCHAR) ||
        COALESCE(cd.title, d.titulo, 'NA') ||
        COALESCE(cd.description, d.descripcion, 'NA') ||
        COALESCE(cd.content_type, d.tipo_contenido, 'NA') ||
        COALESCE(MAX(CASE WHEN ca.attribute_name = 'Categoría' THEN ca.string_value END), d.categoria, 'NA') ||
        COALESCE(CAST(MAX(CASE WHEN ca.attribute_name = 'Duración' THEN ca.decimal_value END) AS VARCHAR), CAST(d.duracion AS VARCHAR), 'NA')) AS id_dim_contenido,
    d.id_dim_contenido AS existing_id_dim_contenido,
    CASE 
        WHEN d.id_dim_contenido IS NULL THEN 'I' -- Nuevo contenido
        WHEN COALESCE(cd.title, d.titulo, 'NA') <> d.titulo
             OR COALESCE(cd.description, d.descripcion, 'NA') <> d.descripcion
             OR COALESCE(cd.content_type, d.tipo_contenido, 'NA') <> d.tipo_contenido
             OR MAX(CASE WHEN ca.attribute_name = 'Categoría' THEN ca.string_value ELSE d.categoria END) <> d.categoria
             OR MAX(CASE WHEN ca.attribute_name = 'Duración' THEN ca.decimal_value ELSE d.duracion END) <> d.duracion
        THEN 'U' -- Actualización
        ELSE 'S' -- Sin cambios
    END AS upd_flag,
    CASE 
        WHEN COALESCE(cd.content_type, d.tipo_contenido, 'NA') <> d.tipo_contenido
             OR MAX(CASE WHEN ca.attribute_name = 'Categoría' THEN ca.string_value ELSE d.categoria END) <> d.categoria
             OR MAX(CASE WHEN ca.attribute_name = 'Duración' THEN ca.decimal_value ELSE d.duracion END) <> d.duracion
        THEN '2' -- SCD Tipo 2
        ELSE '1' -- SCD Tipo 1
    END AS scd_type,
    COALESCE(cd.updated_at, MAX(ca.created_at)) AS updated_at -- Fecha de última modificación
FROM content_attributes_daily ca
FULL OUTER JOIN contents_daily cd
ON ca.content_id = cd.content_id
LEFT JOIN dim_contenidos d
ON COALESCE(cd.content_id, ca.content_id) = d.id_contenido
   AND d.es_actual = TRUE
GROUP BY 
    COALESCE(cd.content_id, ca.content_id),
    cd.title,
    cd.description,
    cd.content_type,
    d.titulo,
    d.descripcion,
    d.tipo_contenido,
    d.categoria,
    d.duracion,
    d.id_dim_contenido,
    cd.updated_at,
    d.fecha_actualizacion;

BEGIN TRANSACTION;

-- Actualización SCD Tipo 1 (titulo, descripcion)
UPDATE dim_contenidos
SET
    titulo = stg.title,
    descripcion = stg.description,
    fecha_actualizacion = stg.updated_at
FROM tmp_dim_contenidos_stg stg
WHERE
    stg.upd_flag = 'U' AND stg.scd_type = '1'
    AND dim_contenidos.id_contenido = stg.content_id;

-- Expirar versiones actuales para SCD Tipo 2 (tipo_contenido, categoria, duracion)
UPDATE dim_contenidos
SET
    valido_hasta = cast(stg.updated_at - INTERVAL '1 day' as date), -- si estuviesemos ejecutando un proceso diario, podemos usar current_date
    es_actual = FALSE
FROM tmp_dim_contenidos_stg stg
WHERE
    stg.upd_flag = 'U' AND stg.scd_type = '2'
    AND dim_contenidos.id_contenido = stg.content_id;

-- Insertar nuevos registros para cambios SCD Tipo 2
INSERT INTO dim_contenidos (
    id_dim_contenido, id_contenido, titulo, descripcion, tipo_contenido, categoria, duracion, fecha_creacion, fecha_actualizacion, valido_desde, valido_hasta, es_actual
)
SELECT
    stg.id_dim_contenido,
    stg.content_id,
    stg.title,
    stg.description,
    stg.content_type,
    stg.categoria,
    stg.duracion,
    NULL AS fecha_creacion,  -- No se modifica la fecha de creación
    stg.updated_at AS fecha_actualizacion,
    stg.updated_at::date AS valido_desde, -- si estuviesemos ejecutando un proceso diario, podemos usar current_date
    '9999-12-31' AS valido_hasta,
    TRUE AS es_actual
FROM tmp_dim_contenidos_stg stg
WHERE stg.upd_flag = 'I' OR (stg.upd_flag = 'U' AND stg.scd_type = '2');

COMMIT;