DROP TABLE IF EXISTS fact_creacion_contenido;

CREATE TABLE fact_creacion_contenido (
    id_dim_contenido VARCHAR NOT NULL,      -- Clave foránea hacia dim_contenidos
    id_dim_cuenta VARCHAR NOT NULL,         -- Clave foránea hacia dim_cuentas
    id_dim_tiempo_dia INT NOT NULL,         -- Clave foránea hacia dim_tiempo_dia
    PRIMARY KEY (id_dim_contenido,id_dim_cuenta, id_dim_tiempo_dia), -- Combinación única
    FOREIGN KEY (id_dim_contenido) REFERENCES dim_contenidos(id_dim_contenido),
    FOREIGN KEY (id_dim_cuenta) REFERENCES dim_cuentas(id_dim_cuenta),
    FOREIGN KEY (id_dim_tiempo_dia) REFERENCES dim_tiempo_dia(id_dim_tiempo_dia)
);

-- carga historica
INSERT INTO fact_creacion_contenido (
    id_dim_contenido,
    id_dim_cuenta,
    id_dim_tiempo_dia
)
SELECT
    coalesce(dc.id_dim_contenido,'NA') as id_dim_contenido,
    coalesce(dcu.id_dim_cuenta,'NA') as id_dim_cuenta,
    CAST(strftime(ch.created_at, '%Y%m%d') AS INT) AS id_dim_tiempo_dia
FROM
    contents_historical ch
LEFT JOIN
    dim_contenidos dc
    ON dc.id_contenido = ch.content_id
    AND ch.created_at::date BETWEEN dc.valido_desde AND dc.valido_hasta -- Calce en periodo de validez
LEFT JOIN
    dim_cuentas dcu
    ON dcu.id_cuenta = ch.account_id
    AND ch.created_at::date BETWEEN dcu.valido_desde AND dcu.valido_hasta -- Calce en periodo de validez
LEFT JOIN
    dim_tiempo_dia dt
    ON dt.id_dim_tiempo_dia = CAST(strftime(ch.created_at, '%Y%m%d') AS INT);

-- carga diaria
INSERT INTO fact_creacion_contenido (
    id_dim_contenido,
    id_dim_cuenta,
    id_dim_tiempo_dia
)
SELECT
    COALESCE(dc.id_dim_contenido, 'NA') AS id_dim_contenido,
    COALESCE(dcu.id_dim_cuenta, 'NA') AS id_dim_cuenta,
    CAST(strftime(cd.created_at, '%Y%m%d') AS INT) AS id_dim_tiempo_dia
FROM
    contents_daily cd
LEFT JOIN
    dim_contenidos dc
    ON dc.id_contenido = cd.content_id
    AND cd.created_at::date BETWEEN dc.valido_desde AND dc.valido_hasta -- Calce en periodo de validez
LEFT JOIN
    dim_cuentas dcu
    ON dcu.id_cuenta = cd.account_id
    AND cd.created_at::date BETWEEN dcu.valido_desde AND dcu.valido_hasta -- Calce en periodo de validez
LEFT JOIN
    dim_tiempo_dia dt
    ON dt.id_dim_tiempo_dia = CAST(strftime(cd.created_at, '%Y%m%d') AS INT)
WHERE
    CAST(strftime(cd.created_at, '%Y%m%d') AS INT) > (
        SELECT COALESCE(MAX(id_dim_tiempo_dia), 0) -- Obtener el máximo id_dim_tiempo_dia existente
        FROM fact_creacion_contenido
    );
