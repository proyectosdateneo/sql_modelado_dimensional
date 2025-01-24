DROP TABLE IF EXISTS dim_tiempo_dia;

CREATE TABLE dim_tiempo_dia (
    id_dim_tiempo_dia INT PRIMARY KEY,    -- Clave primaria en formato yyyyMMdd
    fecha DATE NOT NULL,                  -- Fecha completa
    anio INT,                             -- Año
    mes INT,                              -- Mes
    dia INT,                              -- Día
    dia_semana VARCHAR,                   -- Día de la semana (ej. "Lunes")
    es_fin_de_semana BOOLEAN,             -- Indicador de fin de semana
    trimestre INT                         -- Trimestre del año
);

INSERT INTO dim_tiempo_dia (
    id_dim_tiempo_dia, fecha, anio, mes, dia, dia_semana, es_fin_de_semana, trimestre
)
SELECT
    CAST(strftime(fecha, '%Y%m%d') AS INT) AS id_dim_tiempo_dia, -- Clave primaria en formato yyyyMMdd
    fecha,
    EXTRACT(YEAR FROM fecha) AS anio,
    EXTRACT(MONTH FROM fecha) AS mes,
    EXTRACT(DAY FROM fecha) AS dia,
    strftime(fecha, '%w') AS dia_semana, -- Día de la semana como número (0 = Domingo, 6 = Sábado)
    CASE WHEN EXTRACT(DOW FROM fecha) IN (0, 6) THEN TRUE ELSE FALSE END AS es_fin_de_semana,
    CEIL(EXTRACT(MONTH FROM fecha) / 3.0) AS trimestre -- Trimestre del año
FROM (
    SELECT DATE '2010-01-01' + INTERVAL (x - 1) DAY AS fecha
    FROM range(1, (DATE '2030-12-31' - DATE '2010-01-01')::INT + 2) AS t(x)
) AS fechas;

