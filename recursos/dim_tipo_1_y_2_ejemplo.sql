-- Crear tabla temporal para identificar cambios en la dimensión de productos
 DROP TABLE IF EXISTS tmp_product_dim_stg;
 CREATE TABLE tmp_product_dim_stg AS
 SELECT
    src.producto_id,                          -- Identificador único del producto en el sistema fuente
    src.descripcion,                          -- Descripción del producto (SCD Tipo 1: sobrescritura directa)
    src.categoria,                            -- Categoría del producto (SCD Tipo 2: mantiene historial)
    trg.id_dim_productos,                     -- Clave sustituta (surrogate key) de la dimensión destino
  trg.categoria AS old_categoria,           -- Categoría previa en la dimensión destino (para
 comparación)
    trg.descripcion AS old_descripcion,       -- Descripción previa en la dimensión destino (para
 comparación)
    
    -- Definir el tipo de acción para la fila (Insertar, Actualizar o Sin cambios)
    CAST(CASE 
        WHEN trg.id_dim_productos IS NULL THEN 'I'   -- Insertar nueva fila si no existe en la dimensión
 destino
        WHEN trg.id_dim_productos IS NOT NULL AND (
            -- Detectar cambios en los valores clave
            COALESCE(src.descripcion, 'UNKNOWN') <> COALESCE(trg.descripcion, 'UNKNOWN') OR
            COALESCE(src.categoria, 'UNKNOWN')   <> COALESCE(trg.categoria, 'UNKNOWN')
        ) THEN 'U'   -- Actualizar si hay cambios en descripción o categoría
        ELSE 'S'     -- Sin cambios si los valores coinciden
    END AS CHAR(1)) AS upd_flag,
    -- Identificar el tipo de cambio (Tipo 1 o Tipo 2)
    CAST(CASE 
        WHEN COALESCE(src.categoria, 'UNKNOWN') <> COALESCE(trg.categoria, 'UNKNOWN') THEN
 '2' -- Cambio SCD Tipo 2
        ELSE '1'  -- Cambios en atributos de Tipo 1
    END AS CHAR(1)) AS scd_type,
    CURRENT_DATE AS valid_from,               -- Fecha de inicio de validez de la nueva fila
    '9999-12-31' AS valid_to                  -- Fecha de expiración predeterminada para filas activas
 FROM stg.stg_productos src
 LEFT JOIN dw.dim_productos trg
    ON src.producto_id = trg.producto_id
    AND trg.valid_now = 1;                    -- Solo considerar filas actuales en la dimensión destino
 
 BEGIN TRANSACTION;-- Actualización de SCD Tipo 1 (descripcion)
 UPDATE dim_productos
 SET 
    descripcion      = stg.descripcion     -- Actualizar descripción directamente
 FROM tmp_product_dim_stg stg
 WHERE 
    stg.upd_flag = 'U' AND stg.scd_type = '1'  -- Cambios de tipo 1
    AND dim_productos.producto_id = stg.producto_id;
    
-- Expirar versiones actuales para SCD Tipo 2 (categoria)
 UPDATE dim_productos
 SET 
    valid_to     = CURRENT_DATE - INTERVAL '1 DAY', -- Expirar versión actual
    valid_now    = '0'
 FROM tmp_product_dim_stg stg
 WHERE 
    dim_productos.producto_id = stg.producto_id 
    AND dim_productos.valid_now = '1'               -- Solo registros activos
    AND stg.upd_flag = 'U' AND stg.scd_type = '2';  
    
-- Cambios de tipo 2
-- Insertar nuevos registros y versiones SCD Tipo 2
 INSERT INTO dim_productos (
    producto_id,
    descripcion,
    categoria,
    valid_from,
    valid_to,
    valid_now
 )
 SELECT
    stg.producto_id,
    stg.descripcion,                    -- Atributo actualizado o sin cambios
    stg.categoria,                      -- Nueva versión de categoría
    CURRENT_DATE,                       -- Nueva fecha de inicio de validez
    '9999-12-31',                       -- Fecha de expiración predeterminada
    '1'                                -- Nueva versión activa
 FROM tmp_product_dim_stg stg
 WHERE 
    stg.upd_flag = 'I'                  -- Nuevas filas
    OR (stg.upd_flag = 'U' AND stg.scd_type = '2'); -- Nuevas versiones de Tipo 2
 COMMIT