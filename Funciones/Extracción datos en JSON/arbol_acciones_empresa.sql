-- DROP FUNCTION seguridad.arbol_acciones_empresa(int4, int4, int4);

CREATE OR REPLACE FUNCTION seguridad.arbol_acciones_empresa(p_max_level integer, p_padre integer, p_empresa integer)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
DECLARE
    result JSON;
BEGIN
    WITH RECURSIVE cte_actions AS (
        -- PRIMER SELECT: nivel 1
        SELECT
            aprin.id_accion AS id,
            aprin.nombre,
            aprin.id_padre AS id_padre,
            aprin.orden,
            1 AS nivel,
            CASE 
                WHEN EXISTS (
                    SELECT 1 FROM empresa_accion ea 
                    WHERE ea.id_accion = aprin.id_accion 
                      AND ea.id_empresa = p_empresa
                ) THEN true
                ELSE false
            END AS estado
        FROM acciones aprin
        WHERE 1 < p_max_level
          AND (aprin.id_padre = p_padre OR (p_padre IS NULL AND aprin.id_padre IS NULL))
          AND aprin.estado

        UNION ALL

        -- SEGUNDO SELECT: niveles > 1
        SELECT
            a.id_accion AS id,
            a.nombre,
            a.id_padre AS id_padre,
            a.orden,
            p.nivel + 1 AS nivel,
            CASE 
                WHEN EXISTS (
                    SELECT 1 FROM empresa_accion ea 
                    WHERE ea.id_accion = a.id_accion 
                      AND ea.id_empresa = p_empresa
                ) THEN true
                ELSE false
            END AS estado
        FROM acciones a
        INNER JOIN cte_actions p ON a.id_padre = p.id
        WHERE p.nivel < p_max_level
          AND a.estado
    )

    SELECT json_agg(json_build_object(
        'id', acciones.id,
        'id_padre', acciones.id_padre,
        'nombre', acciones.nombre,
        'orden', acciones.orden,
        'estado', acciones.estado,
        'hijos', (
            SELECT arbol_acciones_empresa(p_max_level - 1, acciones.id, p_empresa)
        )
    ) ORDER BY acciones.orden)
    INTO result
    FROM cte_actions acciones
    WHERE acciones.nivel = 1;

    RETURN result;
END;
$function$
;
