--MENU DE LA APLICACION
WITH RECURSIVE nivelMax AS (
    SELECT sh.id_accion, 1 AS nivel
        FROM acciones sh
        WHERE sh.id_padre IS NULL
    UNION ALL
    SELECT sh.id_accion, nivel + 1 AS nivel
        FROM nivelMax n
        INNER JOIN acciones sh 
            ON sh.id_padre = n.id_accion
)
SELECT json_agg(
    json_build_object(
        'id', a.id_accion,
        'id_padre', a.id_padre,
        'nombre', a.nombre,
        'path', a.path,
        'icono', a.icono,
        'orden', a.orden,
        'hijos', get_actions_arbol_user((SELECT MAX(nivel) FROM nivelMax) ,a.id_accion ,u.id_usuario::int ,ea.id_empresa))
        ORDER BY a.orden
    ) AS acciones
    FROM usuarios u
    INNER JOIN perfiles p 
        ON u.id_perfil = p.id_perfil
    INNER JOIN perfiles_acciones pa 
        ON pa.id_perfil = p.id_perfil
    INNER JOIN acciones a 
        ON a.id_accion = pa.id_accion
    INNER JOIN empresas e 
        ON e.id_empresa = :p_id_empresa
    AND e.estado = TRUE 
 INNER JOIN empresa_accion ea 
  ON ea.id_accion = a.id_accion 
  AND ea.id_empresa = e.id_empresa 
 WHERE u.id_usuario = :p_id_usuario
  AND a.id_padre IS NULL 
  AND a.estado 