-- DROP FUNCTION kagua.interfaz_sap_clientes();

CREATE OR REPLACE FUNCTION kagua.interfaz_sap_clientes()
 RETURNS SETOF datos_xml
 LANGUAGE plpgsql
AS $function$
DECLARE
/***************************************************************************************************************************************
* AUTOR: JOSÉ ARMENTA
* FECHA ELABORACIÓN: 2023-12-12
* MODIFICACIONES: 
*       2024-01-04 JOSÉ ARMENTA: SE AJUSTA LA VLAIDACIÓN PARA EL LARGO QUE ACEPTA EL CAMPO DE XML Y SE SEPARA EN DOS CAMPOS
*           LA INFORMACIÓN A ENVIAR
*       2024-01-12 JOSÉ ARMENTA: SE AGREGA LPAD PARA ANTEPONER EL NUMERO "0" EN LOS CODIGOS DE DEPARTAMENTOS QUE POSEAN UN SOLO CARACTER
*       2025-09-17 JAM: SE AGREGA CONSULTA PARA SOLO TENER EN CUENTA LOS CLIENTES QUE PÉRTENECEN AL SERVICIO DE HOGAR REDES.
* DESCRIPCIÓN: 
*       ESTA FUNCIÓN EXTRAE LOS DATOS DE LOS CLIENTE RECIEN CREADOS O QUE SE LE REALIZO ALGUNA MODIFICACIÓN, PARA
        SER ENVIADOS A SAP.
***************************************************************************************************************************************/
    -- declaración de variables  
    datos_cur record; 
    tipo_doc_cli VARCHAR(3);
    tipo_persona VARCHAR(1);
    longitud_dir INT;
    longitud_maxima INT := 60; -- establece la longitud máxima
    direccion_ori TEXT;
    direccion TEXT;
    direccion_compl TEXT;
    longitud_tlf INT;
    longitud_max_tlf INT := 7; -- establece la longitud máxima
    tlf_fijo TEXT;
    celular TEXT;
    tipo_doc TEXT;
    tipo_doc_sap TEXT;
    declara TEXT;
    imp_fact_electr TEXT;
    nro_documento_new TEXT;
    longitudNombre INT;
    longitudMaxNomb INT;
    nombre_completo TEXT;
    nombre1 TEXT;
    nombre2 TEXT;
    nombre3 TEXT;
    nombre4 TEXT;
    salida datos_xml;

    -- declaración del cursor
    cursor_ppal CURSOR FOR 
        SELECT c.cod_clte, c.nro_documento ,c.nombre ,c.apellido ,c.cod_tdoc ,c.nro_documento 
                ,CASE WHEN c.segundo_nombre IS NULL THEN '' ELSE c.segundo_nombre END AS segundo_nombre
                ,CASE WHEN c.segundo_apellido IS NULL THEN '' ELSE c.segundo_apellido END AS segundo_apellido
                ,c.direccion ,LPAD(c.cod_departamento::varchar,2,'0') AS cod_departamento ,c.telefono ,td.perjur ,m.cod_munip 
                ,m.descripcion as nomb_municipio ,c.email ,ov.centro_operativo ,ov.zona_venta 
                ,ov.organ_venta ,ov.ofic_venta ,ov.grupo_vendedor ,c.habeas_data  ,c.digito_verificacion ,t.cod_tratamiento as tratamiento
            FROM cliente c
            INNER JOIN tipo_doc td
                ON td.cod_tdoc = c.cod_tdoc
                AND td.cod_empr = c.cod_empr 
            INNER JOIN municipio m
                ON m.cod_munip = c.cod_munip 
                AND m.cod_empr = c.cod_empr 
            INNER JOIN organizacion_venta ov 
                ON ov.cod_empr = c.cod_empr 
                AND ov.cod_munip = c.cod_munip 
            INNER JOIN tratamiento t 
                ON t.cod_tratamiento = c.cod_tratamiento
            --WHERE cod_clte = 13869;
            WHERE (c.cod_sap is null 
                OR c.enviado_sap = 'N')
                AND c.cod_clte IN (
                     -- 2025-09-17 JAM: SE AGREGA CONSULTA PARA SOLO TENER EN CUENTA LOS CLIENTES QUE PÉRTENECEN AL SERVICIO DE HOGAR REDES.
                     SELECT cs.cod_clte
                        FROM servicio_tipo_cliente st 
                        INNER JOIN cliente_servicios cs 
                            ON cs.id_tipo_cliente = st.id
                        WHERE st.id = 1  
                );
       
BEGIN
    OPEN cursor_ppal; 

        LOOP
            FETCH cursor_ppal INTO datos_cur; 
            EXIT WHEN NOT FOUND; 
        
            -- declaración variables internas
            longitud_dir := LENGTH(datos_cur.direccion);
            direccion_ori := datos_cur.direccion;
            longitud_tlf := LENGTH(datos_cur.telefono);
            tipo_doc := datos_cur.cod_tdoc;
        
        
            -- validación para tipo de documentos CO3 para: "Tarj. Identidad o Certif. Extranjeria", para el resto CO1
            IF datos_cur.cod_tdoc = '2' OR datos_cur.cod_tdoc = '5' THEN
                tipo_doc_cli := 'CO3';
            ELSE
                tipo_doc_cli := 'CO1';
            END IF;
           
            -- validación persona naturarl marca con X, persona juridica dejar vacio
            IF datos_cur.perjur = 'N' THEN
                tipo_persona = 'X';
            ELSE
                tipo_persona = '';
            END IF;
        
            -- validar largo de la direccion
            IF longitud_dir > longitud_maxima THEN
                -- Obtiene los primeros 60 caracteres
                direccion := LEFT(direccion_ori, longitud_maxima);
                -- Obtiene los caracteres restantes
                direccion_compl := RIGHT(direccion_ori, longitud_dir - longitud_maxima);
            ELSE
                direccion := direccion_ori;
                direccion_compl := '';
            END IF;
        
            -- identificar si telefono es fijo o celular
            IF longitud_tlf <= longitud_max_tlf THEN
                tlf_fijo := datos_cur.telefono;
                celular := '';
            ELSE
                tlf_fijo := '';
                celular := datos_cur.telefono;
            END IF;
        
            -- tipo de documento para SAP
            IF tipo_doc = '1' THEN
                tipo_doc_sap := '13'; -- Cedula Ciudadania
            ELSIF tipo_doc = '2' THEN
                tipo_doc_sap := '22'; -- Certif. Extranjeria
            ELSIF tipo_doc = '3' THEN
                tipo_doc_sap := '31'; -- Nit
            ELSIF tipo_doc = '4' THEN
                tipo_doc_sap := '11'; -- Registro Civil
            ELSIF tipo_doc = '5' THEN
                tipo_doc_sap := '12'; -- Tarj. Identidad
            END IF;
        
            --NIT si declara y factura electronica
            IF datos_cur.cod_tdoc = '3' THEN
                declara := 'SI'; -- si declara
                imp_fact_electr := '01'; -- factura electronica
                nro_documento_new =  datos_cur.nro_documento||datos_cur.digito_verificacion;
                nombre_completo = RTRIM(datos_cur.nombre||' '||datos_cur.segundo_nombre||' '||datos_cur.apellido||' '||datos_cur.segundo_apellido);
                longitudNombre := LENGTH(nombre_completo);
                longitudMaxNomb = 35; -- Establece la longitud máxima
                IF longitudNombre > longitudMaxNomb THEN
                    -- Obtiene los primeros 35 caracteres
                    nombre1 := LEFT(nombre_completo, longitudMaxNomb);
                    -- Obtiene los caracteres restantes
                    nombre2 := RIGHT(nombre_completo, longitudNombre - longitudMaxNomb);
                    -- Obtiene los primeros 35 caracteres
                    nombre3 := LEFT(nombre_completo, longitudMaxNomb);
                    -- Obtiene los caracteres restantes
                    nombre4 := RIGHT(nombre_completo, longitudNombre - longitudMaxNomb);
                ELSE
                    nombre1 := nombre_completo;
                    nombre2 := '';
                    nombre3 := nombre_completo;
                    nombre4 := '';
                END IF;
            ELSE 
                declara = 'NO'; -- no declara
                imp_fact_electr := 'ZZ'; -- factura electronica
                nro_documento_new = datos_cur.nro_documento;
                nombre_completo = RTRIM(datos_cur.nombre||' '||datos_cur.segundo_nombre||' '||datos_cur.apellido||' '||datos_cur.segundo_apellido);
                longitudNombre := LENGTH(nombre_completo);
                longitudMaxNomb = 35; -- Establece la longitud máxima
                IF longitudNombre > longitudMaxNomb THEN
                    -- Obtiene los primeros 35 caracteres
                    nombre1 := LEFT(nombre_completo, longitudMaxNomb);
                    -- Obtiene los caracteres restantes
                    nombre2 := RIGHT(nombre_completo, longitudNombre - longitudMaxNomb);
                    nombre3 := RTRIM(datos_cur.nombre||','||datos_cur.segundo_nombre);
                    nombre4 := RTRIM(datos_cur.apellido||','||datos_cur.segundo_apellido);
                ELSE
                    -- Obtiene los primeros 35 caracteres
                    nombre1 := LEFT(nombre_completo, longitudMaxNomb);
                    nombre2 := '';
                    nombre3 := RTRIM(datos_cur.nombre||','||datos_cur.segundo_nombre);
                    nombre4 := RTRIM(datos_cur.apellido||','||datos_cur.segundo_apellido);
                END IF;
            END IF;
           
            salida.nro_registro := datos_cur.cod_clte;
            salida.partn_cat := '2';
            salida.partn_grp := 'ZNAL';
            salida.sort1 := datos_cur.nro_documento;
            salida.title := datos_cur.tratamiento;
            salida.name1 := nombre1;
            salida.name2 := nombre2;
            salida.name3 := nombre3;
            salida.name4 := nombre4;
            salida.spras := 'ES';
            salida.partner_role1 := 'FLCU00';
            salida.partner_role2 := 'FLCU01';
            salida.taxtype := tipo_doc_cli;
            salida.taxNumber := nro_documento_new;
            salida.stkzn := tipo_persona;
            salida.source := 'SI';
            salida.ort01 := datos_cur.nomb_municipio;
            salida.Ort02 := '';
            salida.street := direccion;
            salida.str_suppl1 := direccion_compl;
            salida.country := 'CO';
            salida.region := datos_cur.cod_departamento;
            salida.telf1 := tlf_fijo;
            salida.telf2 := celular;
            salida.smtp_addr := datos_cur.email;
            salida.fityp := 'NA';
            salida.stcdt := tipo_doc_sap;
            salida.bukrs := '1100';
            salida.akont := '1104010003';
            salida.zterm := 'D030';
            salida.fdgrv := 'DU';
            salida.xzver := 'X';
            salida.gricd := '';
            salida.ciiu_code := '';
            salida.vkorg := datos_cur.organ_venta;
            salida.vtweg := '00';
            salida.spart := '00';
            salida.versg := '1';
            salida.kalks := '1';
            salida.kdgrp := '10';
            salida.bzirk := datos_cur.zona_venta;
            salida.vkbur := datos_cur.ofic_venta;
            salida.vkgrp := datos_cur.grupo_vendedor;
            salida.waers := 'COP';
            salida.ktgrd := '10';
            salida.prat1 := 'E1';
            salida.prat2 := '10';
            salida.prat3 := '';
            salida.prat4 := declara;
            salida.prat5 := imp_fact_electr;
            salida.prat6 := '';
            salida.vwerk := datos_cur.centro_operativo;
            salida.vsbed := '01';
            salida.perfk := 'CO';
            salida.zterm1 := 'D030';
            salida.aland := 'CO';
            salida.tatyp := 'MWST';
            salida.taxkd := '1';
            salida.kvgr1 := '30';
            salida.kvgr2 := '10';
            salida.kvgr3 := '710';
            salida.kvgr4 := '10';
            salida.kvgr5 := '';
            salida.kunnr_ve := '';
           
            RETURN NEXT salida;
        END LOOP;

    CLOSE cursor_ppal; 
    RETURN;
END;
$function$
;
