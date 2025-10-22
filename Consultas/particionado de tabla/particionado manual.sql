-- 1. tomar el DDL de la tabla a particionar y se crea una tabla con nombre diferente
CREATE TABLE public.eventos_particionado (
    idvehiculo varchar(25) NOT NULL,
    evento varchar(5) NOT NULL,
    fecha timestamp NOT NULL,
    velocidad varchar(10) NULL,
    direccion varchar(100) NULL,
    latitud varchar(50) NULL,
    longitud varchar(50) NULL,
    xpos numeric(16, 8) NULL,
    ypos numeric(16, 8) NULL,
    municipio varchar(30) NULL,
    departamento varchar(30) NULL,
    idevento3 int4 NULL,
    idgeocerca int8 NULL,
    idpunto int8 NULL,
    indicegeocerca int4 DEFAULT 0 NULL,
    fecha_insercion timestamp DEFAULT now() NOT NULL,
    idconductor int8 NULL,
    idprogramacion int8 NULL,
    idevento int8 DEFAULT nextval('eventosnew_idevento_seq'::regclass) NULL,
    rumbo int8 NULL,
    CONSTRAINT eventos_progvehiculos_fkey FOREIGN KEY (idprogramacion) REFERENCES public.progvehiculos(idprogramacion)
)
PARTITION BY RANGE (fecha_insercion);  -- al final de la tabla de debe agregar esta linea para indicar por el campo que se va a particionar


-- 2. crear las particiones por rango de 1 mes    
DO $$
DECLARE
    fecha_inicio date := '2023-06-01';
    fecha_fin date := '2027-01-01';
    mes date;
BEGIN
    mes := fecha_inicio;
    WHILE mes < fecha_fin LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS public.eventos_%s PARTITION OF public.eventos_particionado FOR VALUES FROM (%L) TO (%L);',
            to_char(mes, 'YYYY_MM'),
            mes,
            mes + interval '1 month'
        );
        mes := mes + interval '1 month';
    END LOOP;
END$$;


-- 3. tener en cuenta la cantidad a particionar en este caso fueron 722.737.973 registros
SELECT count(1)
    FROM eventos e;


-- 4. realizar conteo por mes para llevar el control
SELECT to_char(date_trunc('month', fecha_insercion), 'YYYY-MM') AS mes ,date_trunc('month', fecha_insercion) AS fecha_inicio
        ,(date_trunc('month', fecha_insercion) + interval '1 month') AS fecha_fin ,COUNT(*) AS cantidad
    FROM eventos
    WHERE fecha_insercion >= '2025-02-01 00:00:00'
        AND fecha_insercion < '2025-03-01 00:00:00'
GROUP BY 1, 2, 3
ORDER BY fecha_inicio;


-- 5. para que el cargue sea rapido deshabilitar Write-Ahead Log (WAL), ya que seria un cargue masivo de datos
ALTER TABLE eventos_particionado SET UNLOGGED;


-- 6. ejecutar la función para migrar los datos de la tabla original a la tabla particionada
/*********************************************************************************************************************************************************************
*   teneiendo en cuenta la capacidad de la BD donde se esta trabajando y que la BD esta constantemente ingresando datos en la tabla se creo este función manual
*   para ingresar un rango de fecha mensual y que este inserte por día para no bloquear la tabla y otras funciones sigan trabajando se pausa la ejecucción por  
*   1 segundo con la linea "PERFORM pg_sleep(1);"
*********************************************************************************************************************************************************************/
DO $$
DECLARE
    -- rango a particionar este dependera del volumen de la tabla en este caso se realizo por mes
    dia_inicio DATE := '2025-09-01';
    dia_fin DATE := '2025-10-01';  -- límite exclusivo
    cant_insertadas BIGINT;
BEGIN
    RAISE NOTICE '==> Iniciando inserción diaria, rango: % a %, hora: %', dia_inicio, dia_fin, clock_timestamp();

    WHILE dia_inicio < dia_fin LOOP
        RAISE NOTICE '----> Insertando día: %, hora inicio: %', dia_inicio, clock_timestamp();

        INSERT INTO eventos_particionado
            SELECT *
                FROM eventos
                WHERE fecha_insercion >= dia_inicio
                    AND fecha_insercion < dia_inicio + INTERVAL '1 day';

        GET DIAGNOSTICS cant_insertadas = ROW_COUNT;

        RAISE NOTICE '----> Día % insertado, filas: %, hora fin: %', dia_inicio, cant_insertadas, clock_timestamp();

        dia_inicio := dia_inicio + INTERVAL '1 day';
        PERFORM pg_sleep(1);
    END LOOP;

    RAISE NOTICE '==> Inserción diaria completada hasta %, hora: %', dia_fin, clock_timestamp();
END $$;


-- 6. volver activar el Write-Ahead Log (WAL) y mantenga su comportamiento normal
ALTER TABLE eventos_particionado SET LOGGED;


-- 7. ejecutar analyze para optimizar la tabla particionada al final todo el cargue, este demorara segun la cantidad de datos que tenga la tabla
ANALYZE eventos_particionado;


-- 8. validar los datos migrados conincida con el conteo inicial
SELECT to_char(date_trunc('month', fecha_insercion), 'YYYY-MM') AS mes ,date_trunc('month', fecha_insercion) AS fecha_inicio
        ,(date_trunc('month', fecha_insercion) + interval '1 month') AS fecha_fin ,COUNT(*) AS cantidad
    FROM eventos_particionado
GROUP BY 1, 2, 3
ORDER BY fecha_inicio;


-- 9. por ultimo renombrar tablas para empezar a usar la tabla particionada
ALTER TABLE eventos RENAME TO eventos_old;
ALTER TABLE eventos_particionado RENAME TO eventos;