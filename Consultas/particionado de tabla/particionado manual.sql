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