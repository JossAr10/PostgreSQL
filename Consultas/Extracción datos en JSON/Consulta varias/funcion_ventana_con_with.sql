-- FUNCIONES VENTANAS CON WITH
SELECT sum(df.valor_diferido) AS valor_diferido ,sum(df.valor_cuota) AS  valor_cuota ,greatest(df.cuotas,df.cuotas) AS cuotas ,sum(df.saldo) AS saldo
    FROM (
        WITH diferidos AS (
            SELECT dc2.cod_pred ,dc2.cod_dife ,sum(dc2.valor_total) OVER (PARTITION BY dc2.cod_dife ORDER BY dc2.cod_dife) AS total_diferido
                    ,dc2.valor_total AS val_cuota ,dc.cant_cuotas,dc2.aplicado 
                    ,count(*) FILTER (WHERE aplicado = 'S') OVER (PARTITION BY dc2.cod_dife ORDER BY dc2.cod_dife) AS cant_cuotas_fact
                    ,sum(dc2.valor_total) FILTER (WHERE aplicado = 'N') OVER (PARTITION BY dc2.cod_dife ORDER BY dc2.cod_dife) AS saldo_pend
                    ,dc.valor_cuota
                FROM diferido_cab dc
                INNER JOIN diferido_cuota dc2 
                    USING(cod_pred,cod_dife,cod_empr,cod_munip)
                WHERE cod_pred = $predio
                    AND dc.estado = 'A'	
        )
        SELECT sum(DISTINCT total_diferido ) AS valor_diferido ,sum(DISTINCT valor_cuota ) AS valor_cuota
                ,max(cant_cuotas_fact ||'/'|| cant_cuotas) OVER (PARTITION BY cod_pred) AS cuotas ,sum(DISTINCT saldo_pend ) AS saldo
            FROM diferidos
        GROUP BY cant_cuotas_fact ,cant_cuotas ,cod_pred
    ) df
GROUP BY df.cuotas