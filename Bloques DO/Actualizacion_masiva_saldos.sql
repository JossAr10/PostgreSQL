DO $$
DECLARE
    registro record;
    conteo INTEGER;
    consecutivo INTEGER := 1;
    v_nuevo_valor numeric;
    v_ajuste_actual numeric;
    v_nuevo_sfv numeric;
    v_total_actual numeric;
    v_nro_seq numeric;
    v_confirmar_fact numeric;
    v_confirmar_sfv numeric;

    cur_ppal CURSOR FOR 
        WITH sfv AS (   
            SELECT sfc.cod_sfav ,sfc.cod_pred ,sfc.valor 
                    ,sum(sfd.valor_apl) AS valor_apl
                    ,sum(round(sfc.valor-COALESCE(sfd.valor_apl,0),2)) AS pend_x_aplicar
                FROM saldo_favor_cab sfc
                LEFT JOIN saldo_favor_det sfd 
                    ON sfd.cod_sfav = sfc.cod_sfav
                WHERE sfc.cod_pred IN (
                    203785,17641,17593,17611,18092,205200,17605,18041,18086,18125,18140,202528,202533,203775,17603,17610,17623,18081,18231,202424,202444,202642,203777,17618,18043,18066,18123,202527,202544,202633,202643,203790,17595,17620,17625,17636,17637,17638,17639,17642,17647,18072,18074,18082,18087,18088,18226,18229,202433,202436,202438,202443,202516,202525,202531,202536,202558,202629,202631,202641,203773,203791,17592,17617,17630,18042,18044,18046,18050,18063,18068,18090,18091,18098,18101,18102,18137,18228,18235,18238,18242,202434,202435,202512,202523,202526,202535,202612,202634,203789,17598,17601,17604,17609,17619,17626,17631,17632,17633,17646,18036,18037,18065,18073,18084,18089,18097,18129,18135,18224,18230,202431,202439,202515,202517,202534,202593,202605,202614,202630,202637,202638,202646,203782,203796,205201,17591,17594,17602,17607,17616,17622,17635,17640,17644,17645,18040,18049,18060,18067,18076,18080,18085,18094,18096,18099,18126,18133,18139,18244,202429,202440,202511,202514,202532,202615,202635,202644,202647,203779,203784,203786,204006,17599,17614,17628,17629,18048,18062,18075,18077,18100,18122,18130,18134,18138,18223,18233,18234,18236,202432,202510,202513,202520,202524,202529,202596,202606,202613,202628,202632,202960,203772,203787,203792,203795,17597,17612,17621,17624,17634,18069,18083,18093,18095,18124,18131,18232,202441,202522,202639,203774,203778,203788,204005,17600,17608,17613,17627,18038,18039,18045,18071,18237,202437,202616,202636,202640,203776,203780,203797,205199,205242,14874,13546,14214,13612,16864,13693,13577,13627,13592,13636,13641,13706,14240,14896,13540,13611,13624,14196,14202,14218,14243,14958,14960,14963,13513,13541,13551,13599,13610,13664,13695,14246,14858,14908,14910,14925,14934,14935,14950,13549,13594,13603,13609,13613,13671,13691,14199,14245,14869,14889,14894,14906,14914,14919,14946,14948,14957,14962,203764,13479,13530,13536,13538,13539,13543,13548,13550,13552,13596,13597,13608,13646,13647,13683,13696,13704,14195,14242,14866,14898,14929,14938,13483,13501,13503,13537,13553,13588,13590,13591,13595,13598,13602,13606,13618,13637,13640,13644,13658,13677,13700,13705,13707,14212,14217,14220,14224,14886,14899,14901,14904,14923,14924,14932,13495,13498,13512,13520,13535,13554,13576,13600,13607,13614,13620,13622,13639,13642,13645,13660,13662,13663,13665,13669,13672,13673,13674,13681,13684,13686,13687,13690,13692,13694,13702,14203,14204,14206,14207,14209,14222,14241,14861,14871,14872,14888,14895,14909,14920,14921,14922,14926,14927,14928,14937,14944,14945,14953,16863,203763,13505,13517,13519,13586,13593,13605,13626,13635,13643,13666,13668,13675,13676,13685,13688,13698,14201,14221,14227,14873,14900,14903,14905,14933,14951,205289,13492,13509,13525,13544,13601,13615,13638,13667,13670,13699,13701,14213,14854,14892,14897,14902,14907,14911,14913,14915,203765,205291,205292
                )
                    AND sfc.motivo = 'Peticiones #152093 Cargue valores compensación'
            GROUP BY sfc.cod_sfav ,sfc.cod_pred ,sfc.valor     
        )  
        , facturas AS (
            SELECT fd.cod_pred ,fd.nro_seq ,fd.cod_conc ,fd.descripcion ,fd.total_concp ,fd.vsf_conc ,fd.vad_conc 
                    ,sum(fd.total_concp+fd.vsf_conc+fd.vad_conc) OVER (PARTITION BY fd.cod_pred ,fd.nro_seq ,fd.cod_conc) AS valor_total
                    ,round(fd.total_concp,0)-fd.total_concp AS decimale_ajustar
                FROM factura_cab fc
                INNER JOIN factura_det fd
                    ON fd.cod_pred = fc.cod_pred
                    AND fd.cod_peri = fc.cod_peri
                WHERE fc.cod_peri = 202510
                    AND fc.cod_pred IN (
                        203785,17641,17593,17611,18092,205200,17605,18041,18086,18125,18140,202528,202533,203775,17603,17610,17623,18081,18231,202424,202444,202642,203777,17618,18043,18066,18123,202527,202544,202633,202643,203790,17595,17620,17625,17636,17637,17638,17639,17642,17647,18072,18074,18082,18087,18088,18226,18229,202433,202436,202438,202443,202516,202525,202531,202536,202558,202629,202631,202641,203773,203791,17592,17617,17630,18042,18044,18046,18050,18063,18068,18090,18091,18098,18101,18102,18137,18228,18235,18238,18242,202434,202435,202512,202523,202526,202535,202612,202634,203789,17598,17601,17604,17609,17619,17626,17631,17632,17633,17646,18036,18037,18065,18073,18084,18089,18097,18129,18135,18224,18230,202431,202439,202515,202517,202534,202593,202605,202614,202630,202637,202638,202646,203782,203796,205201,17591,17594,17602,17607,17616,17622,17635,17640,17644,17645,18040,18049,18060,18067,18076,18080,18085,18094,18096,18099,18126,18133,18139,18244,202429,202440,202511,202514,202532,202615,202635,202644,202647,203779,203784,203786,204006,17599,17614,17628,17629,18048,18062,18075,18077,18100,18122,18130,18134,18138,18223,18233,18234,18236,202432,202510,202513,202520,202524,202529,202596,202606,202613,202628,202632,202960,203772,203787,203792,203795,17597,17612,17621,17624,17634,18069,18083,18093,18095,18124,18131,18232,202441,202522,202639,203774,203778,203788,204005,17600,17608,17613,17627,18038,18039,18045,18071,18237,202437,202616,202636,202640,203776,203780,203797,205199,205242,14874,13546,14214,13612,16864,13693,13577,13627,13592,13636,13641,13706,14240,14896,13540,13611,13624,14196,14202,14218,14243,14958,14960,14963,13513,13541,13551,13599,13610,13664,13695,14246,14858,14908,14910,14925,14934,14935,14950,13549,13594,13603,13609,13613,13671,13691,14199,14245,14869,14889,14894,14906,14914,14919,14946,14948,14957,14962,203764,13479,13530,13536,13538,13539,13543,13548,13550,13552,13596,13597,13608,13646,13647,13683,13696,13704,14195,14242,14866,14898,14929,14938,13483,13501,13503,13537,13553,13588,13590,13591,13595,13598,13602,13606,13618,13637,13640,13644,13658,13677,13700,13705,13707,14212,14217,14220,14224,14886,14899,14901,14904,14923,14924,14932,13495,13498,13512,13520,13535,13554,13576,13600,13607,13614,13620,13622,13639,13642,13645,13660,13662,13663,13665,13669,13672,13673,13674,13681,13684,13686,13687,13690,13692,13694,13702,14203,14204,14206,14207,14209,14222,14241,14861,14871,14872,14888,14895,14909,14920,14921,14922,14926,14927,14928,14937,14944,14945,14953,16863,203763,13505,13517,13519,13586,13593,13605,13626,13635,13643,13666,13668,13675,13676,13685,13688,13698,14201,14221,14227,14873,14900,14903,14905,14933,14951,205289,13492,13509,13525,13544,13601,13615,13638,13667,13670,13699,13701,14213,14854,14892,14897,14902,14907,14911,14913,14915,203765,205291,205292
                    )
                    AND fc.estado = 'A'
                    AND fd.cod_conc = 77
        )
        SELECT s.* ,f.descripcion ,f.total_concp 
                ,CASE 
                    WHEN s.pend_x_aplicar = 0 AND valor_apl IS NOT NULL THEN 'sumar al campo vad_conc de factura_det'
                    WHEN s.pend_x_aplicar <> 0 AND s.pend_x_aplicar % 1 <> 0 THEN 'Quitar valor decimal de la tabla saldo_favor_cab'
                    ELSE NULL 
                END AS accion_realizar
                ,CASE 
                    WHEN s.pend_x_aplicar = 0 AND valor_apl IS NOT NULL THEN f.decimale_ajustar 
                    ELSE 0 
                 END AS factura_det_ajuste
                ,CASE 
                    WHEN s.pend_x_aplicar <> 0 AND s.pend_x_aplicar % 1 <> 0 THEN s.pend_x_aplicar % 1 
                    ELSE 0
                END AS saldo_favor_cab_decimal
                ,f.vad_conc+f.decimale_ajustar AS nuevo_valod_ajuste
            FROM sfv s 
            LEFT JOIN facturas f 
                ON f.cod_pred = s.cod_pred 
        ORDER BY accion_realizar;
        
BEGIN
    conteo := 0;
    RAISE NOTICE '======================================= INICIO DE LA EJECUCCIÓN =======================================';
    
    FOR registro IN cur_ppal LOOP
        conteo := conteo + 1;
        
        RAISE NOTICE '======================================================================================================';
        RAISE NOTICE 'Predio: %, Acción a realizar: %',registro.cod_pred ,registro.accion_realizar;

        CASE registro.accion_realizar
            WHEN 'Quitar valor decimal de la tabla saldo_favor_cab' THEN
                v_nuevo_sfv := TRUNC(registro.pend_x_aplicar); 
                
                RAISE NOTICE 'Valor SFV antes de actualizar: %', registro.pend_x_aplicar;
                RAISE NOTICE 'Valor SFV nuevo (truncado): %', v_nuevo_sfv;
                
                UPDATE saldo_favor_cab 
                    SET valor = v_nuevo_sfv
                    WHERE cod_pred = registro.cod_pred
                        AND cod_sfav = registro.cod_sfav;

                SELECT valor
                        INTO v_confirmar_sfv
                    FROM saldo_favor_cab
                    WHERE cod_pred = registro.cod_pred
                        AND cod_sfav = registro.cod_sfav;

                RAISE NOTICE 'Confirmación de la actualización, nuevo valor SFV: %',v_confirmar_sfv;

            WHEN 'sumar al campo vad_conc de factura_det' THEN
                
                SELECT fd.total_concp 
                        INTO v_ajuste_actual
                    FROM factura_det fd 
                    INNER JOIN factura_cab fc 
                        ON fc.cod_pred = fd.cod_pred
                        AND fc.cod_peri = fd.cod_peri 
                        AND fc.nro_seq = fd.nro_seq
                    WHERE fd.cod_peri = 202510
                        AND fd.cod_pred = registro.cod_pred
                        AND fd.cod_conc = 3 
                        AND fc.estado = 'A';

                v_nuevo_valor := ROUND(COALESCE(v_ajuste_actual, 2) + registro.nuevo_valod_ajuste,2);
                
                RAISE NOTICE 'Valor ajuste antes de actualizar: % ,valor a adicionar: %', ROUND(v_ajuste_actual,2) 
                    ,ROUND(registro.nuevo_valod_ajuste,2);
                RAISE NOTICE 'Valor ajuste nuevo: %', v_nuevo_valor;

                SELECT fc.total ,fc.nro_seq
                        INTO v_total_actual ,v_nro_seq
                    FROM factura_cab fc 
                    WHERE fc.cod_peri = 202510
                        AND fc.cod_pred = registro.cod_pred
                        AND fc.estado = 'A';

                RAISE NOTICE 'Valor factura actual: % ,Valor nuevo de factura: %', ROUND(v_total_actual,2) 
                    ,ROUND(v_total_actual+registro.nuevo_valod_ajuste,2);
                
                -- actualizar valor de concepto de ajuste
                UPDATE factura_det
                    SET total_concp = v_nuevo_valor 
                    WHERE cod_pred = registro.cod_pred
                        AND cod_peri = 202510
                        AND nro_seq = v_nro_seq
                        AND cod_conc = 3;

                -- actualizar valor del campo de ajuste para el SFV
                UPDATE factura_det
                    SET vad_conc = round(registro.nuevo_valod_ajuste,2)
                    WHERE cod_pred = registro.cod_pred
                        AND cod_peri = 202510
                        AND nro_seq = v_nro_seq
                        AND cod_conc = 77;

                -- actualizar total de la factura
                UPDATE factura_cab
                    SET total = ROUND(v_total_actual+registro.nuevo_valod_ajuste,2)
                    WHERE cod_pred = registro.cod_pred
                        AND cod_peri = 202510
                        AND nro_seq = v_nro_seq;

                SELECT round(sum(total_concp+COALESCE(vad_conc,0)),2)
                        INTO v_confirmar_fact
                    FROM factura_det
                    WHERE cod_peri = 202510 
                        AND cod_pred = registro.cod_pred
                        AND cod_conc NOT IN (3);

                RAISE NOTICE 'Confirmación de la actualización, nuevo valor factura: %',v_confirmar_fact;

        END CASE; 

        consecutivo := consecutivo + 1;
  
    END LOOP;

    RAISE NOTICE '======================================== FIN DE LA EJECUCCIÓN ========================================';
END $$;