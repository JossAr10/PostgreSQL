-- DROP FUNCTION kagua.xml_fact_electro_ciclo(numeric, varchar, numeric, varchar);

CREATE OR REPLACE FUNCTION kagua.xml_fact_electro_ciclo(v_municipio numeric, v_ciclo character varying, v_periodo numeric, v_empresa character varying)
 RETURNS TABLE(factura character varying, predio numeric, periodo numeric, n_secuencia numeric, ciclo character varying, municipio numeric, empresa character varying, etiqueta xml)
 LANGUAGE plpgsql
AS $function$
DECLARE
    cabecera record;
   	det_csmo record;
    xps_det record;
    items record;
    conteo INTEGER;
   	conteo_ite INTEGER;
   	conteo_xsp INTEGER;
    consecutivo INTEGER := 1;
    etiqueta_ext XML; 
   	factura varchar;
	fact_det varchar;
	fact_ant varchar;
    predio NUMERIC;
    periodo NUMERIC;
    n_secuencia NUMERIC;
    ciclo varchar;
    municipio NUMERIC;
    empresa varchar;
   	error_msg TEXT;
    consec_log NUMERIC;
	predio_exit NUMERIC;
    periodo_exit NUMERIC;
	prefijo varchar;
   
    cur_fact_tot CURSOR FOR 
		SELECT fc.nro_factura ,fc.cod_pred ,fc.cod_peri, fc.cod_cclo
			FROM factura_cab fc 
			WHERE fc.cod_empr = v_empresa
				AND fc.cod_peri = v_periodo	
				AND fc.estado = 'A' --USAR EN ENTORNO PRODUCTIVO
				AND fc.nro_factura IS NOT NULL 
				AND fc.fecexp >= '2024-12-26' --SALIDA A PRODUCCIÓN
				AND fc.fecexp = now()::date --USAR EN ENTORNO PRODUCTIVO
				AND fc.nro_factura NOT IN (SELECT pfe.nro_factura 
					FROM proceso_factura_electronica pfe 
					WHERE pfe.success = TRUE)
				AND fc.cod_munip IN (SELECT CASE WHEN v_municipio <> 0 THEN v_municipio ELSE cod_munip END FROM municipio) 
				AND fc.cod_cclo IN (SELECT CASE WHEN v_ciclo <> '0' THEN v_ciclo ELSE cod_cclo END FROM ciclo) 
			;
BEGIN
    conteo := 0;
    conteo_ite := 0;
   	conteo_xsp := 0;
	fact_det := '0';

	SET lc_time = 'es_ES';

	SELECT descripcion 
			INTO prefijo
		FROM empr_consecutivo
		WHERE cod_cons = 'SPD_PREFI';

    FOR cabecera IN cur_fact_tot LOOP
        conteo := conteo + 1;

		SELECT pfe.cod_pred ,pfe.cod_peri
       			INTO predio_exit ,periodo_exit
       		FROM  proceso_factura_electronica pfe
       		WHERE pfe.tipo_doc_elec = 'FC'
       			AND pfe.success = TRUE
       			AND pfe.cod_pred = cabecera.cod_pred
       			AND pfe.cod_peri = cabecera.cod_peri;

       IF predio_exit IS NULL AND periodo_exit IS NULL THEN

        -- Incrementar consecutivo para el siguiente detalle
        consecutivo := consecutivo + 1;
       
       	SELECT CASE WHEN max(xfe.consecutivo) IS NULL THEN 1 ELSE max(xfe.consecutivo)+1 END AS consec INTO consec_log
			FROM xml_fact_electronica xfe
			WHERE xfe.tipo_doc = 'FC'
				AND xfe.nro_documento = cabecera.nro_factura::varchar;

		SELECT cabecera.nro_factura ,fc.cod_pred ,fc.cod_peri ,fc.nro_seq ,fc.cod_cclo, fc.cod_munip ,fc.cod_empr
					,xmlelement(name "CABECERA",
					xmlagg(
						xmlelement(name "ENC",
							xmlforest(
								'INVOIC' AS "ENC_1",
								'UBL 2.1' AS "ENC_2",
								'601' AS "ENC_3",
								'DIAN 2.1: Documento Equivalente SPD' AS "ENC_4",
								'1' AS "ENC_5", --1 = Producción ; 2 = Pruebas
								prefijo||fc.nro_factura AS "ENC_6", --PREFIJO
								fc.fecexp AS "ENC_7",--fc.fecexp AS "ENC_7", now()::date
								(SELECT TO_CHAR(fcp.feccierra::timestamp, 'HH24:MI:SS')||'-05:00' AS hora
									FROM fact_ciclo_peri fcp
									WHERE (fcp.cod_peri ,fcp.cod_cclo) IN (
										SELECT cod_peri ,cod_cclo 
											FROM factura_cab 
											WHERE nro_factura = cabecera.nro_factura
									)
								) AS "ENC_8",
								fc.fecvto AS "ENC_9",
								'60' AS "ENC_10", --Documento Expedido para los Servicios P�blicos y Domiciliarios
								'COP' AS "ENC_11",
								CASE WHEN fc.total = 0 AND fc.total_sin_descto = 0 THEN 1 ELSE (SELECT count(1) 
									FROM factura_det fd
									WHERE fd.cod_empr = fc.cod_empr
										AND fd.cod_munip = fc.cod_munip 
										AND fd.cod_pred = fc.cod_pred
										AND fd.cod_peri = fc.cod_peri
										AND fd.nro_seq = fc.nro_seq 
										AND (fd.total_concp > 0 OR fd.cod_conc IN (	
											WITH data AS (
								    			SELECT ep.cod_conc_ppal, ep.cod_conc_alc
								    				FROM empr_parametros ep
								    				WHERE ep.cod_empr = v_empresa
											)
											SELECT concepto
												FROM data,
												LATERAL (VALUES 
											    	(cod_conc_ppal),
											    	(cod_conc_alc)
												) AS pivot(concepto)
											) 
										)
										AND fd.cod_conc NOT IN (SELECT dc.cod_conc FROM diferido_concepto dc)
										AND fd.cod_conc NOT IN (	
											WITH data AS (
								    			SELECT ep.codsaldfavor ,ep.cod_ajuste_dec ,999
								    				FROM empr_parametros ep
								    				WHERE ep.cod_empr = v_empresa
											)
											SELECT concepto
												FROM data,
												LATERAL (VALUES 
											    	(codsaldfavor),
											    	(cod_ajuste_dec),
											    	(999)
												) AS pivot(concepto)
										)
								) END AS "ENC_12",
								(SELECT CASE WHEN fpc.fch_ult_pago IS NULL THEN fpc.fecvto ELSE fpc.fch_ult_pago END
									FROM factura_per_cab fpc WHERE fpc.nro_factura = cabecera.nro_factura) AS "ENC_13",
								fc.nro_factura AS "ENC_14"
							)
						)
					),
					xmlagg(
						xmlelement(name "EMI",
							xmlforest(
								'1' AS "EMI_1",
								e.rasonsocial AS "EMI_2",
								split_part(e.nit, '-', 1) AS "EMI_3",
								split_part(e.nit, '-', 2) AS "EMI_4",
								'31' AS "EMI_5", --NIT
								e.nombre AS "EMI_6",
								'11001' AS "EMI_7",
								'Bogotá, D.C.' AS "EMI_8",
								'11' AS "EMI_10",
								'Bogotá' AS "EMI_11",
								e.direccion AS "EMI_12",
								'CO' AS "EMI_13",
								pa.nombre_pais AS "EMI_14"
							),
							xmlelement(
			                	name "TAC",
				                xmlforest(
				                    'O-13;O-15' AS "TAC_1"
				               	)
				            ),
							xmlelement(
			                	name "ICC",
				                xmlforest(
				                    prefijo AS "ICC_1" --PREFIJO
				                )
				            ),
				            xmlelement(
			                	name "DFE",
				                xmlforest(
				                    '11001' AS "DFE_1",
				                    e.ciudad AS "DFE_2",
				                    '11' AS "DFE_4",
				                    e.ciudad AS "DFE_5",
				                    e.direccion AS "DFE_6",
				                    'CO' AS "DFE_7",
				                	pa.nombre_pais AS "DFE_8"
				            	)
				            ),
				            xmlelement(
			                	name "CDE",
				                xmlforest(
				                    e.telefono AS "CDE_1",
				                    e.correo AS "CDE_2"
				                )
				            ),
							xmlelement(
			                	name "GTE",
				                xmlforest(
				                    '01' AS "GTE_1",
				                    'IVA' AS "GTE_2"
				            	)
				            )
						)
					),
					xmlagg(
						xmlelement(name "ADQ",
							xmlforest(
								CASE WHEN td.abrev = 'NIT' OR td.abrev = 'NI' THEN '1' ELSE '2' END AS "ADQ_1",
								c.nro_documento AS "ADQ_2",
								CASE WHEN td.abrev = 'CC' THEN '13' --C�dula de Ciudadan�a
									WHEN td.abrev = 'NIT' OR td.abrev = 'NI' THEN '31' --NIT
									WHEN td.abrev = 'PA' THEN '41' --Pasaporte
									WHEN td.abrev = 'CE' THEN '13' --C�dula de Extranjer�a 
									WHEN td.abrev = 'RC' THEN '11' --Registro Civil
								END AS "ADQ_3",
								concat(c.nombre,' ',c.segundo_nombre,' ',c.apellido,' ',c.segundo_apellido) AS "ADQ_4",
								CASE WHEN p.estrato IS NULL THEN u.cod_sui ELSE p.estrato END AS "ADQ_5",
								CASE WHEN u.cod_uso = '1' THEN '01'
									WHEN u.cod_uso = '2' THEN '03'
									WHEN u.cod_uso = '3' THEN '01'
								END AS "ADQ_6",
								concat(d.id,lpad(m.cod_dane::varchar,3,'0')) AS "ADQ_7",
								m.descripcion AS "ADQ_8",
								d.nombre AS "ADQ_10",
								d.id AS "ADQ_11",
								p.direccion AS "ADQ_12",
								'CO' AS "ADQ_13",
								pa.nombre_pais AS "ADQ_14",
								concat(c.nombre,' ',c.segundo_nombre,' ',c.apellido,' ',c.segundo_apellido) AS "ADQ_15",
								c.nro_documento AS "ADQ_16",
								CASE WHEN td.abrev = 'NIT' OR td.abrev = 'NI' THEN c.digito_verificacion ELSE '' END AS "ADQ_17",
								CASE WHEN td.abrev = 'CC' THEN '13' --C�dula de Ciudadan�a
									WHEN td.abrev = 'NIT' OR td.abrev = 'NI' THEN '31' --NIT
									WHEN td.abrev = 'PA' THEN '41' --Pasaporte
									WHEN td.abrev = 'CE' THEN '13' --C�dula de Extranjer�a 
									WHEN td.abrev = 'RC' THEN '11' --Registro Civil
								END AS "ADQ_18",
								'R-99-PN' AS "ADQ_19",
								p.cod_pred AS "ADQ_20"
							),
							xmlelement(
			                	name "ILA",
				                xmlforest(
				                    concat(c.nombre,' ',c.segundo_nombre,' ',c.apellido,' ',c.segundo_apellido) AS "ILA_1",
				                    c.nro_documento AS "ILA_2",
				                    CASE WHEN td.abrev = 'NIT' OR td.abrev = 'NI' THEN c.digito_verificacion ELSE '' END AS "ILA_3",
				                    CASE WHEN td.abrev = 'CC' THEN '13' --C�dula de Ciudadan�a
										WHEN td.abrev = 'NIT' OR td.abrev = 'NI' THEN '31' --NIT
										WHEN td.abrev = 'PA' THEN '41' --Pasaporte
										WHEN td.abrev = 'CE' THEN '13' --C�dula de Extranjer�a 
										WHEN td.abrev = 'RC' THEN '11' --Registro Civil
									END AS "ILA_4"
				                )
				            ),
				            xmlelement(
			                	name "GTA",
				                	xmlforest(
				                    	'ZZ' AS "GTA_1",
				                    	'No aplica' AS "GTA_2"
				                    )
				            )
						)
					),
					xmlagg(
						xmlelement(name "TOT",
							xmlforest(
								(SELECT round(sum(fd.total_concp+COALESCE(fd.vad_conc,0)),2) 
										FROM factura_det fd
										INNER JOIN factura_cab fc 
											USING (cod_empr,cod_munip,cod_pred,cod_peri,nro_seq)
										WHERE fc.nro_factura = cabecera.nro_factura
											AND fd.cod_conc NOT IN (SELECT dc.cod_conc FROM diferido_concepto dc)
											AND fd.cod_conc NOT IN (	
												WITH data AS (
									    			SELECT ep.codsaldfavor ,ep.cod_ajuste_dec ,999
									    				FROM empr_parametros ep
									    				WHERE ep.cod_empr = v_empresa
												)
												SELECT concepto
													FROM data,
													LATERAL (VALUES 
												    	(codsaldfavor),
												    	(cod_ajuste_dec),
												    	(999)
													) AS pivot(concepto)
											)
								) AS "TOT_1", --Total Valor Bruto antes de tributos.
								'COP' AS "TOT_2",
								round(0.0,2) AS "TOT_3", --Total Valor Base Imponible: Base imponible para el c�lculo de los tributos.
								'COP' AS "TOT_4",
								(SELECT round(sum(fd.total_concp+COALESCE(fd.vsf_conc,0)+fd.vad_conc),2)
										FROM factura_det fd
										INNER JOIN factura_cab fc 
											USING (cod_empr,cod_munip,cod_pred,cod_peri,nro_seq)
										WHERE fc.nro_factura = cabecera.nro_factura
											AND fd.cod_conc NOT IN (SELECT dc.cod_conc FROM diferido_concepto dc)
											AND fd.cod_conc NOT IN (	
												WITH data AS (
									    			SELECT ep.codsaldfavor ,ep.cod_ajuste_dec ,999
									    				FROM empr_parametros ep
									    				WHERE ep.cod_empr = v_empresa
												)
												SELECT concepto
													FROM data,
													LATERAL (VALUES 
												    	(codsaldfavor),
												    	(cod_ajuste_dec),
												    	(999)
													) AS pivot(concepto)
											)
								) AS "TOT_5", --Valor total del documento equivalente TOT_7 + TOT_11 - TOT_9.
								'COP' AS "TOT_6",
								(SELECT round(sum(fd.total_concp+fd.vad_conc),2) 
										FROM factura_det fd
										INNER JOIN factura_cab fc 
											USING (cod_empr,cod_munip,cod_pred,cod_peri,nro_seq)
										WHERE fc.nro_factura = cabecera.nro_factura
											AND fd.cod_conc NOT IN (SELECT dc.cod_conc FROM diferido_concepto dc)
											AND fd.cod_conc NOT IN (	
												WITH data AS (
									    			SELECT ep.codsaldfavor ,ep.cod_ajuste_dec ,999
									    				FROM empr_parametros ep
									    				WHERE ep.cod_empr = v_empresa
												)
												SELECT concepto
													FROM data,
													LATERAL (VALUES 
												    	(codsaldfavor),
												    	(cod_ajuste_dec),
												    	(999)
													) AS pivot(concepto)
											)
								) AS "TOT_7", --Total de Valor Bruto m�s tributos.
								'COP' AS "TOT_8",
								(SELECT round(sum(COALESCE(fd.vsf_conc,0)*-1),2)
										FROM factura_det fd
										INNER JOIN factura_cab fc 
											USING (cod_empr,cod_munip,cod_pred,cod_peri,nro_seq)
										WHERE fc.nro_factura = cabecera.nro_factura
											AND fd.cod_conc NOT IN (SELECT dc.cod_conc FROM diferido_concepto dc)
											AND fd.cod_conc NOT IN (	
												WITH data AS (
									    			SELECT ep.codsaldfavor ,ep.cod_ajuste_dec ,999
									    				FROM empr_parametros ep
									    				WHERE ep.cod_empr = v_empresa
												)
												SELECT concepto
													FROM data,
													LATERAL (VALUES 
												    	(codsaldfavor),
												    	(cod_ajuste_dec),
												    	(999)
													) AS pivot(concepto)
											)
								) AS "TOT_9", --Descuento Total.
								'COP' AS "TOT_10"
							)
						)
					),
					xmlagg(
						xmlelement(name "DSC",
							xmlforest(
								'1' AS "DSC_1", 
								'1' AS "DSC_2", -- 1 = Descuento
								'false' AS "DSC_3",
								'00' AS "DSC_4", --Descuento no condicionado *
								'Saldo a favor' AS "DSC_5",
								(SELECT round(sum(COALESCE(fd.vsf_conc,0)*-1),2)
										FROM factura_det fd
										INNER JOIN factura_cab fc 
											USING (cod_empr,cod_munip,cod_pred,cod_peri,nro_seq)
										WHERE fc.nro_factura = cabecera.nro_factura
											AND fd.cod_conc NOT IN (SELECT dc.cod_conc FROM diferido_concepto dc)
											AND fd.cod_conc NOT IN (	
												WITH data AS (
									    			SELECT ep.codsaldfavor ,ep.cod_ajuste_dec ,999
									    				FROM empr_parametros ep
									    				WHERE ep.cod_empr = v_empresa
												)
												SELECT concepto
													FROM data,
													LATERAL (VALUES 
												    	(codsaldfavor),
												    	(cod_ajuste_dec),
												    	(999)
													) AS pivot(concepto)
											)
								) AS "DSC_7",
								'COP' AS "DSC_8",
								0 AS "DSC_9",
								'COP' AS "DSC_10"
							)
						)
					),
					xmlagg(
						xmlelement(name "DRF",
							xmlforest(
								prefijo AS "DRF_1", --PREFIJO
								'1' AS "DRF_2",
								'9999999' AS "DRF_3"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'1.-'||fc.cod_pred||'|'||fc.nro_factura AS "NOT_1"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'3.-'||(WITH deuda AS (
										SELECT round(sum(cd.total_concp),2) AS total_cc
					                        FROM ccobro_det cd
					                        INNER JOIN factura_per_cab fpc
					                        	ON fpc.cod_pred = cd.cod_pred
					                        	AND fpc.nro_cuenta_cobro = cd.nro_cuenta
					                        WHERE fpc.nro_factura = cabecera.nro_factura
					                        	AND fpc.cod_pred = fc.cod_pred
					                            AND cd.cod_peri != fc.cod_peri
								)
								SELECT CASE WHEN d.total_cc IS NULL THEN 0 ELSE d.total_cc END 
									FROM deuda d)||'|'||
								(SELECT round(m.porc_mora*100,1)||'%'
									FROM municipio m
									WHERE m.cod_munip = fc.cod_munip) AS "NOT_1"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'6.-|'||fc.cod_cclo AS "NOT_1"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'7.-'||fc.cod_pred||'|'||fc.nro_factura||'|'
									||(SELECT round(sum(fd.total_concp+COALESCE(fd.vsf_conc,0)+fd.vad_conc),2)
											FROM factura_det fd
											INNER JOIN factura_cab fc 
												USING (cod_empr,cod_munip,cod_pred,cod_peri,nro_seq)
											WHERE fc.nro_factura = cabecera.nro_factura
												AND fd.cod_conc NOT IN (SELECT dc.cod_conc FROM diferido_concepto dc)
												AND fd.cod_conc NOT IN (	
													WITH data AS (
										    			SELECT ep.codsaldfavor ,ep.cod_ajuste_dec ,999
										    				FROM empr_parametros ep
										    				WHERE ep.cod_empr = v_empresa
													)
													SELECT concepto
														FROM data,
														LATERAL (VALUES 
													    	(codsaldfavor),
													    	(cod_ajuste_dec),
													    	(999)
														) AS pivot(concepto)
												)
									)||'|'
									||(WITH diferidos AS (
										SELECT round(sum(fd.total_concp+COALESCE(fd.vsf_conc,0)+fd.vad_conc),2) AS total
											FROM factura_det fd
											INNER JOIN factura_cab fc 
												USING (cod_empr,cod_munip,cod_pred,cod_peri,nro_seq)
											WHERE fc.nro_factura = cabecera.nro_factura
												AND fd.cod_conc IN (SELECT dc.cod_conc FROM diferido_concepto dc)
									)	
									SELECT CASE WHEN total IS NULL THEN 0 ELSE total END 
										FROM diferidos) AS "NOT_1"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'8.-'||(SELECT b.descripcion FROM barrio b WHERE b.cod_barrio = p.cod_barrio)||'|' AS "NOT_1"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'9.-'||fc.fecvto||'|' AS "NOT_1"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'10.-'||p.rutareparto||'|'||(SELECT fpc.nropermora||'|'
									||TO_CHAR(TO_DATE(substring(fpc.cod_peri::varchar,5,2) , 'MM'), 'TMMonth')
								FROM factura_per_cab fpc WHERE fpc.nro_factura = cabecera.nro_factura)||'|' AS "NOT_1"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'11.-'||(SELECT round(fpc.csmo_lect_real,3)||'|'||round(fpc.factor_corre,3)||'|'||round(fpc.consumo,3)||'|'
									||(SELECT poder
										FROM componentes_tarifa ct
										WHERE ct.cod_munip = fpc.cod_munip
											AND ct.ano::varchar = substring(fpc.cod_peri::varchar,3,2) 
											AND ct.mes::varchar = substring(fpc.cod_peri::varchar,6,1))||'|'
									||(SELECT codigo FROM tipo_gas tg WHERE tg.cod_gas = p.cod_gas)||'|'
									FROM factura_per_cab fpc WHERE fpc.nro_factura = cabecera.nro_factura) AS "NOT_1"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'12.-'||p.fec_rtr||'|'||p.fec_ven_rtr AS "NOT_1"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'13.-'||(SELECT TO_CHAR(TO_DATE(substring(fpc.cod_peri::varchar,5,2) , 'MM'), 'TMMonth')||'-'||substr(fpc.cod_peri::varchar,1,4)
										||'|'||fpc.fec_susp 
									FROM factura_per_cab fpc 
									WHERE fpc.nro_factura = cabecera.nro_factura)  AS "NOT_1"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'14.-'||(SELECT lectura_anterior||'|'||lectura||'|'||CASE WHEN fpc.nro_cuenta_cobro IS NOT NULL 
									THEN
										round((SELECT round(sum(cd.total_concp),2) AS total_concp 
											FROM factura_per_cab fpc
											INNER JOIN ccobro_det cd
										 		ON fpc.cod_pred = cd.cod_pred
										     	AND fpc.nro_cuenta_cobro = cd.nro_cuenta
											WHERE fpc.nro_factura = cabecera.nro_factura 
												AND fpc.cod_pred = fpc.cod_pred
										     	AND cd.cod_peri != fpc.cod_peri)+fpc.total,2)	
									ELSE 
										round(fpc.total,2)
									END||'|'||fpc.dias_facturados
								FROM factura_per_cab fpc
								WHERE fpc.nro_factura = cabecera.nro_factura) AS "NOT_1"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'16.-'||(SELECT ct.gt||'|'||ct.cm||'|'||ct.con5y6||'|'||ct.et||'|'||ct.fv||'|'||ct.connores||'|'||ct.nt||'|'||ct.p||'|'
										||ct.tt||'|'||ct.costo||'|'||ct.dm||'|'||ct.meq1||'|'||ct.meq2||'|'||ct.porsub1||'|'||ct.porsub2 
									FROM kagua.componentes_tarifa ct 
									WHERE ct.cod_munip = fc.cod_munip
										AND ct.ano = substring(fc.cod_peri::varchar,2,3)::numeric 
										AND ct.mes = substring(fc.cod_peri::varchar,5,2)::numeric ) AS "NOT_1"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'18.-'||(SELECT '0.01-7.26'||' '||round(fd.canti_e1,2)||'*'||pf.precioe1||'*'||round(pf.precioe1*fd.canti_e1,2)
										FROM preciotar_fecha pf 
										INNER JOIN tarifa t
											ON t.cod_empr = pf.cod_empr 
											AND t.cod_munip = pf.cod_munip 
											AND t.cod_conc = pf.cod_conc 
											AND t.ref_tarf = pf.cod_tarf 
										INNER JOIN factura_per_det fd 
											ON fd.cod_empr = pf.cod_empr
											AND fd.cod_munip = pf.cod_munip
											AND fd.cod_peri = pf.cod_peri
											AND fd.cod_conc = pf.cod_conc
										WHERE pf.cod_peri = cabecera.cod_peri
											AND fd.cod_pred = cabecera.cod_pred
											AND t.cod_tarf IN (
												SELECT pc.cod_tarf
													FROM predio_concep pc 
													WHERE pc.cod_pred = cabecera.cod_pred))||'|'
									||(WITH seg_rango AS (
										SELECT CASE WHEN fd.canti_e1 = 7.26 THEN ('7.27-999'||' '||round(fd.canti_e2,2))
											ELSE NULL END
											||'*'||CASE WHEN fd.canti_e1 = 7.26 THEN pf.precioe2 ELSE NULL END
											||'*'||CASE WHEN fd.canti_e1 = 7.26 THEN round(pf.precioe2*fd.canti_e2,2) ELSE NULL END AS rango2
										FROM preciotar_fecha pf 
										INNER JOIN tarifa t
											ON t.cod_empr = pf.cod_empr 
											AND t.cod_munip = pf.cod_munip 
											AND t.cod_conc = pf.cod_conc 
											AND t.ref_tarf = pf.cod_tarf 
										INNER JOIN factura_per_det fd 
											ON fd.cod_empr = pf.cod_empr
											AND fd.cod_munip = pf.cod_munip
											AND fd.cod_peri = pf.cod_peri
											AND fd.cod_conc = pf.cod_conc
										WHERE pf.cod_peri = cabecera.cod_peri
											AND fd.cod_pred = cabecera.cod_pred
											AND t.cod_tarf IN (
												SELECT pc.cod_tarf
													FROM predio_concep pc 
													WHERE pc.cod_pred = cabecera.cod_pred
											)
									)
									SELECT CASE WHEN rango2 IS NULL THEN '' ELSE rango2 END 
										FROM seg_rango) AS "NOT_1"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'19.-'||(SELECT ft.texto 
									FROM kagua.factura_txt ft 
									WHERE ft.cod_peri = fc.cod_peri
										AND ft.cod_munip = fc.cod_munip)||'|' AS "NOT_1"
							)
						)
					),
					xmlagg(
						xmlelement(name "NOT",
							xmlforest(
								'25.-'||(SELECT STRING_AGG(fila, '|') AS resultado
									FROM (
									    SELECT 
									        dif.concepto || '*' || 
									        round(dif.saldo_cap_ant, 2)::TEXT || '*' || 
									        round(dif.valor_capital, 2)::TEXT || '*' || 
									        round(dif.int_fin, 2)::TEXT || '*' || 
									        round(dif.valor_cuota, 2)::TEXT || '*' || 
									        round(dif.nuevo_saldo_cap, 2)::TEXT || '*' || 
									        dif.cuotas_pend || '*' || 
									        dif.porc_interes::TEXT AS fila
									    FROM (
									        SELECT dfc.descripcion AS concepto, dcu.cod_dife, dcu.cod_peri,
									               CASE 
									                   WHEN dc.cod_peri_ini = v_periodo THEN dc.valor_financiado
									                   WHEN dc.cod_peri_ini != v_periodo THEN 
									                       (SELECT SUM(d.valor_capital) 
									                        FROM diferido_cuota d 
									                        WHERE d.cod_dife = dcu.cod_dife 
									                          AND d.cod_pred = dcu.cod_pred 
									                          AND d.cod_peri >= v_periodo) 
									               END AS saldo_cap_ant,
									               dcu.valor_capital,
									               dcu.valor_total - dcu.valor_capital AS int_fin,
									               dcu.valor_total AS valor_cuota,
									               COALESCE(
									                   (SELECT SUM(d.valor_capital) 
									                    FROM diferido_cuota d 
									                    WHERE d.cod_dife = dcu.cod_dife 
									                      AND d.cod_pred = dcu.cod_pred 
									                      AND d.cod_peri > v_periodo),
									                   0) AS nuevo_saldo_cap,
									               (SELECT COUNT(1) 
									                FROM diferido_cuota d 
									                WHERE d.cod_dife = dcu.cod_dife 
									                  AND d.cod_pred = dcu.cod_pred 
									                  AND d.cod_peri > v_periodo) AS cuotas_pend,
									               dc.cant_cuotas,
									               ROUND(dc.porc_interes * 10, 1) AS porc_interes,
									               dc.cod_pred, 1 AS nro_seq, dc.cod_munip, dc.cod_empr
									        FROM diferido_cab dc
									        INNER JOIN diferido_cuota dcu 
									            ON dcu.cod_empr = dc.cod_empr 
									            AND dcu.cod_munip = dc.cod_munip 
									            AND dcu.cod_dife = dc.cod_dife 
									            AND dcu.cod_dcon = dc.cod_dcon
									        INNER JOIN diferido_concepto dfc
									            ON dfc.cod_empr = dc.cod_empr 
									            AND dfc.cod_dcon = dc.cod_dcon
									        WHERE dc.cod_pred = fc.cod_pred
									          AND dc.estado = 'A'
									        ORDER BY dcu.cod_dife, dcu.cod_peri
									    ) dif 
									    WHERE dif.cod_peri = v_periodo
									) diferidos
								) AS "NOT_1"
							)
						)
					),						
					xmlagg(
						xmlelement(name "MEP",
							xmlforest(
								'2' AS "MEP_1",
								'ZZZ' AS "MEP_2",
								fc.fecvto AS "MEP_3"
							)
						)
					),
					xmlagg(
						xmlelement(name "CTS",
							xmlforest(
								'COLG03' AS "CTS_1" --vacio en PROD , CALIDAD: COLG03
							)
						)
					),
					xmlagg(
						xmlelement(name "PAY",
							xmlforest(
								(WITH datos AS (
								    SELECT fpc.nro_factura ,fpc.fecvto ,fpc.total AS total_fact ,m.codigo_recaudo,
								           COALESCE(fpc.nro_cuenta_cobro, 0) AS nro_cuenta_cobro ,COALESCE(cc.total, 0) AS total_cuenta
								    FROM factura_per_cab fpc
								    INNER JOIN municipio m 
								        ON m.cod_empr = fpc.cod_empr
								        AND m.cod_munip = fpc.cod_munip 
								    LEFT JOIN ccobro_cab cc 
								        ON cc.cod_empr = fpc.cod_empr 
								        AND cc.cod_pred = fpc.cod_pred 
								        AND cc.nro_cuenta = fpc.nro_cuenta_cobro
								    WHERE fpc.nro_factura = cabecera.nro_factura
								)
								SELECT replace(a, 'ñ', '') as texto
									FROM kagua.barcode_factura(
										(SELECT codigo_recaudo FROM datos)::numeric, 
										(SELECT CASE WHEN (SELECT nro_cuenta_cobro FROM datos) = 0 THEN '1' || lpad((SELECT nro_factura FROM datos)::text, 9, '0') 
								      		ELSE '5' || lpad((SELECT nro_cuenta_cobro FROM datos)::text, 9, '0') 
								   		END)::numeric,
								        (SELECT CASE WHEN (SELECT nro_cuenta_cobro FROM datos) = 0 THEN (SELECT total_fact FROM datos) 
								      		ELSE (SELECT total_cuenta FROM datos) 
								   		END),
								         to_char((SELECT fecvto FROM datos)::DATE, 'YYYY-MM-DD')) a) AS "PAY_1"
							)
						)
					)
				) AS etiqueta_ext 
				INTO factura ,predio ,periodo ,n_secuencia ,ciclo ,municipio ,empresa ,etiqueta_ext
			FROM factura_cab fc 
			INNER JOIN empresas e 
				ON e.cod_empr = fc.cod_empr 
			INNER JOIN predio p 
				ON p.cod_empr = fc.cod_empr 
				AND p.cod_pred = fc.cod_pred 
			INNER JOIN cliente c 
				ON c.cod_empr = p.cod_empr 
				AND c.cod_clte = p.cod_clte  
			INNER JOIN tipo_doc td	
				ON td.cod_empr = c.cod_empr 
				AND td.cod_tdoc = c.cod_tdoc 
			INNER JOIN municipio m 
				ON m.cod_empr = p.cod_empr 
				AND m.cod_munip = p.cod_munip 
			INNER JOIN departamentos d 
				ON d.id = m.cod_dpto
			INNER JOIN paises pa 
				ON pa.cod_pais = d.cod_pais
			INNER JOIN uso u 
				ON u.cod_empr = p.cod_empr
				AND u.cod_uso = p.cod_uso
			WHERE fc.cod_peri = v_periodo	
				AND fc.nro_factura = cabecera.nro_factura
			GROUP BY fc.cod_pred ,fc.cod_peri ,fc.nro_seq ,fc.cod_cclo, fc.cod_munip ,fc.cod_empr;
        
		RAISE NOTICE 'Factura: % ,Etiqueta EXT: %', factura ,etiqueta_ext;
	
		RETURN QUERY
        SELECT factura ,predio ,periodo ,n_secuencia ,ciclo ,municipio ,empresa ,etiqueta_ext;
        
        --almacenar xml por si solicitan conocer el xml enviado a la DIAN
       	INSERT INTO xml_fact_electronica VALUES (consec_log ,'FC' ,factura ,etiqueta_ext,now() ,periodo);
       
		FOR det_csmo IN (
			SELECT fd.cod_conc ,fd.cod_peri ,fd.cod_pred ,fd.nro_seq
				FROM factura_cab fc 
				INNER JOIN factura_det fd 
					USING (cod_empr,cod_munip,cod_pred,cod_peri,nro_seq)
				WHERE fc.cod_empr = v_empresa
					AND fc.cod_peri = v_periodo	
					--AND fc.nro_seq = 1
					AND fc.nro_factura = cabecera.nro_factura 
					--AND fc.cod_pred = v_predio   --SOLO PARA PRUEBAS INDIVIDUALES
					AND fd.cod_conc IN (	
						WITH data AS (
			    			SELECT ep.cod_conc_ppal, ep.cod_conc_alc
			    				FROM empr_parametros ep
			    				WHERE ep.cod_empr = v_empresa
						)
						SELECT concepto
							FROM data,
							LATERAL (VALUES 
						    	(cod_conc_ppal),
						    	(cod_conc_alc)
							) AS pivot(concepto)
					)
		) LOOP
			--conteo_xsp := conteo_xsp + 1;

			IF fact_det::numeric = 0 THEN 
				conteo_xsp := conteo_xsp + 1;
				RAISE NOTICE 'Conteo para XPS CSMO inicial: %', conteo_xsp;
			ELSIF cabecera.nro_factura::numeric = fact_det::numeric THEN 
				conteo_xsp := conteo_xsp + 1;
				RAISE NOTICE 'Conteo para XPS CSMO: %', conteo_xsp;
			ELSE 
				conteo_xsp := 1;
				RAISE NOTICE 'Conteo para XPS CSMO: %', conteo_xsp;
			END IF;
			
			SELECT cabecera.nro_factura ,fd.cod_pred ,fd.cod_peri ,fd.nro_seq ,fd.cod_munip, fd.cod_munip ,fd.cod_empr 
					,xmlelement(name "DETALLE",
					xmlagg(
						xmlelement(name "XSP",
							xmlforest(
								conteo_xsp AS "XSP_1",
								'SPD' AS "XSP_2",
								fd.descripcion AS "XSP_3",
								e.nombre AS "XSP_4",
								e.nombre AS "XSP_5",
								'true' AS "XSP_6"
							),
							xmlelement(
			                		name "SPB",
			                		xmlforest(
				                        fd.cod_pred AS "SPB_1",
				                        'Contrato' AS "SPB_2"
				                    ),
									xmlelement(
			                    		name "SPS",
				                    	xmlforest(
				                        	(SELECT c.email
												FROM cliente c
												INNER JOIN predio p
													ON p.cod_clte = c.cod_clte
												WHERE p.cod_pred = fd.cod_pred
											) AS "SPS_8"
				                    	)
				                    ),
				                    xmlelement(
			                    		name "SCP",
				                    	xmlforest(
				                        	'1' AS "SCP_1",
							                '94' AS "SCP_2",
							                round(fd.total_concp+fd.vad_conc,2) AS "SCP_3",
							                'COP' AS "SCP_4"
				                    	)
				                    ),
			                		xmlelement(
			                    		name "SPC",
				                    	xmlforest(
				                        	CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) < 0 THEN '1' ELSE '' END AS "SPC_1",
				                        	CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) < 0 THEN 'Subsidio' 
				                        		ELSE ''
				                        	END AS "SPC_2",
				                        	CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) < 0 THEN round(((fd.subapo_cons+fd.subapo_cfijo)*-1),2)::varchar
				                        		ELSE ''
				                        	END AS "SPC_3",
				                        	CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) < 0 THEN 'COP'
				                        		ELSE ''
				                        	END  AS "SPC_4"
				                    	)
				                    ),
			                		xmlelement(
			                    		name "SPC",
				                    	xmlforest(
				                        	CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) < 0 THEN '2'
												ELSE 
													CASE WHEN fd.vad_conc < 0 THEN '1' 
														ELSE ''
													END
											END AS "SPC_1",
				                        	CASE WHEN fd.vad_conc < 0 THEN 'Ajuste a la decena' 
				                        		ELSE ''
				                        	END AS "SPC_2",
				                        	CASE WHEN fd.vad_conc < 0 THEN round((fd.vad_conc*-1),2)::varchar
				                        		ELSE ''
				                        	END AS "SPC_3",
				                        	CASE WHEN fd.vad_conc < 0 THEN 'COP'
				                        		ELSE ''
				                        	END  AS "SPC_4"
				                    	)
				                    ),
				                    xmlelement(
			                    		name "SPC",
				                    	xmlforest(
				                        	CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) < 0 AND fd.vad_conc < 0 THEN '3'
												ELSE 
													CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) < 0 OR fd.vad_conc < 0 THEN '2' 
														ELSE 
															CASE WHEN COALESCE(fd.vsf_conc,0) < 0 THEN '1' 
																ELSE ''
															END
													END
											END AS "SPC_1",
				                        	CASE WHEN COALESCE(fd.vsf_conc,0) < 0 THEN 'Saldo a favor' 
				                        		ELSE ''
				                        	END AS "SPC_2",
				                        	CASE WHEN COALESCE(fd.vsf_conc,0) < 0 THEN round((COALESCE(fd.vsf_conc,0)*-1),2)::varchar
				                        		ELSE ''
				                        	END AS "SPC_3",
				                        	CASE WHEN COALESCE(fd.vsf_conc,0) < 0 THEN 'COP'
				                        		ELSE ''
				                        	END  AS "SPC_4"
				                    	)
				                    ),
				                    xmlelement(
			                    		name "SPD",
				                    	xmlforest(
				                        	CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) > 0 THEN fd.consecutivo::varchar
				                        		ELSE ''
				                        	END AS "SPD_1",
				                        	CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) > 0 THEN 'Contribucción' 
				                        		ELSE ''
				                        	END AS "SPD_2",
				                        	CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) > 0 THEN round((fd.subapo_cons+fd.subapo_cfijo),2)::varchar
				                        		ELSE ''
				                        	END AS "SPD_3",
				                        	CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) > 0 THEN 'COP'
				                        		ELSE ''
				                        	END AS "SPD_4"
				                    	)
				                    ),
				                    xmlelement(
			                    		name "SPD",
				                    	xmlforest(
				                        	CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) > 0 THEN '2'
												ELSE 
													CASE WHEN fd.vad_conc > 0 THEN '1' 
														ELSE ''
													END
											END AS "SPD_1",
				                        	CASE WHEN fd.vad_conc > 0 THEN 'Ajuste a la decena' 
				                        		ELSE ''
				                        	END AS "SPD_2",
				                        	CASE WHEN fd.vad_conc > 0 THEN round(fd.vad_conc,2)::varchar
				                        		ELSE ''
				                        	END AS "SPD_3",
				                        	CASE WHEN fd.vad_conc > 0 THEN 'COP'
				                        		ELSE ''
				                        	END AS "SPD_4"
				                    	)
				                    ),
				                    xmlelement(
			                    		name "SPM",
				                    	xmlforest(
				                        	(SELECT p.serialmedi FROM predio p WHERE p.cod_pred = fd.cod_pred) AS "SPM_1",
											(SELECT fec_lectura_ant FROM factura_per_cab WHERE nro_factura = cabecera.nro_factura) AS "SPM_2",
											(SELECT feclectura FROM factura_per_cab WHERE nro_factura = cabecera.nro_factura) AS "SPM_5"
				                    	)
				                    ),
				                    xmlelement(
			                    		name "SPF",
				                    	xmlforest(
				                        	'' AS "SPF_1",
							                '' AS "SPF_2",
							                (SELECT fec_lectura_ant FROM factura_per_cab WHERE nro_factura = cabecera.nro_factura) AS "SPF_3",
							                (SELECT feclectura FROM factura_per_cab WHERE nro_factura = cabecera.nro_factura) AS "SPF_4",
							                (SELECT dias_facturados FROM factura_per_cab WHERE nro_factura = cabecera.nro_factura) AS "SPF_5",
							                'DAY' AS "SPF_6",
											(SELECT CASE WHEN fpc.nro_cuenta_cobro IS NOT NULL 
												THEN
													round((SELECT round(sum(cd.total_concp),2) AS total_concp 
														FROM factura_per_cab fpc
														INNER JOIN ccobro_det cd
													 		ON fpc.cod_pred = cd.cod_pred
													     	AND fpc.nro_cuenta_cobro = cd.nro_cuenta
														WHERE fpc.nro_factura = cabecera.nro_factura 
															AND fpc.cod_pred = fpc.cod_pred
													     	AND cd.cod_peri != fpc.cod_peri)+fpc.total,2)	
												ELSE 
													round(fpc.total,2)
												END
											FROM factura_per_cab fpc
											WHERE fpc.nro_factura = cabecera.nro_factura) AS "SPF_7",
							                'COP' AS "SPF_8"
				                    	)
				                    )
				            )
						)
					)
				) AS etiqueta_ext 
					INTO fact_det ,predio ,periodo ,n_secuencia ,cabecera.cod_cclo ,municipio ,empresa ,etiqueta_ext
				FROM factura_det fd
				INNER JOIN empresas e 
					ON e.cod_empr = fd.cod_empr 
				WHERE fd.cod_pred = det_csmo.cod_pred
					AND fd.cod_peri = det_csmo.cod_peri
					AND fd.nro_seq = det_csmo.nro_seq
					AND fd.cod_conc = det_csmo.cod_conc
				GROUP BY fd.cod_pred ,fd.cod_peri ,fd.nro_seq ,fd.cod_munip, fd.cod_munip ,fd.cod_empr;  
			
			RAISE NOTICE 'Factura: % ,Etiqueta EXT: %', fact_det ,etiqueta_ext;
			RETURN QUERY
            SELECT fact_det ,predio ,periodo ,n_secuencia ,ciclo ,municipio ,empresa ,etiqueta_ext;
           
           	--almacenar xml por si solicitan conocer el xml enviado a la DIAN
           	INSERT INTO xml_fact_electronica VALUES (consec_log ,'FC',fact_det ,etiqueta_ext,now() ,periodo);
        END LOOP;
       
       
        FOR xps_det IN (
			SELECT fd.cod_conc ,fd.cod_peri ,fd.cod_pred ,fd.nro_seq ,fd.consecutivo
				FROM factura_cab fc 
				INNER JOIN factura_det fd
					USING (cod_empr,cod_munip,cod_pred,cod_peri,nro_seq)
				WHERE fc.cod_empr = v_empresa
					AND fc.cod_peri = v_periodo	
					--AND fc.nro_seq = 1
					AND fc.nro_factura = cabecera.nro_factura 
					--AND fc.cod_pred = v_predio   --SOLO PARA PRUEBAS INDIVIDUALES
					AND fd.total_concp > 0
					AND fd.cod_conc NOT IN (	
						WITH data AS (
			    			SELECT ep.cod_conc_ppal, COALESCE(ep.cod_conc_alc,0) AS cod_conc_alc ,ep.codsaldfavor ,ep.cod_ajuste_dec ,999
			    				FROM empr_parametros ep
			    				WHERE ep.cod_empr = v_empresa
						)
						SELECT concepto
							FROM data,
							LATERAL (VALUES 
						    	(cod_conc_ppal),
						    	(cod_conc_alc),
						    	(codsaldfavor),
						    	(cod_ajuste_dec),
						    	(999)
							) AS pivot(concepto)
					)
					AND fd.cod_conc NOT IN (SELECT dc.cod_conc FROM diferido_concepto dc)
				ORDER BY fd.consecutivo ASC 
		) LOOP
			--conteo_xsp := conteo_xsp + 1;

			IF fact_det::numeric = 0 THEN 
				conteo_xsp := conteo_xsp + 1;
				RAISE NOTICE 'Conteo para OTROS CONCEPTOS inicial: %', conteo_xsp;
			ELSIF cabecera.nro_factura::numeric = fact_det::numeric THEN 
				conteo_xsp := conteo_xsp + 1;
				RAISE NOTICE 'Conteo para OTROS CONCEPTOS: %', conteo_xsp;
			ELSE 
				conteo_xsp := 1;
				RAISE NOTICE 'Conteo para OTROS CONCEPTOS: %', conteo_xsp;
			END IF;
			
			SELECT cabecera.nro_factura ,fd.cod_pred ,fd.cod_peri ,fd.nro_seq ,fd.cod_munip, fd.cod_munip ,fd.cod_empr
					,xmlelement(name "OTRO_CONCEP",
					xmlagg(
						xmlelement(name "XSP",
							xmlforest(
								conteo_xsp AS "XSP_1",
								'SPD' AS "XSP_2",
								fd.descripcion AS "XSP_3",
								e.nombre AS "XSP_4",
								e.nombre AS "XSP_5",
								'true' AS "XSP_6"
							),
							xmlelement(
			               		name "SPB",
			                	xmlforest(
				                	fd.cod_pred AS "SPB_1",
				                    'Contrato' AS "SPB_2"
				              	),
								xmlelement(
			                    	name "SCP",
				                    xmlforest(
				                        '1' AS "SCP_1",
							            '94' AS "SCP_2",
							        	round(fd.total_concp+fd.vad_conc,2) AS "SCP_3",
							            'COP' AS "SCP_4"
				                    )
				                ),
				              	xmlelement(
			           				name "SPC",
				            		xmlforest(
				               			CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) < 0 AND fd.vad_conc < 0 THEN '3'
											ELSE 
												CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) < 0 OR fd.vad_conc < 0 THEN '2' 
													ELSE 
														CASE WHEN COALESCE(fd.vsf_conc,0) < 0 THEN '1' 
															ELSE ''
														END
												END
										END AS "SPC_1",
				                       	CASE WHEN COALESCE(fd.vsf_conc,0) < 0 THEN 'Saldo a favor' 
				                        	ELSE ''
				                      	END AS "SPC_2",
				                    	CASE WHEN COALESCE(fd.vsf_conc,0) < 0 THEN round((COALESCE(fd.vsf_conc,0)*-1),2)::varchar
				                        	ELSE ''
				                   		END AS "SPC_3",
				                      	CASE WHEN COALESCE(fd.vsf_conc,0) < 0 THEN 'COP'
				                        	ELSE ''
				                    	END  AS "SPC_4"
				       				)
				                ),
								xmlelement(
			                    	name "SPC",
				                    xmlforest(
				                        CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) < 0 AND fd.vad_conc < 0 THEN '3'
											ELSE 
												CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) < 0 OR fd.vad_conc < 0 THEN '2' 
													ELSE 
														CASE WHEN COALESCE(fd.vsf_conc,0) < 0 THEN '1' 
															ELSE ''
														END
												END
										END AS "SPC_1",
				                        CASE WHEN fd.vad_conc < 0 THEN 'Ajuste a la decena' 
				                        	ELSE ''
				                        END AS "SPC_2",
				                        CASE WHEN fd.vad_conc < 0 THEN round((fd.vad_conc*-1),2)::varchar
				                        	ELSE ''
				                        END AS "SPC_3",
				                        CASE WHEN fd.vad_conc < 0 THEN 'COP'
				                        	ELSE ''
				                        END  AS "SPC_4"
				                    )
				                ),
								xmlelement(
			                    	name "SPD",
				                    xmlforest(
				                        CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) > 0 THEN '2'
											ELSE 
												CASE WHEN fd.vad_conc > 0 THEN '1' 
													ELSE ''
												END
										END AS "SPD_1",
				                        CASE WHEN fd.vad_conc > 0 THEN 'Ajuste a la decena' 
				                        	ELSE ''
				                        END AS "SPD_2",
				                        CASE WHEN fd.vad_conc > 0 THEN round(fd.vad_conc,2)::varchar
				                        	ELSE ''
				                        END AS "SPD_3",
				                        CASE WHEN fd.vad_conc > 0 THEN 'COP'
				                        	ELSE ''
				                        END AS "SPD_4"
				                    )
				                )
				           	)
				    	)
				    )
				) AS etiqueta_ext 
					INTO fact_det ,predio ,periodo ,n_secuencia ,cabecera.cod_cclo ,municipio ,empresa ,etiqueta_ext
				FROM factura_det fd
				INNER JOIN empresas e 
					ON e.cod_empr = fd.cod_empr 
				WHERE fd.cod_pred = xps_det.cod_pred
					AND fd.cod_peri = xps_det.cod_peri
					AND fd.nro_seq = xps_det.nro_seq
					AND fd.cod_conc = xps_det.cod_conc
					AND fd.consecutivo = xps_det.consecutivo
				GROUP BY fd.cod_pred ,fd.cod_peri ,fd.nro_seq ,fd.cod_munip, fd.cod_munip ,fd.cod_empr;
			
			RAISE NOTICE 'Factura: % ,Etiqueta EXT: %', fact_det ,etiqueta_ext;
			RETURN QUERY
            SELECT fact_det ,predio ,periodo ,n_secuencia ,ciclo ,municipio ,empresa ,etiqueta_ext;
            
            --almacenar xml por si solicitan conocer el xml enviado a la DIAN
           	INSERT INTO xml_fact_electronica VALUES (consec_log ,'FC',fact_det ,etiqueta_ext,now() ,periodo);
        END LOOP;
       
       FOR items IN (
			WITH importe AS (					
				SELECT fd.cod_conc ,fd.cod_peri ,fd.cod_pred ,fd.nro_seq ,fd.consecutivo ,fd.total_concp
								FROM factura_cab fc 
								INNER JOIN factura_det fd
									USING (cod_empr,cod_munip,cod_pred,cod_peri,nro_seq)
								WHERE fc.cod_empr = v_empresa
									AND fc.cod_peri = v_periodo	
									--AND fc.nro_seq = 1
									AND fc.nro_factura =  cabecera.nro_factura 
									AND fd.cod_conc NOT IN (SELECT dc.cod_conc FROM diferido_concepto dc)
									AND fd.cod_conc NOT IN (	
										WITH data AS (
							    			SELECT ep.codsaldfavor ,ep.cod_ajuste_dec ,999
							    				FROM empr_parametros ep
							    				WHERE ep.cod_empr = v_empresa
										)
										SELECT concepto
											FROM data,
											LATERAL (VALUES 
										    	(codsaldfavor),
										    	(cod_ajuste_dec),
										    	(999)
											) AS pivot(concepto)
									)
									--AND fc.cod_pred = v_predio   --SOLO PARA PRUEBAS INDIVIDUALES
									ORDER BY fd.consecutivo ASC 
			)
			SELECT * 
				FROM importe i
				WHERE i.total_concp > 0
					OR i.cod_conc IN (	
						WITH data AS (
			    			SELECT ep.cod_conc_ppal, ep.cod_conc_alc
			    				FROM empr_parametros ep
			    				WHERE ep.cod_empr = v_empresa
						)
						SELECT concepto
							FROM data,
							LATERAL (VALUES 
						    	(cod_conc_ppal),
						    	(cod_conc_alc)
							) AS pivot(concepto)
					)
			ORDER BY i.consecutivo ASC
		) LOOP
			--conteo_ite := conteo_ite + 1;
			RAISE NOTICE 'factura actual: %',fact_det;
			
			
			IF fact_det::numeric = 0 THEN 
				conteo_ite := conteo_ite + 1;
				RAISE NOTICE 'Conteo para ITEM inicial: %', conteo_ite;
			ELSIF fact_ant::numeric = fact_det::numeric THEN 
				conteo_ite := conteo_ite + 1;
				RAISE NOTICE 'Conteo para ITEM: %', conteo_ite;
				RAISE NOTICE 'factura cabecera: %',cabecera.nro_factura;
				RAISE NOTICE 'factura actual: %',fact_det;
			ELSE 
				conteo_ite := 1;
				RAISE NOTICE 'Conteo para ITEM: %', conteo_ite;
			END IF;
			
			SELECT cabecera.nro_factura ,fd.cod_pred ,fd.cod_peri ,fd.nro_seq ,fd.cod_munip, fd.cod_munip ,fd.cod_empr
					,xmlelement(name "ITEMS",
					xmlagg(
						xmlelement(name "ITE",
							xmlforest(
								conteo_ite AS "ITE_1",
								--2025-07-01 JAM: se agrega validación para cuando solo tenga el concepto consumo con importe cero.
								CASE WHEN fd.total_concp = 0 THEN 1 
									ELSE round(fd.canti_fija,2) 
								END AS "ITE_2",
								'94' AS "ITE_3",
								round(fd.total_concp+COALESCE(fd.vad_conc,0),2) AS "ITE_4",
								'COP' AS "ITE_5",
								round(fd.total_concp+COALESCE(fd.vad_conc,0)-COALESCE(fd.subapo_cons+fd.subapo_cfijo,0),2) AS "ITE_6",
								'COP' AS "ITE_7",
								round(fd.canti_fija,0)AS "ITE_9",
								'94' AS "ITE_10",
								(SELECT c.cod_sap FROM concepto c WHERE c.cod_conc = fd.cod_conc) AS "ITE_11",
								fd.cod_conc AS "ITE_13",
								round(fd.total_concp+COALESCE(fd.vad_conc,0),2)||'|' AS "ITE_14"
							),
							xmlelement(
			     				name "DIT",
								xmlforest(
				    				fd.descripcion AS "DIT_1"
				                )
				  			),
							CASE WHEN round(fd.total_concp+COALESCE(fd.vad_conc,0),2) = 0 THEN 
				  				xmlelement(
				     				name "IPA",
									xmlforest(
					    				3 AS "IPA_1",
					    				round(fd.total_concp+COALESCE(fd.vad_conc,0),2) AS "IPA_2",
					    				'COP' AS "IPA_3"
					                )
					  			)
					  		END,
				  			xmlelement(
			     				name "IDI",
								xmlforest(
				    				'0' AS "IDI_1"
				                )
				  			),
				  			xmlelement(
			     				name "IAE",
								xmlforest(
				    				fd.cod_conc AS "IAE_1",
				    				'999' AS "IAE_2",
				    				'Estándar de adopción del contribuyente' AS "IAE_4"
				                )
				  			),
							CASE WHEN round(COALESCE(fd.vad_conc,0),2) <> 0 THEN 
								xmlelement(
				     				name "IDE",
									xmlforest(
					    				conteo_ite AS "IDE_1",
										1 AS "IDE_2",
										CASE WHEN round(COALESCE(fd.vad_conc,0),2) < 0 THEN 'false' ELSE 'true' END AS "IDE_3",
										CASE WHEN round(COALESCE(fd.vad_conc,0),2) < 0 THEN 'Descuento' ELSE 'Cargo' END AS "IDE_4",
										CASE WHEN round(COALESCE(fd.vad_conc,0),2) < 0 THEN round((COALESCE(fd.vad_conc,0)*-1),2)::varchar
											ELSE round(COALESCE(fd.vad_conc,0),2)::varchar END AS "IDE_6",
										'COP' AS "IDE_7",
										round(fd.total_concp+COALESCE(fd.vad_conc,0)-COALESCE(fd.subapo_cons,0+fd.subapo_cfijo),2) AS "IDE_8",
										'COP' AS "IDE_9"
					                )
					  			)
							END,
							CASE WHEN round((fd.subapo_cons+fd.subapo_cfijo),2) <> 0 THEN 
								xmlelement(
				     				name "IDE",
									xmlforest(
					    				conteo_ite+1 AS "IDE_1",
										1 AS "IDE_2",
										CASE WHEN COALESCE(fd.subapo_cons+fd.subapo_cfijo,0) < 0 THEN 'false' ELSE 'true' END AS "IDE_3",
										CASE WHEN COALESCE(fd.subapo_cons+fd.subapo_cfijo,0) < 0 THEN 'Subsidio' ELSE 'Contribucci�n' END AS "IDE_4",
										CASE WHEN (fd.subapo_cons+fd.subapo_cfijo) < 0 THEN round(((fd.subapo_cons+fd.subapo_cfijo)*-1),2)::varchar
											ELSE round((fd.subapo_cons+fd.subapo_cfijo),2)::varchar END AS "IDE_6",
										'COP' AS "IDE_7",
										round(fd.total_concp+COALESCE(fd.vad_conc,0)-COALESCE(fd.subapo_cons,0+fd.subapo_cfijo),2) AS "IDE_8",
										'COP' AS "IDE_9"
					                )
					  			)
							END
						)
					)
				) AS etiqueta_ext 
					INTO fact_det ,predio ,periodo ,n_secuencia ,cabecera.cod_cclo ,municipio ,empresa ,etiqueta_ext
				FROM factura_det fd
				INNER JOIN empresas e 
					ON e.cod_empr = fd.cod_empr 
				WHERE fd.cod_pred = items.cod_pred
					AND fd.cod_peri = items.cod_peri
					AND fd.nro_seq = items.nro_seq
					AND fd.cod_conc = items.cod_conc
					AND fd.consecutivo = items.consecutivo
				GROUP BY fd.cod_pred ,fd.cod_peri ,fd.nro_seq ,fd.cod_munip, fd.cod_munip ,fd.cod_empr;
				
			fact_ant := fact_det;
			RAISE NOTICE 'Factura: % ,Etiqueta EXT: %', fact_det ,etiqueta_ext;
			RETURN QUERY
            SELECT fact_det ,predio ,periodo ,n_secuencia ,ciclo ,municipio ,empresa ,etiqueta_ext;
           
           --almacenar xml por si solicitan conocer el xml enviado a la DIAN
           	INSERT INTO xml_fact_electronica VALUES (consec_log ,'FC',fact_det ,etiqueta_ext,now() ,periodo);
        END LOOP;

		WITH datos AS (
		    SELECT fpc.nro_factura ,fpc.cod_pred ,fpc.cod_empr ,fpc.cod_munip ,fpc.cod_peri ,fpc.cod_cclo ,fpc.nro_seq
			    FROM factura_cab fpc
			    INNER JOIN municipio m 
			        ON m.cod_empr = fpc.cod_empr
			        AND m.cod_munip = fpc.cod_munip 
			    WHERE fpc.nro_factura = cabecera.nro_factura
		)
		SELECT (SELECT nro_factura FROM datos) AS factura ,(SELECT cod_pred FROM datos) AS predio ,(SELECT cod_peri FROM datos) AS periodos 
				,(SELECT nro_seq FROM datos) n_secuencia ,(SELECT cod_cclo FROM datos) AS ciclo ,(SELECT cod_munip FROM datos) municipio ,(SELECT cod_empr FROM datos) AS empresa
	    		,'<EXTENSIONES><EXT><EXT_1>GRAFICA</EXT_1><EGC><EGC_1>GRAFICA</EGC_1>' ||
			    STRING_AGG(
			        '<ECA>' ||
			        '<ECA_1>' || mes || '</ECA_1>' ||
			        '<ECA_2>' || consumo || '</ECA_2>' ||
			        '</ECA>', 
			        ''
			    ) ||
			    '</EGC></EXT></EXTENSIONES>' AS resultado
			FROM (
			    SELECT h.periodo, h.mes, h.consumo
			    FROM historico((SELECT cod_pred::int FROM datos), (SELECT cod_empr FROM datos), (SELECT cod_munip::int FROM datos), (SELECT cod_peri::int FROM datos), (SELECT cod_cclo FROM datos)) h
			    ORDER BY h.periodo ASC
			) etiqueta_ext 
			INTO factura ,predio ,periodo ,n_secuencia ,cabecera.cod_cclo ,municipio ,empresa ,etiqueta_ext;
	
		RAISE NOTICE 'Factura: % ,Etiqueta NOT: %', factura ,etiqueta_ext;
		RETURN QUERY
	  	SELECT factura ,predio ,periodo ,n_secuencia ,ciclo ,municipio ,empresa ,etiqueta_ext;
	           
		--almacenar xml por si solicitan conocer el xml enviado a la DIAN
	  	INSERT INTO xml_fact_electronica VALUES (consec_log ,'FC',factura ,etiqueta_ext,now() ,periodo);
       
        END IF;
    END LOOP;
   
   	RAISE NOTICE 'total de facturas: %', conteo;
    
END;
$function$
;
