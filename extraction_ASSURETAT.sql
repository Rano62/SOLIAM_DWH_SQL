set term off;
set autotrace off;
set verify off;
set colsep ';';
set echo off;
set feedback off;
set heading off; 
set pagesize 0;
set linesize 10000;
set termout off; 
set trimspool on;

ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY';


WHENEVER SQLERROR EXIT FAILURE;

spool Extraction_ASSURETAT_&&2&&3&&4..csv

select 
'ENTITE;PTF;IDENTIFIANT;VALEUR;LIBELLE_VALEUR_ABR;LIBELLE_VALEUR;DEVISE_COT;DEVISE_VAL;PLACE_COT;LIBELLE_PLACE_COTATION;NOMINAL_PIECE;MCP;PERIODICITE;DATE_FIN_COUPON;DATE_REMB;PRIX_REMB;TAUX_FIXE;TAUX_VAR;MNEMONIQUE_EMETTEUR;NOM_EMETTEUR;SECTEUR_ECO;PAYS_EMETTEUR;TYPE_VALEUR;SOUS_TYPE_VALEUR;DEVISE;DATE_INVENTAIRE;CIC;LIBELLE_CIC;PLACE_COT_LIQ;DURATION;SENSIBILITE;CIC_LIQ;CODE_LEI;CODE_NACE;NOTATION;NOTA_SP;NOTA_MOODYS;NOTA_FITCH;QTE_DETENUE;VALACQ;SURCOTE;DECOTE;RVATIT;VALNET;EVA_MON_EVAEXC;CC;REVENUS'
from dual;

select ENT ||';'|| PTF ||';'|| NIE ||';'|| VALEUR ||';'|| RESTE1 ||';'|| QTE_DET ||';'|| RESTE2
from (
			select 
				trim(ent.ent_ent) as ENT		, trim(ptf.cti_cpt) as PTF			, trim(val.val_nie) as  NIE , trim(val.iex_val) 	as VALEUR				, trim(val.val_lib_abr)  			||';'||
				trim(val.val_lib) 			||';'|| trim(val.val_dev_cot) 			||';'|| trim(val.val_dev) 	||';'|| trim(val.val_pco) 						||';'|| trim(L1.lib) 					||';'||
				trim(val.val_nom_monpce) 	||';'|| trim(val.val_mcp) 				||';'|| trim(val.val_per) 	||';'|| trim(val.val_dat_fincou) 				||';'|| trim(val.val_dat_rbt) 			||';'||
				trim(val.val_cou_rbt) 		||';'|| trim(val.pec_tfx) 				||';'|| trim(val.pec_tta) 	||';'|| trim(val.tie_emt_mne) 					||';'|| trim(val.tie_emt_nom) 			||';'||
				trim(val.tie_emt_sec) 		||';'|| trim(val.tie_emt_pay) 			||';'|| trim(val.val_tvl) 	||';'|| trim(val.val_stv) 						||';'|| trim(dev.dev_iso) 				||';'||
				trim(inv.dat_inv) 			||';'|| trim(insfin.dimval_val_zlb4) 	||';'|| trim(L2.lib) 		||';'|| trim(insfin.dimval_val_zlb8) 			||';'|| trim(insfin.dimval_val_zlb9) 	||';'||
				trim(insfin.dimval_val_zlb10) ||';'|| trim(insfin.dimval_val_zlb11) ||';'|| trim(insfin.dimval_val_zlb12) 										||';'|| trim(insfin.dimval_val_zlb13) 	||';'||
				trim(val_str_codrat) 		||';'|| trim(val_ras) 					||';'|| trim(val_ram) 		||';'|| trim(val_rfi) 	as RESTE1						,
				sum(decode(pos.qte_det,null,0,pos.qte_det)) as QTE_DET				,
				sum(decode(pos.dex_valacq,null,0,pos.dex_valacq)) 					||';'||
				sum(decode(pos.dex_surcotbi,null,0,pos.dex_surcotbi)) 				||';'||
				sum(decode(pos.dex_decotebi,null,0,pos.dex_decotebi)) 				||';'||
				sum(decode(pos.dex_rvatit,null,0,pos.dex_rvatit)) 					||';'||
				(sum(decode(pos.dex_valacq,null,0,pos.dex_valacq))  + sum(decode(pos.dex_surcotbi,null,0,pos.dex_surcotbi))  + sum(decode(pos.dex_decotebi,null,0,pos.dex_decotebi))  + sum(decode(pos.dex_rvatit,null,0,pos.dex_rvatit))) ||';'||
				max(decode(eva.eva_mon_evaexc,null,0,eva.eva_mon_evaexc))  || ';' || 
				abs(sum(decode(pos.dex_coupbrut,null,0,pos.dex_coupbrut))) || ';' ||
				abs(sum(decode(pos.dex_intbrut,null,0,pos.dex_intbrut) + decode(pos.dex_divbrut,null,0,pos.dex_divbrut))) 					as RESTE2
			from fact_inv_pos_with_null pos 
			join dim_dat_cal cal on pos.dat_key = cal.dat_key and cal.dat_jou = '&&1'
			join dim_ent ent on ent.ent_key = pos.ent_key
			join dim_ptf ptf on pos.ptf_key = ptf.ptf_key
			join dim_val val on val.val_key = pos.val_key  and val.val_der_ver_swi='Y' and val.val_str_cou is null
			join dim_ins_fin  insfin on insfin.ins_fin_key = val.val_key
			join dim_dev dev on dev.dev_key = pos.dev_val_key
			join dim_tva tva on pos.tva_key = tva.tva_key
			join dim_inv inv on inv.inv_key = pos.inv_key
			join dim_tdi tdi on tdi.tdi_key = pos.tdi_key
			left join fact_inv_eva eva on eva.dat_key = pos.dat_key and eva.val_key = pos.val_key and eva.ptf_key = pos.ptf_key and eva.tdi_key = pos.tdi_key and eva.tva_key = pos.tva_key and eva.ent_key = pos.ent_key
			left join DWHADMIN.libs L1 on L1.tab_name = 'PCO' AND L1.pk_cod_ext = val.val_pco AND L1.lang = 'FF'
			left join DWHADMIN.libs L2 on L2.tab_name = 'NZL003' AND L2.pk_cod_ext = insfin.dimval_val_zlb4 AND L2.lang = 'FF'
			group by  ent.ent_ent, ptf.cti_cpt, val.val_nie, val.iex_val, val.val_lib_abr, val.val_lib,
				val.val_dev_cot, val.val_dev, val.val_pco,
				L1.lib,
				val.val_nom_monpce, val.val_mcp,
				val.val_per, val.val_dat_fincou, val.val_dat_rbt, val.val_cou_rbt, val.pec_tfx,val.pec_tta,
				val.tie_emt_mne, val.tie_emt_nom, val.tie_emt_sec, val.tie_emt_pay,
				val.val_tvl, val.val_stv, dev.dev_iso, inv.dat_inv,
				insfin.dimval_val_zlb4, L2.lib,
				insfin.dimval_val_zlb8,
				insfin.dimval_val_zlb9,
				insfin.dimval_val_zlb10,
				insfin.dimval_val_zlb11,
				insfin.dimval_val_zlb12,
				insfin.dimval_val_zlb13,
				val_str_codrat,val_ras,val_ram,val_rfi
			order by ent.ent_ent asc ,ptf.cti_cpt asc,val.iex_val asc
	)
where not exists ( 
					select * 
					from (select 
							trim(ent.ent_ent) as ENT_OLD, 
							trim(ptf.cti_cpt) as PTF_OLD, 
							trim(val.val_nie) as NIE_OLD,
							trim(val.iex_val) as VALEUR_OLD, 
							trim(inv.dat_inv) , 
							sum(decode(pos.qte_det,null,0,pos.qte_det)) as QTEDET_OLD
							from fact_inv_pos_with_null pos 
							join dim_dat_cal cal on pos.dat_key = cal.dat_key and cal.dat_jou = to_date('31/12/' || (&&4 - 1),'DD/MM/YYYY') 
							join dim_ent ent on ent.ent_key = pos.ent_key
							join dim_ptf ptf on pos.ptf_key = ptf.ptf_key
							join dim_val val on val.val_key = pos.val_key  and val.val_der_ver_swi='Y' and  val.val_str_cou is null
							join dim_dev dev on dev.dev_key = pos.dev_val_key
							join dim_tva tva on pos.tva_key = tva.tva_key
							join dim_inv inv on inv.inv_key = pos.inv_key
							join dim_tdi tdi on tdi.tdi_key = pos.tdi_key
							group by  ent.ent_ent, ptf.cti_cpt, val.val_nie, val.iex_val, inv.dat_inv
                          )
						  where  QTEDET_OLD='0' and (ENT = ENT_OLD) and (PTF = PTF_OLD) and (NIE = NIE_OLD) and (VALEUR = VALEUR_OLD) and (QTE_DET = QTEDET_OLD)
                 )
;
commit;

spool off;

exit;
