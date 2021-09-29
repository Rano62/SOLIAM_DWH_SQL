SELECT UNIQUE val.IEX_VAL,  val.VAL_ZLB3,val.VAL_PCO ,ptf.CTI_CPT
FROM DWH.FACT_INV_POS_WITH_NULL pos
JOIN dim_ptf ptf ON
	pos.ptf_key = ptf.ptf_key AND
	ptf.PTF_DER_VER_SWI='Y'
join dim_inv inv on inv.inv_key = pos.inv_key
AND inv.DAT_INV='30/06/2020' 
JOIN DIM_VAL val ON
val.val_key = pos.val_key AND
val.VAL_DER_VER_SWI='Y' /*AND val.IEX_VAL='DE000A2TSTE8'*/
AND val.val_str_cou IS NULL 