/*
$LastChangedDate: 2016-01-08 16:13:43 +0100 (ven, 08 gen 2016) $
$Rev: 237 $		
$Author: davitali $	

Sono coinvolti i seguenti parametri:
		-tlbmign_path
		-racc_inc_outdir
		
*/
  
/* Accedo alla tabella TLBMIGN*/

tlbmign = LOAD '$tlbmign_path'
			USING PigStorage (';')
			AS (
				  cd_isti_ced     :chararray 
				 ,ndg_ced         :chararray 
				 ,cd_abi_ced      :chararray 
				 ,cd_isti_ric     :chararray 
				 ,ndg_ric         :chararray 
				 ,cd_abi_ric      :chararray
				 ,fl01_anag       :chararray
				 ,fl02_anag       :chararray
				 ,fl03_anag       :chararray
				 ,fl04_anag       :chararray
				 ,fl05_anag       :chararray
				 ,fl06_anag       :chararray
				 ,fl07_anag       :chararray
				 ,fl08_anag       :chararray
				 ,fl09_anag       :chararray
				 ,fl10_anag       :chararray
				 ,cod_migraz      :chararray
				 ,data_migraz     :chararray
				 ,data_ini_appl   :chararray
				 ,data_fin_appl   :chararray
				 ,data_ini_appl2  :chararray 
			   );

/*incremento di un mese la data migrazione*/
			   
tlbmign_for = FOREACH tlbmign

				GENERATE 
				    cd_isti_ric as cd_isti_ric
				   ,ndg_ric as ndg_ric
				   ,cd_isti_ced as cd_isti_ced
				   ,ndg_ced as ndg_ced
				   ,AddDuration(ToDate(data_migraz,'yyyyMMdd'),'P1M') AS month_up
				   ;


racc_inc_out = FOREACH tlbmign_for
				GENERATE
					  cd_isti_ric                   AS  ist_ric_inc             
					  ,ndg_ric                      AS  ndg_ric_inc             
					  ,null                           AS  num_ric_inc             
					  ,cd_isti_ced                  AS  ist_ced_inc             
					  ,ndg_ced                      AS  ndg_ced_inc             
					  ,null                           AS  num_ced_inc             
					  ,month_up	                    AS data_fine_primo_mese_ric
					;
					
STORE racc_inc_out INTO '$racc_inc_outdir' USING PigStorage (';');
 					
