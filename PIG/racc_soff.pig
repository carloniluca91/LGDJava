/*
$LastChangedDate: 2016-01-08 16:13:43 +0100 (ven, 08 gen 2016) $
$Rev: 237 $		
$Author: davitali $	

sono coinvolti i seguenti parametri:
		-dblabtlbjxd9_path
		-racc_soff_outdir
		
*/
		
/*
carico la tabella DBLAB_TLBJXD9
*/

dblabtlbjxd9 = LOAD '$dblabtlbjxd9_path' 
      USING PigStorage(';') 
      AS (
          istricsof            :chararray
          ,ndgricsof            :chararray
          ,numricsof            :chararray
          ,istcedsof            :chararray
          ,ndgcedsof            :chararray
          ,numcedsof            :chararray
          ,data_primo_fine_me   :chararray	 
		);
		
/* Ottengo già una tabella in uscita*/

racc_soff_out = FOREACH dblabtlbjxd9
				GENERATE	
                        istricsof				 AS  IST_RIC_SOF
                        ,ndgricsof				 AS  NDG_RIC_SOF
                        ,numricsof				 AS  NUM_RIC_SOF
                        ,istcedsof				 AS  IST_CED_SOF
                        ,ndgcedsof				 AS  NDG_CED_SOF
                        ,numcedsof				 AS  NUM_CED_SOF
                        ,data_primo_fine_me      AS  DATA_FINE_PRIMO_MESE_RIC
					   ;
					   
STORE racc_soff_out INTO '$racc_soff_outdir' USING PigStorage (';');
				   
