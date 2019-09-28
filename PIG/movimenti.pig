/*
$LastChangedDate: 2016-01-08 16:13:43 +0100 (ven, 08 gen 2016) $
$Rev: 237 $		
$Author: davitali $	

Sono coinvolti i seguenti parametri:
		-cicli_ndg_path
		-tlbmovconta_path
		-data_osservazione
		-mov_outdir
		-periodo
		
*/
     
/*
carico la tabella MOVCONTA
*/

tlbmovconta = LOAD '$tlbmovconta_path'
			USING PigStorage (';')
			AS (
				 mo_dt_riferimento       :chararray
				 ,mo_istituto            :chararray
				 ,mo_ndg                 :chararray
				 ,mo_sportello           :chararray
				 ,mo_conto               :chararray
				 ,mo_conto_esteso        :chararray
				 ,mo_num_soff            :chararray
				 ,mo_cat_rapp_soff       :chararray
				 ,mo_fil_rapp_soff       :chararray
				 ,mo_num_rapp_soff       :chararray
				 ,mo_id_movimento        :chararray
				 ,mo_categoria           :chararray
				 ,mo_causale             :chararray
				 ,mo_dt_contabile        :int
				 ,mo_dt_valuta           :chararray
				 ,mo_imp_movimento       :chararray
				 ,mo_flag_extracont      :chararray
				 ,mo_flag_storno         :chararray
				 ,mo_ndg_principale		 :chararray
				 ,mo_dt_inizio_ciclo	 :chararray
				);
				
				
/*filtro in base alla data di osservazione*/				
				
tlbmovconta_filter = FILTER tlbmovconta BY mo_dt_contabile <= $data_osservazione;		

/*ottengo la mappatura finale*/

mov_out = FOREACH tlbmovconta_filter
          GENERATE
					  mo_istituto	         AS  istituto
					 ,mo_ndg				 AS  ndg 					
					 ,mo_dt_riferimento      AS  datariferimento 
					 ,mo_sportello			 AS	 sportello					 
					 ,mo_conto_esteso		 AS	 conto				
					 ,mo_num_soff			 AS	 numerosofferenza				
					 ,mo_cat_rapp_soff		 AS	 catrappsoffer				
					 ,mo_fil_rapp_soff		 AS	 filrappsoffer				
					 ,mo_num_rapp_soff		 AS	 numrappsoffer				
					 ,mo_id_movimento		 AS	 idmovimento				
					 ,mo_categoria			 AS	 categoria				
					 ,mo_causale			 AS	 casuale					
					 ,mo_dt_contabile		 AS	 dtcontab				
					 ,mo_dt_valuta			 AS	 dtvaluta				
					 ,mo_imp_movimento		 AS	 importo				
					 ,mo_flag_extracont	     AS	 flagextracontab					
					 ,mo_flag_storno		 AS	 flagstorno
					;

mov_out_dist =   DISTINCT ( 
             FOREACH mov_out
			 GENERATE
			  istituto
			 ,ndg 					
			 ,datariferimento 
			 ,sportello					 
			 ,conto				
			 ,numerosofferenza				
			 ,catrappsoffer				
			 ,filrappsoffer				
			 ,numrappsoffer				
			 ,idmovimento				
			 ,categoria				
			 ,casuale					
			 ,dtcontab				
			 ,dtvaluta				
			 ,importo				
			 ,flagextracontab					
			 ,flagstorno
            );

STORE mov_out_dist INTO '$mov_outdir' USING PigStorage (';');
