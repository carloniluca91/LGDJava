/*
$LastChangedDate: 2016-07-13 18:19:39 +0200 (mer, 13 lug 2016) $
$Rev: 489 $		
$Author: arzelenko $	

Sono coinvolti i seguenti parametri:
		-cicli_ndg_path
		-tlbgaran_path
		-data_da
		-numero_mesi_1
		-data_a
		-numero_mesi_2
		-frapp_puma_outdir
		
*/

register LeastDate.jar;

/*
carico il file di cicli
*/
tlbcidef = LOAD '$cicli_ndg_path'
      USING PigStorage(';') 
  	  AS (
			codicebanca: chararray
		   ,ndgprincipale: chararray
		   ,datainiziodef: int
		   ,datafinedef: int
		   ,datainiziopd: chararray
		   ,datainizioristrutt: chararray
		   ,datainizioinc: chararray
		   ,datainiziosoff: chararray 
		   ,c_key: chararray
		   ,tipo_segmne: chararray
		   ,sae_segm: chararray
		   ,rae_segm: chararray
		   ,segmento: chararray
		   ,tp_ndg: chararray
		   ,provincia_segm: chararray
		   ,databilseg: chararray
		   ,strbilseg: chararray
		   ,attivobilseg: chararray
		   ,fatturbilseg: chararray
           ,ndg_collegato : chararray
		   ,codicebanca_collegato : chararray
		   ,cd_collegamento: chararray
		   ,cd_fiscale: chararray
		   ,dt_rif_udct           : int
         );

cicli_ndg_princ = FILTER tlbcidef BY cd_collegamento IS NULL;

cicli_ndg_coll = FILTER tlbcidef BY cd_collegamento IS NOT NULL;

/*
carico la tabella TLBGARAN
*/

tlbgaran = LOAD '$tlbgaran_path'
			USING PigStorage (';')
			AS (
			     cd_istituto       :chararray
			     ,ndg              :chararray
			     ,sportello        :chararray
			     ,dt_riferimento   :int
			     ,conto_esteso     :chararray
			     ,cd_puma2         :chararray
			     ,ide_garanzia     :chararray
			     ,importo          :chararray
			     ,fair_value       :chararray
				);
				
/*
metto in join le due tabelle 
*/				
tlbcidef_princ_tlbgaran_join = JOIN  tlbgaran BY (cd_istituto, ndg), cicli_ndg_princ BY (codicebanca_collegato, ndg_collegato);

/* 
join dei collegati per ist,ndg e data per prendere le anagrafiche per le sole date presenti in ucol 
*/
tlbcidef_coll_tlbgaran_join = JOIN  tlbgaran BY (cd_istituto, ndg, dt_riferimento), cicli_ndg_coll BY (codicebanca_collegato, ndg_collegato, dt_rif_udct);

/*filtro la tabella con la inizio fine ciclo */
tlbcidef_princ_tlbgaran_join_filt = FILTER tlbcidef_princ_tlbgaran_join 
BY ToDate( (chararray)dt_riferimento,'yyyyMMdd') >= ToDate( (chararray)datainiziodef,'yyyyMMdd' ) 
and SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING( (chararray)LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd') ,$data_a),0,6 );

tlbcidef_tlbgaran_princ = FOREACH tlbcidef_princ_tlbgaran_join_filt
                              GENERATE 
		                          tlbgaran::cd_istituto			 AS cd_isti
						         ,tlbgaran::ndg					 AS ndg				
						         ,tlbgaran::sportello			 AS sportello		
						         ,tlbgaran::dt_riferimento		 AS dt_riferimento	
						         ,tlbgaran::conto_esteso		 AS conto_esteso	
						         ,tlbgaran::cd_puma2			 AS cd_puma2		
						         ,tlbgaran::ide_garanzia		 AS ide_garanzia	
						         ,tlbgaran::importo				 AS importo			
						         ,tlbgaran::fair_value			 AS fair_value	
						         ,cicli_ndg_princ::codicebanca	 AS codicebanca	
							     ,cicli_ndg_princ::ndgprincipale AS ndgprincipale
								 ,cicli_ndg_princ::datainiziodef AS	datainiziodef
                            ;
                            
tlbcidef_tlbgaran_coll = FOREACH tlbcidef_coll_tlbgaran_join
                              GENERATE 
		                          tlbgaran::cd_istituto			 AS cd_isti
						         ,tlbgaran::ndg					 AS ndg				
						         ,tlbgaran::sportello			 AS sportello		
						         ,tlbgaran::dt_riferimento		 AS dt_riferimento	
						         ,tlbgaran::conto_esteso		 AS conto_esteso	
						         ,tlbgaran::cd_puma2			 AS cd_puma2		
						         ,tlbgaran::ide_garanzia		 AS ide_garanzia	
						         ,tlbgaran::importo				 AS importo			
						         ,tlbgaran::fair_value			 AS fair_value	
						         ,cicli_ndg_coll::codicebanca	 AS codicebanca	
							     ,cicli_ndg_coll::ndgprincipale	 AS ndgprincipale
								 ,cicli_ndg_coll::datainiziodef	 AS	datainiziodef
                            ;
                            

cicli_all = UNION tlbcidef_tlbgaran_princ
                 ,tlbcidef_tlbgaran_coll
                 ;

/*ottengo la mapping finale*/

frapp_puma_out = DISTINCT ( FOREACH cicli_all
						    GENERATE
								  cd_isti
						         ,ndg				
						         ,sportello		
						         ,dt_riferimento	
						         ,conto_esteso	
						         ,cd_puma2		
						         ,ide_garanzia	
						         ,importo			
						         ,fair_value	
						         ,codicebanca	
							     ,ndgprincipale
								 ,datainiziodef
                           );
		
STORE  frapp_puma_out INTO '$frapp_puma_outdir' USING PigStorage(';');
