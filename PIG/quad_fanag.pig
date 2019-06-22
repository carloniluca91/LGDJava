/*
$LastChangedDate: 2016-06-01 10:53:23 +0200 (mer, 01 giu 2016) $
$Rev: 389 $		
$Author: arzelenko $	

Sono coinvolti i seguenti parametri:
		-cicli_ndg_path
		-tlbpratsoff_path
		-data_osservazione
		-soff_outdir
				
*/

/*
carico il file fanag prodotto da hadoop
*/
hadoop_fanag = LOAD '/app/drlgd/datarequest/dt/out/finale/$ufficio/fanag'
      USING PigStorage(';') 
  	  AS (
             codicebanca       :chararray
			,ndg               :chararray
			,datariferimento   :chararray
			,totaccordato      :chararray
			,totutilizzo       :chararray
			,totaccomortgage   :chararray
			,totutilmortgage   :chararray
			,totsaldi0063      :chararray
			,totsaldi0260      :chararray
			,naturagiuridica   :chararray
			,intestazione      :chararray
			,codicefiscale     :chararray
			,partitaiva        :chararray
			,sae               :chararray
			,rae               :chararray
			,ciae              :chararray
			,provincia         :chararray
			,provincia_cod     :chararray
			,sportello         :chararray
			,attrn011          :chararray
			,attrn175          :chararray
			,attrn186          :chararray
			,cdrapristr        :chararray
			,segmento          :chararray
			,cd_collegamento   :chararray
			,ndg_caponucleo    :chararray
			,flag_ristrutt     :chararray
			,codicebanca_princ :chararray
			,ndgprincipale     :chararray
			,datainiziodef     :chararray
         );
   
     
/*
carico la tabella MOVCONTA
*/

oldfanag_load = LOAD '/app/drlgd/store/oldfanag'
			USING PigStorage (';')
			AS (
				 CODICEBANCA        :chararray
				,NDG		        :chararray
				,DATARIFERIMENTO    :chararray
				,TOTACCORDATO	    :chararray
				,TOTUTILIZZO	    :chararray
				,TOTACCOMORTGAGE    :chararray
				,TOTUTILMORTGAGE    :chararray
				,TOTSALDI0063	    :chararray
				,TOTSALDI0260	    :chararray
				,NATURAGIURIDICA    :chararray
				,INTESTAZIONE	    :chararray
				,CODICEFISCALE	    :chararray
				,PARTITAIVA	        :chararray
				,SAE		        :chararray
				,RAE		        :chararray
				,CIAE		        :chararray
				,PROVINCIA_COD	    :chararray
				,PROVINCIA	        :chararray
				,SPORTELLO	        :chararray
				,ATTRN011	        :chararray
				,ATTRN175	        :chararray
				,ATTRN186	        :chararray
				,CDRAPRISTR 	    :chararray
				,SEGMENTO	        :chararray
				);

fcoll = LOAD '/app/drlgd/datarequest/dt/out/quad/$ufficio/fcoll'
			USING PigStorage (';')
			AS (
				 CODICEBANCA        :chararray
				,NDGPRINCIPALE	    :chararray
				,DATAINIZIODEF	    :chararray
				,DATAFINEDEF		:chararray
	            ,DATA_DEFAULT       :chararray
	            ,ISTITUTO_COLLEGATO :chararray
	            ,NDG_COLLEGATO      :chararray
	            ,DATA_COLLEGAMENTO  :chararray
	            ,CUMULO             :chararray
			   );

oldfanag_load_fcoll_join = JOIN oldfanag_load BY (CODICEBANCA, NDG, DATARIFERIMENTO)
                               ,fcoll BY (ISTITUTO_COLLEGATO, NDG_COLLEGATO, DATA_COLLEGAMENTO);

oldfanag = FOREACH oldfanag_load_fcoll_join
			     GENERATE
					 oldfanag_load::CODICEBANCA      as CODICEBANCA
					,oldfanag_load::NDG		         as NDG
					,oldfanag_load::DATARIFERIMENTO  as DATARIFERIMENTO
					,oldfanag_load::TOTACCORDATO	 as TOTACCORDATO
					,oldfanag_load::TOTUTILIZZO	     as TOTUTILIZZO
					,oldfanag_load::TOTACCOMORTGAGE  as TOTACCOMORTGAGE
					,oldfanag_load::TOTUTILMORTGAGE  as TOTUTILMORTGAGE
					,oldfanag_load::TOTSALDI0063	 as TOTSALDI0063
					,oldfanag_load::TOTSALDI0260	 as TOTSALDI0260
					,oldfanag_load::NATURAGIURIDICA  as NATURAGIURIDICA
					,oldfanag_load::INTESTAZIONE	 as INTESTAZIONE
					,oldfanag_load::CODICEFISCALE	 as CODICEFISCALE
					,oldfanag_load::PARTITAIVA	     as PARTITAIVA
					,oldfanag_load::SAE		         as SAE
					,oldfanag_load::RAE		         as RAE
					,oldfanag_load::CIAE		     as CIAE
					,oldfanag_load::PROVINCIA_COD	 as PROVINCIA_COD
					,oldfanag_load::PROVINCIA	     as PROVINCIA
					,oldfanag_load::SPORTELLO	     as SPORTELLO
					,oldfanag_load::ATTRN011	     as ATTRN011
					,oldfanag_load::ATTRN175	     as ATTRN175
					,oldfanag_load::ATTRN186	     as ATTRN186
					,oldfanag_load::CDRAPRISTR 	     as CDRAPRISTR
					,oldfanag_load::SEGMENTO         as SEGMENTO
                    ,fcoll::CODICEBANCA              as CODICEBANCA_PRINC
                    ,fcoll::NDGPRINCIPALE            as NDGPRINCIPALE
                    ,fcoll::DATAINIZIODEF            as DATAINIZIODEF
					;


/*unisco le due tabelle hadoop e old */
hadoop_fanag_oldfanag_join = JOIN hadoop_fanag BY (codicebanca_princ, ndgprincipale, datainiziodef, codicebanca, ndg, datariferimento) FULL OUTER
                                 ,oldfanag BY (CODICEBANCA_PRINC, NDGPRINCIPALE, DATAINIZIODEF, CODICEBANCA, NDG, DATARIFERIMENTO );

/*unisco le due tabelle hadoop e old solo per NDG */
hadoop_fanag_oldfanag_join_ndg = JOIN hadoop_fanag BY (codicebanca, ndg, datariferimento) FULL OUTER
                                 ,oldfanag BY (CODICEBANCA, NDG, DATARIFERIMENTO );

solo_hadoop_fanag_filter = FILTER hadoop_fanag_oldfanag_join BY oldfanag::CODICEBANCA IS NULL;	

solo_oldfanag_filter = FILTER hadoop_fanag_oldfanag_join BY hadoop_fanag::codicebanca IS NULL;


/*filtri per ramo NDG*/
solo_hadoop_fanag_filter_ndg = FILTER hadoop_fanag_oldfanag_join_ndg BY oldfanag::CODICEBANCA IS NULL;	

solo_oldfanag_filter_ndg = FILTER hadoop_fanag_oldfanag_join_ndg BY hadoop_fanag::codicebanca IS NULL;

/* Leggo la tlbucol */
tlbucol_load = LOAD '/app/dblab/store/ucol_*'   USING PigStorage(';')
          AS (
		      cd_istituto     : chararray
			 ,ndg             : chararray
			 ,cd_collegamento : chararray
			 ,ndg_collegato   : chararray
			 ,dt_riferimento  : chararray
			 );
			 
/* filtro il tipo di collegamento */
tlbucol_filtered = FILTER tlbucol_load BY cd_collegamento=='103' 
                                       OR cd_collegamento=='105' 
                                       OR cd_collegamento=='106' 
                                       OR cd_collegamento=='107' 
                                       OR cd_collegamento=='207';

/* join della parte solo in fanagold con ucol per scrivere solo quelli presenti COLLEGATO */
solo_oldfanag_filter_ucol_coll = JOIN solo_oldfanag_filter BY (CODICEBANCA, NDG, DATARIFERIMENTO)
                                     ,tlbucol_filtered BY (cd_istituto, ndg_collegato, dt_riferimento);
                                     
/* join della parte solo in fanagold con ucol per scrivere solo quelli presenti NDG */
solo_oldfanag_filter_ucol_princ = JOIN solo_oldfanag_filter BY (CODICEBANCA, NDG, DATARIFERIMENTO)
                                     ,tlbucol_filtered BY (cd_istituto, ndg, dt_riferimento);
                                     
/*join per ramo NDG*/
/* join della parte solo in fanagold con ucol per scrivere solo quelli presenti COLLEGATO */
solo_oldfanag_filter_ucol_coll_ndg = JOIN solo_oldfanag_filter_ndg BY (CODICEBANCA, NDG, DATARIFERIMENTO)
                                     ,tlbucol_filtered BY (cd_istituto, ndg_collegato, dt_riferimento);
                                     
/* join della parte solo in fanagold con ucol per scrivere solo quelli presenti NDG */
solo_oldfanag_filter_ucol_princ_ndg = JOIN solo_oldfanag_filter_ndg BY (CODICEBANCA, NDG, DATARIFERIMENTO)
                                     ,tlbucol_filtered BY (cd_istituto, ndg, dt_riferimento);

/*
abbinati_filter = FILTER hadoop_fanag_oldfanag_join BY hadoop_fanag::codicebanca IS NOT NULL AND oldfanag::CODICEBANCA IS NOT NULL
AND hadoop_fanag::codicebanca     != oldfanag::CODICEBANCA
AND hadoop_fanag::ndg             != oldfanag::NDG
AND hadoop_fanag::datariferimento != oldfanag::DATARIFERIMENTO
;
*/

/*ottengo la mappatura finale*/
hadoop_fanag_out = FOREACH solo_hadoop_fanag_filter
			     GENERATE
             '$ufficio'           as ufficio
            ,hadoop_fanag::codicebanca       
			,hadoop_fanag::ndg               
			,hadoop_fanag::datariferimento   
			,hadoop_fanag::totaccordato      
			,hadoop_fanag::totutilizzo       
			,hadoop_fanag::totaccomortgage   
			,hadoop_fanag::totutilmortgage   
			,hadoop_fanag::totsaldi0063      
			,hadoop_fanag::totsaldi0260      
			,hadoop_fanag::naturagiuridica   
			,hadoop_fanag::intestazione      
			,hadoop_fanag::codicefiscale     
			,hadoop_fanag::partitaiva        
			,hadoop_fanag::sae               
			,hadoop_fanag::rae               
			,hadoop_fanag::ciae              
			,hadoop_fanag::provincia         
			,hadoop_fanag::provincia_cod     
			,hadoop_fanag::sportello         
			,hadoop_fanag::attrn011          
			,hadoop_fanag::attrn175          
			,hadoop_fanag::attrn186          
			,hadoop_fanag::cdrapristr        
			,hadoop_fanag::segmento          
			,hadoop_fanag::cd_collegamento   
			,hadoop_fanag::ndg_caponucleo    
			,hadoop_fanag::flag_ristrutt
            ,hadoop_fanag::codicebanca_princ
			,hadoop_fanag::ndgprincipale
			,hadoop_fanag::datainiziodef
			,oldfanag::CODICEBANCA        
			,oldfanag::NDG		    
			,oldfanag::DATARIFERIMENTO    
			,oldfanag::TOTACCORDATO	    
			,oldfanag::TOTUTILIZZO	    
			,oldfanag::TOTACCOMORTGAGE    
			,oldfanag::TOTUTILMORTGAGE    
			,oldfanag::TOTSALDI0063	    
			,oldfanag::TOTSALDI0260	    
			,oldfanag::NATURAGIURIDICA    
			,oldfanag::INTESTAZIONE	    
			,oldfanag::CODICEFISCALE	    
			,oldfanag::PARTITAIVA	    
			,oldfanag::SAE		    
			,oldfanag::RAE		    
			,oldfanag::CIAE		    
			,oldfanag::PROVINCIA_COD	    
			,oldfanag::PROVINCIA	    
			,oldfanag::SPORTELLO	    
			,oldfanag::ATTRN011	    
			,oldfanag::ATTRN175	    
			,oldfanag::ATTRN186	    
			,oldfanag::CDRAPRISTR 	    
			,oldfanag::SEGMENTO
			,oldfanag::CODICEBANCA_PRINC
            ,oldfanag::NDGPRINCIPALE
            ,oldfanag::DATAINIZIODEF
            ;
            
/*ottengo la mappatura finale per ramo NDG*/
hadoop_fanag_out_ndg = FOREACH solo_hadoop_fanag_filter_ndg
			     GENERATE
             '$ufficio'           as ufficio
            ,hadoop_fanag::codicebanca       
			,hadoop_fanag::ndg               
			,hadoop_fanag::datariferimento   
			,hadoop_fanag::totaccordato      
			,hadoop_fanag::totutilizzo       
			,hadoop_fanag::totaccomortgage   
			,hadoop_fanag::totutilmortgage   
			,hadoop_fanag::totsaldi0063      
			,hadoop_fanag::totsaldi0260      
			,hadoop_fanag::naturagiuridica   
			,hadoop_fanag::intestazione      
			,hadoop_fanag::codicefiscale     
			,hadoop_fanag::partitaiva        
			,hadoop_fanag::sae               
			,hadoop_fanag::rae               
			,hadoop_fanag::ciae              
			,hadoop_fanag::provincia         
			,hadoop_fanag::provincia_cod     
			,hadoop_fanag::sportello         
			,hadoop_fanag::attrn011          
			,hadoop_fanag::attrn175          
			,hadoop_fanag::attrn186          
			,hadoop_fanag::cdrapristr        
			,hadoop_fanag::segmento          
			,hadoop_fanag::cd_collegamento   
			,hadoop_fanag::ndg_caponucleo    
			,hadoop_fanag::flag_ristrutt
            ,hadoop_fanag::codicebanca_princ
			,hadoop_fanag::ndgprincipale
			,hadoop_fanag::datainiziodef
			,oldfanag::CODICEBANCA        
			,oldfanag::NDG		    
			,oldfanag::DATARIFERIMENTO    
			,oldfanag::TOTACCORDATO	    
			,oldfanag::TOTUTILIZZO	    
			,oldfanag::TOTACCOMORTGAGE    
			,oldfanag::TOTUTILMORTGAGE    
			,oldfanag::TOTSALDI0063	    
			,oldfanag::TOTSALDI0260	    
			,oldfanag::NATURAGIURIDICA    
			,oldfanag::INTESTAZIONE	    
			,oldfanag::CODICEFISCALE	    
			,oldfanag::PARTITAIVA	    
			,oldfanag::SAE		    
			,oldfanag::RAE		    
			,oldfanag::CIAE		    
			,oldfanag::PROVINCIA_COD	    
			,oldfanag::PROVINCIA	    
			,oldfanag::SPORTELLO	    
			,oldfanag::ATTRN011	    
			,oldfanag::ATTRN175	    
			,oldfanag::ATTRN186	    
			,oldfanag::CDRAPRISTR 	    
			,oldfanag::SEGMENTO
			,oldfanag::CODICEBANCA_PRINC
            ,oldfanag::NDGPRINCIPALE
            ,oldfanag::DATAINIZIODEF
            ;
					
oldfanag_out_coll = FOREACH solo_oldfanag_filter_ucol_coll
			     GENERATE
		     '$ufficio'           as ufficio
		    ,hadoop_fanag::codicebanca       
			,hadoop_fanag::ndg               
			,hadoop_fanag::datariferimento   
			,hadoop_fanag::totaccordato      
			,hadoop_fanag::totutilizzo       
			,hadoop_fanag::totaccomortgage   
			,hadoop_fanag::totutilmortgage   
			,hadoop_fanag::totsaldi0063      
			,hadoop_fanag::totsaldi0260      
			,hadoop_fanag::naturagiuridica   
			,hadoop_fanag::intestazione      
			,hadoop_fanag::codicefiscale     
			,hadoop_fanag::partitaiva        
			,hadoop_fanag::sae               
			,hadoop_fanag::rae               
			,hadoop_fanag::ciae              
			,hadoop_fanag::provincia         
			,hadoop_fanag::provincia_cod     
			,hadoop_fanag::sportello         
			,hadoop_fanag::attrn011          
			,hadoop_fanag::attrn175          
			,hadoop_fanag::attrn186          
			,hadoop_fanag::cdrapristr        
			,hadoop_fanag::segmento          
			,hadoop_fanag::cd_collegamento   
			,hadoop_fanag::ndg_caponucleo    
			,hadoop_fanag::flag_ristrutt
            ,hadoop_fanag::codicebanca_princ
			,hadoop_fanag::ndgprincipale
			,hadoop_fanag::datainiziodef
			,oldfanag::CODICEBANCA        
			,oldfanag::NDG		    
			,oldfanag::DATARIFERIMENTO    
			,oldfanag::TOTACCORDATO	    
			,oldfanag::TOTUTILIZZO	    
			,oldfanag::TOTACCOMORTGAGE    
			,oldfanag::TOTUTILMORTGAGE    
			,oldfanag::TOTSALDI0063	    
			,oldfanag::TOTSALDI0260	    
			,oldfanag::NATURAGIURIDICA    
			,oldfanag::INTESTAZIONE	    
			,oldfanag::CODICEFISCALE	    
			,oldfanag::PARTITAIVA	    
			,oldfanag::SAE		    
			,oldfanag::RAE		    
			,oldfanag::CIAE		    
			,oldfanag::PROVINCIA_COD	    
			,oldfanag::PROVINCIA	    
			,oldfanag::SPORTELLO	    
			,oldfanag::ATTRN011	    
			,oldfanag::ATTRN175	    
			,oldfanag::ATTRN186	    
			,oldfanag::CDRAPRISTR 	    
			,oldfanag::SEGMENTO
			,oldfanag::CODICEBANCA_PRINC
            ,oldfanag::NDGPRINCIPALE
            ,oldfanag::DATAINIZIODEF
					;
					
oldfanag_out_princ = FOREACH solo_oldfanag_filter_ucol_princ
			     GENERATE
		     '$ufficio'           as ufficio
		    ,hadoop_fanag::codicebanca       
			,hadoop_fanag::ndg               
			,hadoop_fanag::datariferimento   
			,hadoop_fanag::totaccordato      
			,hadoop_fanag::totutilizzo       
			,hadoop_fanag::totaccomortgage   
			,hadoop_fanag::totutilmortgage   
			,hadoop_fanag::totsaldi0063      
			,hadoop_fanag::totsaldi0260      
			,hadoop_fanag::naturagiuridica   
			,hadoop_fanag::intestazione      
			,hadoop_fanag::codicefiscale     
			,hadoop_fanag::partitaiva        
			,hadoop_fanag::sae               
			,hadoop_fanag::rae               
			,hadoop_fanag::ciae              
			,hadoop_fanag::provincia         
			,hadoop_fanag::provincia_cod     
			,hadoop_fanag::sportello         
			,hadoop_fanag::attrn011          
			,hadoop_fanag::attrn175          
			,hadoop_fanag::attrn186          
			,hadoop_fanag::cdrapristr        
			,hadoop_fanag::segmento          
			,hadoop_fanag::cd_collegamento   
			,hadoop_fanag::ndg_caponucleo    
			,hadoop_fanag::flag_ristrutt
            ,hadoop_fanag::codicebanca_princ
			,hadoop_fanag::ndgprincipale
			,hadoop_fanag::datainiziodef
			,oldfanag::CODICEBANCA        
			,oldfanag::NDG		    
			,oldfanag::DATARIFERIMENTO    
			,oldfanag::TOTACCORDATO	    
			,oldfanag::TOTUTILIZZO	    
			,oldfanag::TOTACCOMORTGAGE    
			,oldfanag::TOTUTILMORTGAGE    
			,oldfanag::TOTSALDI0063	    
			,oldfanag::TOTSALDI0260	    
			,oldfanag::NATURAGIURIDICA    
			,oldfanag::INTESTAZIONE	    
			,oldfanag::CODICEFISCALE	    
			,oldfanag::PARTITAIVA	    
			,oldfanag::SAE		    
			,oldfanag::RAE		    
			,oldfanag::CIAE		    
			,oldfanag::PROVINCIA_COD	    
			,oldfanag::PROVINCIA	    
			,oldfanag::SPORTELLO	    
			,oldfanag::ATTRN011	    
			,oldfanag::ATTRN175	    
			,oldfanag::ATTRN186	    
			,oldfanag::CDRAPRISTR 	    
			,oldfanag::SEGMENTO
			,oldfanag::CODICEBANCA_PRINC
            ,oldfanag::NDGPRINCIPALE
            ,oldfanag::DATAINIZIODEF
					;
					
oldfanag_out = UNION oldfanag_out_coll
                    ,oldfanag_out_princ
                    ;

/*mappatura solo old per ramo NDG*/
oldfanag_out_coll_ndg = FOREACH solo_oldfanag_filter_ucol_coll_ndg
			     GENERATE
		     '$ufficio'           as ufficio
		    ,hadoop_fanag::codicebanca       
			,hadoop_fanag::ndg               
			,hadoop_fanag::datariferimento   
			,hadoop_fanag::totaccordato      
			,hadoop_fanag::totutilizzo       
			,hadoop_fanag::totaccomortgage   
			,hadoop_fanag::totutilmortgage   
			,hadoop_fanag::totsaldi0063      
			,hadoop_fanag::totsaldi0260      
			,hadoop_fanag::naturagiuridica   
			,hadoop_fanag::intestazione      
			,hadoop_fanag::codicefiscale     
			,hadoop_fanag::partitaiva        
			,hadoop_fanag::sae               
			,hadoop_fanag::rae               
			,hadoop_fanag::ciae              
			,hadoop_fanag::provincia         
			,hadoop_fanag::provincia_cod     
			,hadoop_fanag::sportello         
			,hadoop_fanag::attrn011          
			,hadoop_fanag::attrn175          
			,hadoop_fanag::attrn186          
			,hadoop_fanag::cdrapristr        
			,hadoop_fanag::segmento          
			,hadoop_fanag::cd_collegamento   
			,hadoop_fanag::ndg_caponucleo    
			,hadoop_fanag::flag_ristrutt
            ,hadoop_fanag::codicebanca_princ
			,hadoop_fanag::ndgprincipale
			,hadoop_fanag::datainiziodef
			,oldfanag::CODICEBANCA        
			,oldfanag::NDG		    
			,oldfanag::DATARIFERIMENTO    
			,oldfanag::TOTACCORDATO	    
			,oldfanag::TOTUTILIZZO	    
			,oldfanag::TOTACCOMORTGAGE    
			,oldfanag::TOTUTILMORTGAGE    
			,oldfanag::TOTSALDI0063	    
			,oldfanag::TOTSALDI0260	    
			,oldfanag::NATURAGIURIDICA    
			,oldfanag::INTESTAZIONE	    
			,oldfanag::CODICEFISCALE	    
			,oldfanag::PARTITAIVA	    
			,oldfanag::SAE		    
			,oldfanag::RAE		    
			,oldfanag::CIAE		    
			,oldfanag::PROVINCIA_COD	    
			,oldfanag::PROVINCIA	    
			,oldfanag::SPORTELLO	    
			,oldfanag::ATTRN011	    
			,oldfanag::ATTRN175	    
			,oldfanag::ATTRN186	    
			,oldfanag::CDRAPRISTR 	    
			,oldfanag::SEGMENTO
			,oldfanag::CODICEBANCA_PRINC
            ,oldfanag::NDGPRINCIPALE
            ,oldfanag::DATAINIZIODEF
					;
					
oldfanag_out_princ_ndg = FOREACH solo_oldfanag_filter_ucol_princ_ndg
			     GENERATE
		     '$ufficio'           as ufficio
		    ,hadoop_fanag::codicebanca       
			,hadoop_fanag::ndg               
			,hadoop_fanag::datariferimento   
			,hadoop_fanag::totaccordato      
			,hadoop_fanag::totutilizzo       
			,hadoop_fanag::totaccomortgage   
			,hadoop_fanag::totutilmortgage   
			,hadoop_fanag::totsaldi0063      
			,hadoop_fanag::totsaldi0260      
			,hadoop_fanag::naturagiuridica   
			,hadoop_fanag::intestazione      
			,hadoop_fanag::codicefiscale     
			,hadoop_fanag::partitaiva        
			,hadoop_fanag::sae               
			,hadoop_fanag::rae               
			,hadoop_fanag::ciae              
			,hadoop_fanag::provincia         
			,hadoop_fanag::provincia_cod     
			,hadoop_fanag::sportello         
			,hadoop_fanag::attrn011          
			,hadoop_fanag::attrn175          
			,hadoop_fanag::attrn186          
			,hadoop_fanag::cdrapristr        
			,hadoop_fanag::segmento          
			,hadoop_fanag::cd_collegamento   
			,hadoop_fanag::ndg_caponucleo    
			,hadoop_fanag::flag_ristrutt
            ,hadoop_fanag::codicebanca_princ
			,hadoop_fanag::ndgprincipale
			,hadoop_fanag::datainiziodef
			,oldfanag::CODICEBANCA        
			,oldfanag::NDG		    
			,oldfanag::DATARIFERIMENTO    
			,oldfanag::TOTACCORDATO	    
			,oldfanag::TOTUTILIZZO	    
			,oldfanag::TOTACCOMORTGAGE    
			,oldfanag::TOTUTILMORTGAGE    
			,oldfanag::TOTSALDI0063	    
			,oldfanag::TOTSALDI0260	    
			,oldfanag::NATURAGIURIDICA    
			,oldfanag::INTESTAZIONE	    
			,oldfanag::CODICEFISCALE	    
			,oldfanag::PARTITAIVA	    
			,oldfanag::SAE		    
			,oldfanag::RAE		    
			,oldfanag::CIAE		    
			,oldfanag::PROVINCIA_COD	    
			,oldfanag::PROVINCIA	    
			,oldfanag::SPORTELLO	    
			,oldfanag::ATTRN011	    
			,oldfanag::ATTRN175	    
			,oldfanag::ATTRN186	    
			,oldfanag::CDRAPRISTR 	    
			,oldfanag::SEGMENTO
			,oldfanag::CODICEBANCA_PRINC
            ,oldfanag::NDGPRINCIPALE
            ,oldfanag::DATAINIZIODEF
					;
					
oldfanag_out_ndg = UNION oldfanag_out_coll_ndg
                        ,oldfanag_out_princ_ndg
                    ;

/*
abbinati_out = FOREACH abbinati_filter
			     GENERATE
			 '$ufficio'           as ufficio
			,hadoop_fanag::codicebanca       
			,hadoop_fanag::ndg               
			,hadoop_fanag::datariferimento   
			,hadoop_fanag::totaccordato      
			,hadoop_fanag::totutilizzo       
			,hadoop_fanag::totaccomortgage   
			,hadoop_fanag::totutilmortgage   
			,hadoop_fanag::totsaldi0063      
			,hadoop_fanag::totsaldi0260      
			,hadoop_fanag::naturagiuridica   
			,hadoop_fanag::intestazione      
			,hadoop_fanag::codicefiscale     
			,hadoop_fanag::partitaiva        
			,hadoop_fanag::sae               
			,hadoop_fanag::rae               
			,hadoop_fanag::ciae              
			,hadoop_fanag::provincia         
			,hadoop_fanag::provincia_cod     
			,hadoop_fanag::sportello         
			,hadoop_fanag::attrn011          
			,hadoop_fanag::attrn175          
			,hadoop_fanag::attrn186          
			,hadoop_fanag::cdrapristr        
			,hadoop_fanag::segmento          
			,hadoop_fanag::cd_collegamento   
			,hadoop_fanag::ndg_caponucleo    
			,hadoop_fanag::flag_ristrutt
            ,hadoop_fanag::codicebanca_princ
			,hadoop_fanag::ndgprincipale
			,hadoop_fanag::datainiziodef
			,oldfanag::CODICEBANCA        
			,oldfanag::NDG		    
			,oldfanag::DATARIFERIMENTO    
			,oldfanag::TOTACCORDATO	    
			,oldfanag::TOTUTILIZZO	    
			,oldfanag::TOTACCOMORTGAGE    
			,oldfanag::TOTUTILMORTGAGE    
			,oldfanag::TOTSALDI0063	    
			,oldfanag::TOTSALDI0260	    
			,oldfanag::NATURAGIURIDICA    
			,oldfanag::INTESTAZIONE	    
			,oldfanag::CODICEFISCALE	    
			,oldfanag::PARTITAIVA	    
			,oldfanag::SAE		    
			,oldfanag::RAE		    
			,oldfanag::CIAE		    
			,oldfanag::PROVINCIA_COD	    
			,oldfanag::PROVINCIA	    
			,oldfanag::SPORTELLO	    
			,oldfanag::ATTRN011	    
			,oldfanag::ATTRN175	    
			,oldfanag::ATTRN186	    
			,oldfanag::CDRAPRISTR 	    
			,oldfanag::SEGMENTO
			,oldfanag::CODICEBANCA_PRINC
            ,oldfanag::NDGPRINCIPALE
            ,oldfanag::DATAINIZIODEF	    
            ;
*/
 
STORE hadoop_fanag_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/fanag/fanag_solo_hadoop' USING PigStorage (';');

STORE oldfanag_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/fanag/fanag_solo_old' USING PigStorage (';');

STORE hadoop_fanag_out_ndg INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/fanag_ndg/fanag_solo_hadoop' USING PigStorage (';');

STORE oldfanag_out_ndg INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/fanag_ndg/fanag_solo_old' USING PigStorage (';');

--STORE abbinati_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/fanag/fanag_abbinati' USING PigStorage (';');
