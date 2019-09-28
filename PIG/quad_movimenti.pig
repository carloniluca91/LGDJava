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
carico il file di cicli
*/
hadoop_movimenti = LOAD '/app/drlgd/datarequest/dt/out/finale/$ufficio/movimenti'
      USING PigStorage(';') 
  	  AS (
           istituto           :chararray
          ,ndg                :chararray
          ,datariferimento    :chararray
          ,sportello          :chararray
          ,conto              :chararray
          ,numerosofferenza   :chararray
          ,catrappsoffer      :chararray
          ,filrappsoffer      :chararray
          ,numrappsoffer      :chararray
          ,idmovimento        :chararray
          ,categoria          :chararray
          ,casuale            :chararray
          ,dtcontab           :chararray
          ,dtvaluta           :chararray
          ,importo            :chararray
          ,flagextracontab    :chararray
          ,flagstorno         :chararray
          ,codicebanca_princ  :chararray
          ,ndgprincipale      :chararray
          ,datainiziodef      :chararray
         );
   
     
/*
carico la tabella MOVCONTA
*/

oldmovimenti_load = LOAD '/app/drlgd/store/oldmovimenti'
			USING PigStorage (';')
			AS (
				 ISTITUTO           :chararray
                ,NDG                :chararray
                ,DATARIFERIMENTO    :chararray
                ,DATASOFFERENZA     :chararray
                ,SPORTELLO          :chararray
                ,CONTO              :chararray
                ,NUMEROSOFFERENZA   :chararray
                ,CATRAPPSOFFER      :chararray
                ,FILRAPPSOFFER      :chararray
                ,NUMRAPPSOFFER      :chararray
                ,IDMOVIMENTO        :chararray
                ,CATEGORIA          :chararray
                ,CATEGORIG          :chararray
                ,CAUSALE            :chararray
                ,CAUSORIG           :chararray
                ,DTCONTAB           :chararray
                ,DTVALUTA           :chararray
                ,IMPORTO            :chararray
                ,TIPOMOVIMENTO      :chararray
                ,FLAGEXTRACONTAB    :chararray
                ,FLAGSTORNO         :chararray
                ,FLAGMOVIM          :chararray
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

oldmovimenti_load_fcoll_join = JOIN oldmovimenti_load BY (ISTITUTO, NDG, DATARIFERIMENTO), fcoll BY (ISTITUTO_COLLEGATO, NDG_COLLEGATO, DATA_DEFAULT);

oldmovimenti = FOREACH oldmovimenti_load_fcoll_join
			     GENERATE
					 oldmovimenti_load::ISTITUTO           as ISTITUTO        
					,oldmovimenti_load::NDG                as NDG             
					,oldmovimenti_load::DATARIFERIMENTO    as DATARIFERIMENTO 
					,oldmovimenti_load::DATASOFFERENZA     as DATASOFFERENZA  
					,oldmovimenti_load::SPORTELLO          as SPORTELLO       
					,oldmovimenti_load::CONTO              as CONTO           
					,oldmovimenti_load::NUMEROSOFFERENZA   as NUMEROSOFFERENZA
					,oldmovimenti_load::CATRAPPSOFFER      as CATRAPPSOFFER   
					,oldmovimenti_load::FILRAPPSOFFER      as FILRAPPSOFFER   
					,oldmovimenti_load::NUMRAPPSOFFER      as NUMRAPPSOFFER   
					,oldmovimenti_load::IDMOVIMENTO        as IDMOVIMENTO     
					,oldmovimenti_load::CATEGORIA          as CATEGORIA       
					,oldmovimenti_load::CATEGORIG          as CATEGORIG       
					,oldmovimenti_load::CAUSALE            as CAUSALE         
					,oldmovimenti_load::CAUSORIG           as CAUSORIG        
					,oldmovimenti_load::DTCONTAB           as DTCONTAB        
					,oldmovimenti_load::DTVALUTA           as DTVALUTA        
					,oldmovimenti_load::IMPORTO            as IMPORTO          
					,oldmovimenti_load::TIPOMOVIMENTO      as TIPOMOVIMENTO    
					,oldmovimenti_load::FLAGEXTRACONTAB    as FLAGEXTRACONTAB  
					,oldmovimenti_load::FLAGSTORNO         as FLAGSTORNO      
					,oldmovimenti_load::FLAGMOVIM          as FLAGMOVIM  
                    ,fcoll::CODICEBANCA              as CODICEBANCA_PRINC
                    ,fcoll::NDGPRINCIPALE            as NDGPRINCIPALE
                    ,fcoll::DATAINIZIODEF            as DATAINIZIODEF
					;

/*unisco le due tabelle TLBCIDEF_FILTER e MOVCONTA*/

hadoop_movimenti_oldmovimenti_join = JOIN hadoop_movimenti BY (istituto, ndg, datariferimento, idmovimento) FULL OUTER 
                                         ,oldmovimenti BY (ISTITUTO, NDG, DATARIFERIMENTO, IDMOVIMENTO);

solo_hadoop_movimenti_filter = FILTER hadoop_movimenti_oldmovimenti_join BY oldmovimenti::ISTITUTO IS NULL;	

solo_oldmovimenti_filter = FILTER hadoop_movimenti_oldmovimenti_join BY hadoop_movimenti::istituto IS NULL;

abbinati_filter = FILTER hadoop_movimenti_oldmovimenti_join 
BY hadoop_movimenti::istituto IS NOT NULL 
AND oldmovimenti::ISTITUTO IS NOT NULL
AND hadoop_movimenti::codicebanca_princ != oldmovimenti::CODICEBANCA_PRINC
AND hadoop_movimenti::ndgprincipale     != oldmovimenti::NDGPRINCIPALE
AND hadoop_movimenti::datainiziodef     != oldmovimenti::DATAINIZIODEF
;

/*ottengo la mappatura finale*/

hadoop_movimenti_out = FOREACH solo_hadoop_movimenti_filter
			     GENERATE
			     '$ufficio'           as ufficio
			    ,hadoop_movimenti::istituto           
				,hadoop_movimenti::ndg                
				,hadoop_movimenti::datariferimento    
				,hadoop_movimenti::sportello          
				,hadoop_movimenti::conto              
				,hadoop_movimenti::numerosofferenza   
				,hadoop_movimenti::catrappsoffer      
				,hadoop_movimenti::filrappsoffer      
				,hadoop_movimenti::numrappsoffer      
				,hadoop_movimenti::idmovimento        
				,hadoop_movimenti::categoria          
				,hadoop_movimenti::casuale            
				,hadoop_movimenti::dtcontab           
				,hadoop_movimenti::dtvaluta           
				,hadoop_movimenti::importo            
				,hadoop_movimenti::flagextracontab    
				,hadoop_movimenti::flagstorno         
				,hadoop_movimenti::codicebanca_princ        
				,hadoop_movimenti::ndgprincipale      
				,hadoop_movimenti::datainiziodef
				,oldmovimenti::ISTITUTO           
				,oldmovimenti::NDG                
				,oldmovimenti::DATARIFERIMENTO    
				,oldmovimenti::DATASOFFERENZA     
				,oldmovimenti::SPORTELLO          
				,oldmovimenti::CONTO              
				,oldmovimenti::NUMEROSOFFERENZA   
				,oldmovimenti::CATRAPPSOFFER      
				,oldmovimenti::FILRAPPSOFFER      
				,oldmovimenti::NUMRAPPSOFFER      
				,oldmovimenti::IDMOVIMENTO        
				,oldmovimenti::CATEGORIA          
				,oldmovimenti::CATEGORIG          
				,oldmovimenti::CAUSALE            
				,oldmovimenti::CAUSORIG           
				,oldmovimenti::DTCONTAB           
				,oldmovimenti::DTVALUTA           
				,oldmovimenti::IMPORTO            
				,oldmovimenti::TIPOMOVIMENTO      
				,oldmovimenti::FLAGEXTRACONTAB    
				,oldmovimenti::FLAGSTORNO         
				,oldmovimenti::FLAGMOVIM
                ,oldmovimenti::CODICEBANCA_PRINC
                ,oldmovimenti::NDGPRINCIPALE
                ,oldmovimenti::DATAINIZIODEF
                ;
					
oldmovimenti_out = FOREACH solo_oldmovimenti_filter
			     GENERATE
				 '$ufficio'           as ufficio
				,hadoop_movimenti::istituto           
				,hadoop_movimenti::ndg                
				,hadoop_movimenti::datariferimento    
				,hadoop_movimenti::sportello          
				,hadoop_movimenti::conto              
				,hadoop_movimenti::numerosofferenza   
				,hadoop_movimenti::catrappsoffer      
				,hadoop_movimenti::filrappsoffer      
				,hadoop_movimenti::numrappsoffer      
				,hadoop_movimenti::idmovimento        
				,hadoop_movimenti::categoria          
				,hadoop_movimenti::casuale            
				,hadoop_movimenti::dtcontab           
				,hadoop_movimenti::dtvaluta           
				,hadoop_movimenti::importo            
				,hadoop_movimenti::flagextracontab    
				,hadoop_movimenti::flagstorno         
				,hadoop_movimenti::codicebanca_princ        
				,hadoop_movimenti::ndgprincipale      
				,hadoop_movimenti::datainiziodef
				,oldmovimenti::ISTITUTO           
				,oldmovimenti::NDG                
				,oldmovimenti::DATARIFERIMENTO    
				,oldmovimenti::DATASOFFERENZA     
				,oldmovimenti::SPORTELLO          
				,oldmovimenti::CONTO              
				,oldmovimenti::NUMEROSOFFERENZA   
				,oldmovimenti::CATRAPPSOFFER      
				,oldmovimenti::FILRAPPSOFFER      
				,oldmovimenti::NUMRAPPSOFFER      
				,oldmovimenti::IDMOVIMENTO        
				,oldmovimenti::CATEGORIA          
				,oldmovimenti::CATEGORIG          
				,oldmovimenti::CAUSALE            
				,oldmovimenti::CAUSORIG           
				,oldmovimenti::DTCONTAB           
				,oldmovimenti::DTVALUTA           
				,oldmovimenti::IMPORTO            
				,oldmovimenti::TIPOMOVIMENTO      
				,oldmovimenti::FLAGEXTRACONTAB    
				,oldmovimenti::FLAGSTORNO         
				,oldmovimenti::FLAGMOVIM
                ,oldmovimenti::CODICEBANCA_PRINC
                ,oldmovimenti::NDGPRINCIPALE
                ,oldmovimenti::DATAINIZIODEF
                ;

abbinati_out = FOREACH abbinati_filter
			     GENERATE
				 '$ufficio'           as ufficio
				,hadoop_movimenti::istituto           
				,hadoop_movimenti::ndg                
				,hadoop_movimenti::datariferimento    
				,hadoop_movimenti::sportello          
				,hadoop_movimenti::conto              
				,hadoop_movimenti::numerosofferenza   
				,hadoop_movimenti::catrappsoffer      
				,hadoop_movimenti::filrappsoffer      
				,hadoop_movimenti::numrappsoffer      
				,hadoop_movimenti::idmovimento        
				,hadoop_movimenti::categoria          
				,hadoop_movimenti::casuale            
				,hadoop_movimenti::dtcontab           
				,hadoop_movimenti::dtvaluta           
				,hadoop_movimenti::importo            
				,hadoop_movimenti::flagextracontab    
				,hadoop_movimenti::flagstorno         
				,hadoop_movimenti::codicebanca_princ        
				,hadoop_movimenti::ndgprincipale      
				,hadoop_movimenti::datainiziodef
				,oldmovimenti::ISTITUTO           
				,oldmovimenti::NDG                
				,oldmovimenti::DATARIFERIMENTO    
				,oldmovimenti::DATASOFFERENZA     
				,oldmovimenti::SPORTELLO          
				,oldmovimenti::CONTO              
				,oldmovimenti::NUMEROSOFFERENZA   
				,oldmovimenti::CATRAPPSOFFER      
				,oldmovimenti::FILRAPPSOFFER      
				,oldmovimenti::NUMRAPPSOFFER      
				,oldmovimenti::IDMOVIMENTO        
				,oldmovimenti::CATEGORIA          
				,oldmovimenti::CATEGORIG          
				,oldmovimenti::CAUSALE            
				,oldmovimenti::CAUSORIG           
				,oldmovimenti::DTCONTAB           
				,oldmovimenti::DTVALUTA           
				,oldmovimenti::IMPORTO            
				,oldmovimenti::TIPOMOVIMENTO      
				,oldmovimenti::FLAGEXTRACONTAB    
				,oldmovimenti::FLAGSTORNO         
				,oldmovimenti::FLAGMOVIM
                ,oldmovimenti::CODICEBANCA_PRINC
                ,oldmovimenti::NDGPRINCIPALE
                ,oldmovimenti::DATAINIZIODEF
                ;
                
/*
carico il file sofferenze hadoop
*/
hadoop_sofferenze = LOAD '/app/drlgd/datarequest/dt/out/finale/$ufficio/sofferenze'
      USING PigStorage(';') 
  	  AS (
           istituto               :chararray
          ,ndg                    :chararray
          ,numerosofferenza       :chararray
          ,datainizio             :chararray
          ,datafine               :chararray
          ,statopratica           :chararray
          ,saldoposizione         :chararray
          ,saldoposizionecontab   :chararray
          ,nummovimtipo01         :chararray
          ,totmovimtipo01         :chararray
          ,nummovimtipo02         :chararray
          ,totmovimtipo02         :chararray
          ,nummovimtipo03         :chararray
          ,totmovimtipo03         :chararray
          ,nummovimtipo04         :chararray
          ,totmovimtipo04         :chararray
          ,nummovimtipo05         :chararray
          ,totmovimtipo05         :chararray
          ,nummovimtipo06         :chararray
          ,totmovimtipo06         :chararray
          ,nummovimtipo07         :chararray
          ,totmovimtipo07         :chararray
          ,nummovimtipo08         :chararray
          ,totmovimtipo08         :chararray
          ,nummovimtipo09         :chararray
          ,totmovimtipo09         :chararray
          ,nummovimtipo10         :chararray
          ,totmovimtipo10         :chararray
          ,nummovimtipo11         :chararray
          ,totmovimtipo11         :chararray
          ,tipoerrore             :chararray
          ,flagtipoquadradura     :chararray
          ,codicebanca            :chararray
          ,ndgprincipale          :chararray
          ,datainiziodef          :chararray
         );
         
hadoop_movimenti_sofferenze_join = JOIN hadoop_movimenti BY (istituto, ndg, numerosofferenza) LEFT
                                       ,hadoop_sofferenze BY (istituto, ndg, numerosofferenza);
                                       
solo_hadoop_movimenti_soff_filter = FILTER hadoop_movimenti_sofferenze_join BY hadoop_sofferenze::istituto IS NULL;

hadoop_movimenti_soff_out = FOREACH solo_hadoop_movimenti_soff_filter
			     GENERATE
			     '$ufficio'           as ufficio
			    ,hadoop_movimenti::istituto           
				,hadoop_movimenti::ndg                
				,hadoop_movimenti::datariferimento    
				,hadoop_movimenti::sportello          
				,hadoop_movimenti::conto              
				,hadoop_movimenti::numerosofferenza   
				,hadoop_movimenti::catrappsoffer      
				,hadoop_movimenti::filrappsoffer      
				,hadoop_movimenti::numrappsoffer      
				,hadoop_movimenti::idmovimento        
				,hadoop_movimenti::categoria          
				,hadoop_movimenti::casuale            
				,hadoop_movimenti::dtcontab           
				,hadoop_movimenti::dtvaluta           
				,hadoop_movimenti::importo            
				,hadoop_movimenti::flagextracontab    
				,hadoop_movimenti::flagstorno         
				,hadoop_movimenti::codicebanca_princ        
				,hadoop_movimenti::ndgprincipale      
				,hadoop_movimenti::datainiziodef
				,'NO_PRATICA' as ISTITUTO
				,NULL as NDG
				,NULL as DATARIFERIMENTO
				,NULL as DATASOFFERENZA
				,NULL as SPORTELLO
				,NULL as CONTO
				,NULL as NUMEROSOFFERENZA
				,NULL as CATRAPPSOFFER
				,NULL as FILRAPPSOFFER
				,NULL as NUMRAPPSOFFER
				,NULL as IDMOVIMENTO
				,NULL as CATEGORIA
				,NULL as CATEGORIG          
				,NULL as CAUSALE
				,NULL as CAUSORIG
				,NULL as DTCONTAB
				,NULL as DTVALUTA
				,NULL as IMPORTO
				,NULL as TIPOMOVIMENTO
				,NULL as FLAGEXTRACONTAB
				,NULL as FLAGSTORNO
				,NULL as FLAGMOVIM
                ,NULL as CODICEBANCA_PRINC
                ,NULL as NDGPRINCIPALE
                ,NULL as DATAINIZIODEF
                ;

STORE hadoop_movimenti_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/movimenti/movimenti_solo_hadoop' USING PigStorage (';');

STORE oldmovimenti_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/movimenti/movimenti_solo_old' USING PigStorage (';');

STORE abbinati_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/movimenti/movimenti_abbinati' USING PigStorage (';');

STORE hadoop_movimenti_soff_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/movimenti/movimenti_sofferenze' USING PigStorage (';');
