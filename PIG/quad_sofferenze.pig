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
carico il file di cicli
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
   
     
/*
carico la tabella MOVCONTA
*/

oldsofferenze_load = LOAD '/app/drlgd/store/oldsofferenze'
			USING PigStorage (';')
			AS (
				 ISTITUTO              :chararray
                ,NDG                   :chararray
                ,NUMEROSOFFERENZA      :chararray
                ,DATAINIZIO            :chararray
                ,DATAFINE              :chararray
                ,STATOPRATICA          :chararray
                ,DATAULTMOVVALUTA      :chararray
                ,DATAULTMOVCONTAB      :chararray
                ,SALDOPOSIZIONE        :chararray
                ,SALDOPOSIZIONECONTAB  :chararray
                ,NUMMOVIMTIPO01        :chararray
                ,TOTMOVIMTIPO01        :chararray
                ,NUMMOVIMTIPO02        :chararray
                ,TOTMOVIMTIPO02        :chararray
                ,NUMMOVIMTIPO03        :chararray
                ,TOTMOVIMTIPO03        :chararray
                ,NUMMOVIMTIPO04        :chararray
                ,TOTMOVIMTIPO04        :chararray
                ,NUMMOVIMTIPO05        :chararray
                ,TOTMOVIMTIPO05        :chararray
                ,NUMMOVIMTIPO06        :chararray
                ,TOTMOVIMTIPO06        :chararray
                ,NUMMOVIMTIPO07        :chararray
                ,TOTMOVIMTIPO07        :chararray
                ,NUMMOVIMTIPO08        :chararray
                ,TOTMOVIMTIPO08        :chararray
                ,NUMMOVIMTIPO09        :chararray
                ,TOTMOVIMTIPO09        :chararray
                ,NUMMOVIMTIPO10        :chararray
                ,TOTMOVIMTIPO10        :chararray
                ,NUMMOVIMTIPO11        :chararray
                ,TOTMOVIMTIPO11        :chararray
                ,TIPOERRORE            :chararray
                ,CATRGRUP              :chararray
                ,FLAGPRESENZASOPRAVV   :chararray
                ,FLAGTIPOQUADRADURA    :chararray
                ,FLAGNORECUPERI        :chararray
                ,GRUPPOSOFFERENZIALE   :chararray
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

oldsofferenze_load_fcoll_join = JOIN oldsofferenze_load BY (ISTITUTO, NDG), fcoll BY (ISTITUTO_COLLEGATO, NDG_COLLEGATO);

oldsofferenze_load_fcoll_join_filt = FILTER oldsofferenze_load_fcoll_join 
 BY (int)SUBSTRING( oldsofferenze_load::DATAINIZIO,0,6 ) >= (int)SUBSTRING( fcoll::DATAINIZIODEF,0,6 )
AND (int)SUBSTRING( oldsofferenze_load::DATAINIZIO,0,6 ) <= (int)SUBSTRING( fcoll::DATAFINEDEF,0,6 )
;

oldsofferenze = FOREACH oldsofferenze_load_fcoll_join_filt
			     GENERATE
				 oldsofferenze_load::ISTITUTO              as ISTITUTO            
				,oldsofferenze_load::NDG                   as NDG                 
				,oldsofferenze_load::NUMEROSOFFERENZA      as NUMEROSOFFERENZA    
				,oldsofferenze_load::DATAINIZIO            as DATAINIZIO          
				,oldsofferenze_load::DATAFINE              as DATAFINE            
				,oldsofferenze_load::STATOPRATICA          as STATOPRATICA        
				,oldsofferenze_load::DATAULTMOVVALUTA      as DATAULTMOVVALUTA    
				,oldsofferenze_load::DATAULTMOVCONTAB      as DATAULTMOVCONTAB    
				,oldsofferenze_load::SALDOPOSIZIONE        as SALDOPOSIZIONE      
				,oldsofferenze_load::SALDOPOSIZIONECONTAB  as SALDOPOSIZIONECONTAB
				,oldsofferenze_load::NUMMOVIMTIPO01        as NUMMOVIMTIPO01      
				,oldsofferenze_load::TOTMOVIMTIPO01        as TOTMOVIMTIPO01      
				,oldsofferenze_load::NUMMOVIMTIPO02        as NUMMOVIMTIPO02      
				,oldsofferenze_load::TOTMOVIMTIPO02        as TOTMOVIMTIPO02      
				,oldsofferenze_load::NUMMOVIMTIPO03        as NUMMOVIMTIPO03      
				,oldsofferenze_load::TOTMOVIMTIPO03        as TOTMOVIMTIPO03      
				,oldsofferenze_load::NUMMOVIMTIPO04        as NUMMOVIMTIPO04      
				,oldsofferenze_load::TOTMOVIMTIPO04        as TOTMOVIMTIPO04      
				,oldsofferenze_load::NUMMOVIMTIPO05        as NUMMOVIMTIPO05      
				,oldsofferenze_load::TOTMOVIMTIPO05        as TOTMOVIMTIPO05      
				,oldsofferenze_load::NUMMOVIMTIPO06        as NUMMOVIMTIPO06      
				,oldsofferenze_load::TOTMOVIMTIPO06        as TOTMOVIMTIPO06      
				,oldsofferenze_load::NUMMOVIMTIPO07        as NUMMOVIMTIPO07      
				,oldsofferenze_load::TOTMOVIMTIPO07        as TOTMOVIMTIPO07      
				,oldsofferenze_load::NUMMOVIMTIPO08        as NUMMOVIMTIPO08      
				,oldsofferenze_load::TOTMOVIMTIPO08        as TOTMOVIMTIPO08      
				,oldsofferenze_load::NUMMOVIMTIPO09        as NUMMOVIMTIPO09      
				,oldsofferenze_load::TOTMOVIMTIPO09        as TOTMOVIMTIPO09      
				,oldsofferenze_load::NUMMOVIMTIPO10        as NUMMOVIMTIPO10      
				,oldsofferenze_load::TOTMOVIMTIPO10        as TOTMOVIMTIPO10      
				,oldsofferenze_load::NUMMOVIMTIPO11        as NUMMOVIMTIPO11      
				,oldsofferenze_load::TOTMOVIMTIPO11        as TOTMOVIMTIPO11      
				,oldsofferenze_load::TIPOERRORE            as TIPOERRORE          
				,oldsofferenze_load::CATRGRUP              as CATRGRUP            
				,oldsofferenze_load::FLAGPRESENZASOPRAVV   as FLAGPRESENZASOPRAVV 
				,oldsofferenze_load::FLAGTIPOQUADRADURA    as FLAGTIPOQUADRADURA  
				,oldsofferenze_load::FLAGNORECUPERI        as FLAGNORECUPERI      
				,oldsofferenze_load::GRUPPOSOFFERENZIALE   as GRUPPOSOFFERENZIALE   
                ,fcoll::CODICEBANCA              as CODICEBANCA_PRINC
                ,fcoll::NDGPRINCIPALE            as NDGPRINCIPALE
                ,fcoll::DATAINIZIODEF            as DATAINIZIODEF
				;

/*unisco le due tabelle TLBCIDEF_FILTER e MOVCONTA*/

hadoop_sofferenze_oldsofferenze_join = JOIN hadoop_sofferenze BY (istituto, numerosofferenza, datainizio) FULL OUTER, oldsofferenze BY (ISTITUTO, NUMEROSOFFERENZA, DATAINIZIO);

solo_hadoop_sofferenze_filter = FILTER hadoop_sofferenze_oldsofferenze_join BY oldsofferenze::ISTITUTO IS NULL;	

solo_oldsofferenze_filter = FILTER hadoop_sofferenze_oldsofferenze_join BY hadoop_sofferenze::istituto IS NULL;

abbinati_filter = FILTER hadoop_sofferenze_oldsofferenze_join 
BY hadoop_sofferenze::istituto IS NOT NULL 
AND oldsofferenze::ISTITUTO IS NOT NULL
AND hadoop_sofferenze::codicebanca IS NOT NULL 
AND oldsofferenze::CODICEBANCA_PRINC IS NOT NULL
AND hadoop_sofferenze::codicebanca   != oldsofferenze::CODICEBANCA_PRINC
AND hadoop_sofferenze::ndgprincipale != oldsofferenze::NDGPRINCIPALE
AND hadoop_sofferenze::datainiziodef != oldsofferenze::DATAINIZIODEF
;

/*ottengo la mappatura finale*/

hadoop_sofferenze_out = FOREACH solo_hadoop_sofferenze_filter
			     GENERATE
				     '$ufficio'           as ufficio
				    ,hadoop_sofferenze::istituto               
					,hadoop_sofferenze::ndg                   
					,hadoop_sofferenze::numerosofferenza      
					,hadoop_sofferenze::datainizio            
					,hadoop_sofferenze::datafine              
					,hadoop_sofferenze::statopratica          
					,hadoop_sofferenze::saldoposizione        
					,hadoop_sofferenze::saldoposizionecontab  
					,hadoop_sofferenze::nummovimtipo01        
					,hadoop_sofferenze::totmovimtipo01        
					,hadoop_sofferenze::nummovimtipo02        
					,hadoop_sofferenze::totmovimtipo02        
					,hadoop_sofferenze::nummovimtipo03        
					,hadoop_sofferenze::totmovimtipo03        
					,hadoop_sofferenze::nummovimtipo04        
					,hadoop_sofferenze::totmovimtipo04        
					,hadoop_sofferenze::nummovimtipo05        
					,hadoop_sofferenze::totmovimtipo05        
					,hadoop_sofferenze::nummovimtipo06        
					,hadoop_sofferenze::totmovimtipo06        
					,hadoop_sofferenze::nummovimtipo07        
					,hadoop_sofferenze::totmovimtipo07        
					,hadoop_sofferenze::nummovimtipo08        
					,hadoop_sofferenze::totmovimtipo08        
					,hadoop_sofferenze::nummovimtipo09        
					,hadoop_sofferenze::totmovimtipo09        
					,hadoop_sofferenze::nummovimtipo10        
					,hadoop_sofferenze::totmovimtipo10        
					,hadoop_sofferenze::nummovimtipo11        
					,hadoop_sofferenze::totmovimtipo11        
					,hadoop_sofferenze::tipoerrore            
					,hadoop_sofferenze::flagtipoquadradura    
					,hadoop_sofferenze::codicebanca           
					,hadoop_sofferenze::ndgprincipale         
					,hadoop_sofferenze::datainiziodef
					,oldsofferenze::ISTITUTO               
					,oldsofferenze::NDG                   
					,oldsofferenze::NUMEROSOFFERENZA      
					,oldsofferenze::DATAINIZIO            
					,oldsofferenze::DATAFINE              
					,oldsofferenze::STATOPRATICA          
					,oldsofferenze::DATAULTMOVVALUTA      
					,oldsofferenze::DATAULTMOVCONTAB      
					,oldsofferenze::SALDOPOSIZIONE        
					,oldsofferenze::SALDOPOSIZIONECONTAB  
					,oldsofferenze::NUMMOVIMTIPO01        
					,oldsofferenze::TOTMOVIMTIPO01        
					,oldsofferenze::NUMMOVIMTIPO02        
					,oldsofferenze::TOTMOVIMTIPO02        
					,oldsofferenze::NUMMOVIMTIPO03        
					,oldsofferenze::TOTMOVIMTIPO03        
					,oldsofferenze::NUMMOVIMTIPO04        
					,oldsofferenze::TOTMOVIMTIPO04        
					,oldsofferenze::NUMMOVIMTIPO05        
					,oldsofferenze::TOTMOVIMTIPO05        
					,oldsofferenze::NUMMOVIMTIPO06        
					,oldsofferenze::TOTMOVIMTIPO06        
					,oldsofferenze::NUMMOVIMTIPO07        
					,oldsofferenze::TOTMOVIMTIPO07        
					,oldsofferenze::NUMMOVIMTIPO08        
					,oldsofferenze::TOTMOVIMTIPO08        
					,oldsofferenze::NUMMOVIMTIPO09        
					,oldsofferenze::TOTMOVIMTIPO09        
					,oldsofferenze::NUMMOVIMTIPO10        
					,oldsofferenze::TOTMOVIMTIPO10        
					,oldsofferenze::NUMMOVIMTIPO11        
					,oldsofferenze::TOTMOVIMTIPO11        
					,oldsofferenze::TIPOERRORE            
					,oldsofferenze::CATRGRUP              
					,oldsofferenze::FLAGPRESENZASOPRAVV   
					,oldsofferenze::FLAGTIPOQUADRADURA    
					,oldsofferenze::FLAGNORECUPERI        
					,oldsofferenze::GRUPPOSOFFERENZIALE
                    ,oldsofferenze::CODICEBANCA_PRINC
                    ,oldsofferenze::NDGPRINCIPALE
                    ,oldsofferenze::DATAINIZIODEF
					;
					
oldsofferenze_out = FOREACH solo_oldsofferenze_filter
			     GENERATE
				  	 '$ufficio'           as ufficio
				  	,hadoop_sofferenze::istituto               
					,hadoop_sofferenze::ndg                   
					,hadoop_sofferenze::numerosofferenza      
					,hadoop_sofferenze::datainizio            
					,hadoop_sofferenze::datafine              
					,hadoop_sofferenze::statopratica          
					,hadoop_sofferenze::saldoposizione        
					,hadoop_sofferenze::saldoposizionecontab  
					,hadoop_sofferenze::nummovimtipo01        
					,hadoop_sofferenze::totmovimtipo01        
					,hadoop_sofferenze::nummovimtipo02        
					,hadoop_sofferenze::totmovimtipo02        
					,hadoop_sofferenze::nummovimtipo03        
					,hadoop_sofferenze::totmovimtipo03        
					,hadoop_sofferenze::nummovimtipo04        
					,hadoop_sofferenze::totmovimtipo04        
					,hadoop_sofferenze::nummovimtipo05        
					,hadoop_sofferenze::totmovimtipo05        
					,hadoop_sofferenze::nummovimtipo06        
					,hadoop_sofferenze::totmovimtipo06        
					,hadoop_sofferenze::nummovimtipo07        
					,hadoop_sofferenze::totmovimtipo07        
					,hadoop_sofferenze::nummovimtipo08        
					,hadoop_sofferenze::totmovimtipo08        
					,hadoop_sofferenze::nummovimtipo09        
					,hadoop_sofferenze::totmovimtipo09        
					,hadoop_sofferenze::nummovimtipo10        
					,hadoop_sofferenze::totmovimtipo10        
					,hadoop_sofferenze::nummovimtipo11        
					,hadoop_sofferenze::totmovimtipo11        
					,hadoop_sofferenze::tipoerrore            
					,hadoop_sofferenze::flagtipoquadradura    
					,hadoop_sofferenze::codicebanca           
					,hadoop_sofferenze::ndgprincipale         
					,hadoop_sofferenze::datainiziodef
					,oldsofferenze::ISTITUTO               
					,oldsofferenze::NDG                   
					,oldsofferenze::NUMEROSOFFERENZA      
					,oldsofferenze::DATAINIZIO            
					,oldsofferenze::DATAFINE              
					,oldsofferenze::STATOPRATICA          
					,oldsofferenze::DATAULTMOVVALUTA      
					,oldsofferenze::DATAULTMOVCONTAB      
					,oldsofferenze::SALDOPOSIZIONE        
					,oldsofferenze::SALDOPOSIZIONECONTAB  
					,oldsofferenze::NUMMOVIMTIPO01        
					,oldsofferenze::TOTMOVIMTIPO01        
					,oldsofferenze::NUMMOVIMTIPO02        
					,oldsofferenze::TOTMOVIMTIPO02        
					,oldsofferenze::NUMMOVIMTIPO03        
					,oldsofferenze::TOTMOVIMTIPO03        
					,oldsofferenze::NUMMOVIMTIPO04        
					,oldsofferenze::TOTMOVIMTIPO04        
					,oldsofferenze::NUMMOVIMTIPO05        
					,oldsofferenze::TOTMOVIMTIPO05        
					,oldsofferenze::NUMMOVIMTIPO06        
					,oldsofferenze::TOTMOVIMTIPO06        
					,oldsofferenze::NUMMOVIMTIPO07        
					,oldsofferenze::TOTMOVIMTIPO07        
					,oldsofferenze::NUMMOVIMTIPO08        
					,oldsofferenze::TOTMOVIMTIPO08        
					,oldsofferenze::NUMMOVIMTIPO09        
					,oldsofferenze::TOTMOVIMTIPO09        
					,oldsofferenze::NUMMOVIMTIPO10        
					,oldsofferenze::TOTMOVIMTIPO10        
					,oldsofferenze::NUMMOVIMTIPO11        
					,oldsofferenze::TOTMOVIMTIPO11        
					,oldsofferenze::TIPOERRORE            
					,oldsofferenze::CATRGRUP              
					,oldsofferenze::FLAGPRESENZASOPRAVV   
					,oldsofferenze::FLAGTIPOQUADRADURA    
					,oldsofferenze::FLAGNORECUPERI        
					,oldsofferenze::GRUPPOSOFFERENZIALE
                    ,oldsofferenze::CODICEBANCA_PRINC
                    ,oldsofferenze::NDGPRINCIPALE
                    ,oldsofferenze::DATAINIZIODEF
					;
					
abbinati_out = FOREACH abbinati_filter
			     GENERATE
				     '$ufficio'           as ufficio
				    ,hadoop_sofferenze::istituto               
					,hadoop_sofferenze::ndg                   
					,hadoop_sofferenze::numerosofferenza      
					,hadoop_sofferenze::datainizio            
					,hadoop_sofferenze::datafine              
					,hadoop_sofferenze::statopratica          
					,hadoop_sofferenze::saldoposizione        
					,hadoop_sofferenze::saldoposizionecontab  
					,hadoop_sofferenze::nummovimtipo01        
					,hadoop_sofferenze::totmovimtipo01        
					,hadoop_sofferenze::nummovimtipo02        
					,hadoop_sofferenze::totmovimtipo02        
					,hadoop_sofferenze::nummovimtipo03        
					,hadoop_sofferenze::totmovimtipo03        
					,hadoop_sofferenze::nummovimtipo04        
					,hadoop_sofferenze::totmovimtipo04        
					,hadoop_sofferenze::nummovimtipo05        
					,hadoop_sofferenze::totmovimtipo05        
					,hadoop_sofferenze::nummovimtipo06        
					,hadoop_sofferenze::totmovimtipo06        
					,hadoop_sofferenze::nummovimtipo07        
					,hadoop_sofferenze::totmovimtipo07        
					,hadoop_sofferenze::nummovimtipo08        
					,hadoop_sofferenze::totmovimtipo08        
					,hadoop_sofferenze::nummovimtipo09        
					,hadoop_sofferenze::totmovimtipo09        
					,hadoop_sofferenze::nummovimtipo10        
					,hadoop_sofferenze::totmovimtipo10        
					,hadoop_sofferenze::nummovimtipo11        
					,hadoop_sofferenze::totmovimtipo11        
					,hadoop_sofferenze::tipoerrore            
					,hadoop_sofferenze::flagtipoquadradura    
					,hadoop_sofferenze::codicebanca           
					,hadoop_sofferenze::ndgprincipale         
					,hadoop_sofferenze::datainiziodef
					,oldsofferenze::ISTITUTO               
					,oldsofferenze::NDG                   
					,oldsofferenze::NUMEROSOFFERENZA      
					,oldsofferenze::DATAINIZIO            
					,oldsofferenze::DATAFINE              
					,oldsofferenze::STATOPRATICA          
					,oldsofferenze::DATAULTMOVVALUTA      
					,oldsofferenze::DATAULTMOVCONTAB      
					,oldsofferenze::SALDOPOSIZIONE        
					,oldsofferenze::SALDOPOSIZIONECONTAB  
					,oldsofferenze::NUMMOVIMTIPO01        
					,oldsofferenze::TOTMOVIMTIPO01        
					,oldsofferenze::NUMMOVIMTIPO02        
					,oldsofferenze::TOTMOVIMTIPO02        
					,oldsofferenze::NUMMOVIMTIPO03        
					,oldsofferenze::TOTMOVIMTIPO03        
					,oldsofferenze::NUMMOVIMTIPO04        
					,oldsofferenze::TOTMOVIMTIPO04        
					,oldsofferenze::NUMMOVIMTIPO05        
					,oldsofferenze::TOTMOVIMTIPO05        
					,oldsofferenze::NUMMOVIMTIPO06        
					,oldsofferenze::TOTMOVIMTIPO06        
					,oldsofferenze::NUMMOVIMTIPO07        
					,oldsofferenze::TOTMOVIMTIPO07        
					,oldsofferenze::NUMMOVIMTIPO08        
					,oldsofferenze::TOTMOVIMTIPO08        
					,oldsofferenze::NUMMOVIMTIPO09        
					,oldsofferenze::TOTMOVIMTIPO09        
					,oldsofferenze::NUMMOVIMTIPO10        
					,oldsofferenze::TOTMOVIMTIPO10        
					,oldsofferenze::NUMMOVIMTIPO11        
					,oldsofferenze::TOTMOVIMTIPO11        
					,oldsofferenze::TIPOERRORE            
					,oldsofferenze::CATRGRUP              
					,oldsofferenze::FLAGPRESENZASOPRAVV   
					,oldsofferenze::FLAGTIPOQUADRADURA    
					,oldsofferenze::FLAGNORECUPERI        
					,oldsofferenze::GRUPPOSOFFERENZIALE
					,oldsofferenze::CODICEBANCA_PRINC
                    ,oldsofferenze::NDGPRINCIPALE
                    ,oldsofferenze::DATAINIZIODEF
					;
					 
STORE hadoop_sofferenze_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/sofferenze/sofferenze_solo_hadoop' USING PigStorage (';');

STORE oldsofferenze_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/sofferenze/sofferenze_solo_old' USING PigStorage (';');

STORE abbinati_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/sofferenze/sofferenze_abbinati' USING PigStorage (';');
