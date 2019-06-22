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
hadoop_fposi = LOAD '/app/drlgd/datarequest/dt/out/finale/$ufficio/fposi'
      USING PigStorage(';') 
  	  AS (
                 codicebanca            :chararray
				,ndgprincipale          :chararray
                ,datainiziodef          :chararray
                ,datafinedef            :chararray
				,ndg_gruppo             :chararray
				,datainiziopd           :chararray
				,datainizioinc          :chararray
				,datainizioristrutt     :chararray
				,datainiziosoff         :chararray
				,totaccordatodatdef     :chararray
				,totutilizzdatdef       :chararray
				,naturagiuridica_segm   :chararray
				,intestazione           :chararray
				,codicefiscale_segm     :chararray
				,partitaiva_segm        :chararray
				,sae_segm               :chararray
				,rae_segm               :chararray
				,ciae_ndg               :chararray
				,provincia_segm         :chararray
				,ateco                  :chararray
				,segmento               :chararray
				,databilseg             :chararray
				,strbilseg              :chararray
				,attivobilseg           :chararray
				,fatturbilseg           :chararray
         );

/*
carico la tabella MOVCONTA
*/

oldfposi_load = LOAD '/app/drlgd/store/oldfposi'
			USING PigStorage (';')
			AS (
                 datainizioDEF   :chararray
				,dataFINEDEF     :chararray
				,dataINIZIOPD    :chararray
				,datainizioinc   :chararray
				,dataSOFFERENZA  :chararray
				,codicebanca     :chararray
				,ndgprincipale   :chararray
				,flagincristrut  :chararray
				,cumulo          :chararray
				);
				
/*
oldfposi1 = FILTER oldfposi_load 
BY cumulo != 'cumulo' ;
*/
				
oldfposi_gen = FOREACH oldfposi_load
			GENERATE
			     ToString(ToDate( datainizioDEF,'yy-MM-dd'),'yyyyMMdd')   as DATAINIZIODEF
				,ToString(ToDate( dataFINEDEF,'yy-MM-dd'),'yyyyMMdd')     as DATAFINEDEF
				,ToString(ToDate( dataINIZIOPD,'yy-MM-dd'),'yyyyMMdd')    as DATAINIZIOPD
				,ToString(ToDate( datainizioinc,'yy-MM-dd'),'yyyyMMdd')   as DATAINIZIOINC
				,ToString(ToDate( dataSOFFERENZA,'yy-MM-dd'),'yyyyMMdd')  as DATASOFFERENZA
				,codicebanca     as CODICEBANCA     
				,ndgprincipale   as NDGPRINCIPALE   
				,flagincristrut  as FLAGINCRISTRUT  
				,cumulo          as CUMULO          
            ;

oldfposi = FILTER oldfposi_gen 
BY ToDate( DATAINIZIODEF,'yyyyMMdd') >= ToDate( '20070131','yyyyMMdd' ) 
and ToDate( DATAINIZIODEF,'yyyyMMdd') <= ToDate( '20071231','yyyyMMdd' );

/*unisco le due tabelle hadoop e old */

hadoop_fposi_oldfposi_join = JOIN hadoop_fposi BY (codicebanca, ndgprincipale, datainiziodef) FULL OUTER, oldfposi BY (CODICEBANCA, NDGPRINCIPALE, DATAINIZIODEF);

solo_hadoop_fposi_filter = FILTER hadoop_fposi_oldfposi_join BY oldfposi::CODICEBANCA IS NULL;	

solo_oldfposi_filter = FILTER hadoop_fposi_oldfposi_join BY hadoop_fposi::codicebanca IS NULL;

abbinati_filter = FILTER hadoop_fposi_oldfposi_join 
BY hadoop_fposi::codicebanca IS NOT NULL 
AND oldfposi::CODICEBANCA IS NOT NULL
AND hadoop_fposi::datafinedef == '99991231'
AND hadoop_fposi::datainiziopd       != oldfposi::DATAINIZIOPD
AND hadoop_fposi::datainizioinc      != oldfposi::DATAINIZIOINC
--AND hadoop_fposi::datainizioristrutt != oldfposi::DATAINIZIOREVOCA
AND hadoop_fposi::datainiziosoff     != oldfposi::DATASOFFERENZA
;


/*ottengo la mappatura finale*/

hadoop_fposi_out = FOREACH solo_hadoop_fposi_filter
			     GENERATE
             '$ufficio'           as ufficio
            ,hadoop_fposi::codicebanca           
			,hadoop_fposi::ndgprincipale         
			,hadoop_fposi::datainiziodef         
			,hadoop_fposi::datafinedef           
			,hadoop_fposi::ndg_gruppo            
			,hadoop_fposi::datainiziopd          
			,hadoop_fposi::datainizioinc         
			,hadoop_fposi::datainizioristrutt    
			,hadoop_fposi::datainiziosoff        
			,hadoop_fposi::totaccordatodatdef    
			,hadoop_fposi::totutilizzdatdef      
			,hadoop_fposi::naturagiuridica_segm  
			,hadoop_fposi::intestazione          
			,hadoop_fposi::codicefiscale_segm    
			,hadoop_fposi::partitaiva_segm       
			,hadoop_fposi::sae_segm              
			,hadoop_fposi::rae_segm              
			,hadoop_fposi::ciae_ndg              
			,hadoop_fposi::provincia_segm        
			,hadoop_fposi::ateco                 
			,hadoop_fposi::segmento              
			,hadoop_fposi::databilseg            
			,hadoop_fposi::strbilseg             
			,hadoop_fposi::attivobilseg          
			,hadoop_fposi::fatturbilseg
            ,oldfposi::DATAINIZIODEF
			,oldfposi::DATAFINEDEF
			,oldfposi::DATAINIZIOPD
			,oldfposi::DATAINIZIOINC
			,oldfposi::DATASOFFERENZA
			,oldfposi::CODICEBANCA 
			,oldfposi::NDGPRINCIPALE 
			,oldfposi::FLAGINCRISTRUT 
			,oldfposi::CUMULO 
            ;
					
oldfposi_out = FOREACH solo_oldfposi_filter
			     GENERATE
			 '$ufficio'           as ufficio
			,hadoop_fposi::codicebanca           
			,hadoop_fposi::ndgprincipale         
			,hadoop_fposi::datainiziodef         
			,hadoop_fposi::datafinedef           
			,hadoop_fposi::ndg_gruppo            
			,hadoop_fposi::datainiziopd          
			,hadoop_fposi::datainizioinc         
			,hadoop_fposi::datainizioristrutt    
			,hadoop_fposi::datainiziosoff        
			,hadoop_fposi::totaccordatodatdef    
			,hadoop_fposi::totutilizzdatdef      
			,hadoop_fposi::naturagiuridica_segm  
			,hadoop_fposi::intestazione          
			,hadoop_fposi::codicefiscale_segm    
			,hadoop_fposi::partitaiva_segm       
			,hadoop_fposi::sae_segm              
			,hadoop_fposi::rae_segm              
			,hadoop_fposi::ciae_ndg              
			,hadoop_fposi::provincia_segm        
			,hadoop_fposi::ateco                 
			,hadoop_fposi::segmento              
			,hadoop_fposi::databilseg            
			,hadoop_fposi::strbilseg             
			,hadoop_fposi::attivobilseg          
			,hadoop_fposi::fatturbilseg
			,oldfposi::DATAINIZIODEF
			,oldfposi::DATAFINEDEF
			,oldfposi::DATAINIZIOPD
			,oldfposi::DATAINIZIOINC
			,oldfposi::DATASOFFERENZA
			,oldfposi::CODICEBANCA 
			,oldfposi::NDGPRINCIPALE 
			,oldfposi::FLAGINCRISTRUT 
			,oldfposi::CUMULO 
					;
					
abbinati_out = FOREACH abbinati_filter
			     GENERATE
			 '$ufficio'           as ufficio
			,hadoop_fposi::codicebanca           
			,hadoop_fposi::ndgprincipale         
			,hadoop_fposi::datainiziodef         
			,hadoop_fposi::datafinedef           
			,hadoop_fposi::ndg_gruppo            
			,hadoop_fposi::datainiziopd          
			,hadoop_fposi::datainizioinc         
			,hadoop_fposi::datainizioristrutt    
			,hadoop_fposi::datainiziosoff        
			,hadoop_fposi::totaccordatodatdef    
			,hadoop_fposi::totutilizzdatdef      
			,hadoop_fposi::naturagiuridica_segm  
			,hadoop_fposi::intestazione          
			,hadoop_fposi::codicefiscale_segm    
			,hadoop_fposi::partitaiva_segm       
			,hadoop_fposi::sae_segm              
			,hadoop_fposi::rae_segm              
			,hadoop_fposi::ciae_ndg              
			,hadoop_fposi::provincia_segm        
			,hadoop_fposi::ateco                 
			,hadoop_fposi::segmento              
			,hadoop_fposi::databilseg            
			,hadoop_fposi::strbilseg             
			,hadoop_fposi::attivobilseg          
			,hadoop_fposi::fatturbilseg
			,oldfposi::DATAINIZIODEF
			,oldfposi::DATAFINEDEF
			,oldfposi::DATAINIZIOPD
			,oldfposi::DATAINIZIOINC
			,oldfposi::DATASOFFERENZA
			,oldfposi::CODICEBANCA 
			,oldfposi::NDGPRINCIPALE 
			,oldfposi::FLAGINCRISTRUT 
			,oldfposi::CUMULO 
					;

STORE hadoop_fposi_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/fposi/fposi_solo_hadoop' USING PigStorage (';');

STORE oldfposi_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/fposi/fposi_solo_old' USING PigStorage (';');

STORE abbinati_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/fposi/fposi_abbinati' USING PigStorage (';');
