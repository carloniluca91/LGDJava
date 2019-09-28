/*

$LastChangedDate: 2016-10-28 14:49:31 +0200 (ven, 28 ott 2016) $
$Rev: 585 $		
$Author: arzelenko $	

Sono coinvolti i seguenti parametri:
		-cicli_ndg_path
		-tlbpaspe_path
		-data_osservazione
		-fpasperd_outdir
		
*/

/*
carico il file di cicli
*/

tlbcidef_load = LOAD '$cicli_ndg_path'
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
         );

/*aggiunta di grace period di 2 mesi alla dt_fine del ciclo come da indicazione di Menegazzi*/
tlbcidef =  FOREACH tlbcidef_load                          
				GENERATE 			                                          
						  codicebanca             AS  codicebanca  
						 ,ndgprincipale	          AS  ndgprincipale	          
						 ,datainiziodef	          AS  datainiziodef	      
						 ,(int)ToString(AddDuration( ToDate( (chararray)datafinedef,'yyyyMMdd' ),'P2M' ),'yyyyMMdd' )	AS  datafinedef	      
						 ,codicebanca_collegato	  AS  codicebanca_collegato	
						 ,ndg_collegato		      AS  ndg_collegato
						;

/*
carico la tabella TLBPASPE
*/

--tlbpaspe = LOAD '$tlbpaspe_path'
tlbpaspe_filter = LOAD '$tlbpaspe_path'
			USING PigStorage (';')
			AS (
			     cd_istituto     :chararray
			     ,ndg            :chararray
			     ,datacont       :int
			     ,causale        :chararray
			     ,importo        :chararray
				);

/*faccio il filtro in base alla data di osservazione*/
--tlbpaspe_filter = FILTER tlbpaspe BY datacont <= $data_osservazione;
				
/*unisco le due tabelle*/				
tlbcidef_tlbpaspe_filter_join = JOIN tlbpaspe_filter BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca_collegato, ndg_collegato);

--STORE tlbcidef_tlbpaspe_filter_join INTO '/app/drlgd/datarequest/dt/tmp/debug/tlbcidef_tlbpaspe_filter_join' USING PigStorage (';');

fpasperd_between = FILTER tlbcidef_tlbpaspe_filter_join 
 BY (int)SUBSTRING((chararray)tlbpaspe_filter::datacont,0,6) >= (int)SUBSTRING((chararray)tlbcidef::datainiziodef,0,6)  
--AND (int)SUBSTRING((chararray)tlbpaspe_filter::datacont,0,6) <= (int)SUBSTRING( ToString( AddDuration( ToDate( (chararray)tlbcidef::datafinedef,'yyyyMMdd' ),'P2M' ),'yyyyMMdd' ),0,6 )
--AND ToDate((chararray)tlbpaspe_filter::datacont,'yyyyMMdd') <= AddDuration( ToDate( (chararray)tlbcidef::datafinedef,'yyyyMMdd' ),'P2M' )
AND (int)SUBSTRING((chararray)tlbpaspe_filter::datacont,0,6) < (int)SUBSTRING( (chararray)tlbcidef::datafinedef,0,6 )
;

/*
mapping che contiene differenza in gg tra fine ciclo e datacont 
per prendere successivamente il ciclo piu' vicino a quella data
*/
fpasperd_between_gen =  FOREACH fpasperd_between                          
				GENERATE 			                                          
						  tlbpaspe_filter::cd_istituto    AS  cd_istituto  
						 ,tlbpaspe_filter::ndg	          AS  ndg	          
						 ,tlbpaspe_filter::datacont	      AS  datacont	      
						 ,tlbpaspe_filter::causale	      AS  causale	      
						 ,tlbpaspe_filter::importo	      AS  importo	
						 ,tlbcidef::codicebanca		      AS  codicebanca
						 ,tlbcidef::ndgprincipale		  AS  ndgprincipale
						 ,tlbcidef::datainiziodef		  AS  datainiziodef
						 ,tlbcidef::datafinedef		      AS  datafinedef
						 ,DaysBetween( ToDate((chararray)tlbcidef::datafinedef,'yyyyMMdd' ), ToDate((chararray)tlbpaspe_filter::datacont,'yyyyMMdd' ) ) as days_diff
						;

--STORE fpasperd_between_gen INTO '/app/drlgd/datarequest/dt/tmp/debug/fpasperd_between_gen' USING PigStorage (';');

/* Estraggo il ciclo con la minima differenza tra fine ciclo e datacont */                           
fpasperd_between_grp = GROUP fpasperd_between_gen BY ( cd_istituto, ndg, datacont, causale, codicebanca, ndgprincipale );
fpasperd_between_limit = FOREACH fpasperd_between_grp 
                                {
                                 sorted = ORDER fpasperd_between_gen by days_diff ASC;
						         top    = LIMIT sorted 1;
						         generate group, FLATTEN(top);
								};

fpasperd_between_out =  FOREACH fpasperd_between_limit 
				GENERATE 
						  group.cd_istituto     AS  cd_istituto 
						 ,group.ndg	            AS  ndg 
						 ,group.datacont	    AS  datacont 
						 ,group.causale	        AS  causale 
						 ,top::importo	        AS  importo 
						 ,group.codicebanca	    AS  codicebanca 
						 ,group.ndgprincipale	AS  ndgprincipale 
						 ,top::datainiziodef	AS  datainiziodef 
						;
						
--STORE fpasperd_between_out INTO '/app/drlgd/datarequest/dt/tmp/debug/fpasperd_between_out' USING PigStorage (';');

/*tutti quelli con cicli agganciati per ndg*/
fpasperd_other = FILTER tlbcidef_tlbpaspe_filter_join 
 BY tlbcidef::codicebanca IS NOT NULL
;

/*ottengo la mapping finale*/
fpasperd_other_gen =  FOREACH fpasperd_other                          
				GENERATE 			                                          
						  tlbpaspe_filter::cd_istituto    AS  cd_istituto  
						 ,tlbpaspe_filter::ndg	          AS  ndg	          
						 ,tlbpaspe_filter::datacont	      AS  datacont	      
						 ,tlbpaspe_filter::causale	      AS  causale	      
						 ,tlbpaspe_filter::importo	      AS  importo	
						 ,NULL		                      AS  codicebanca
						 ,NULL		                      AS  ndgprincipale
						 ,NULL		                      AS  datainiziodef
						;

--STORE fpasperd_other_gen INTO '/app/drlgd/datarequest/dt/tmp/debug/fpasperd_other_gen' USING PigStorage (';');

/*rijoin per trovare quelli completamente fuori ciclo*/
fpasperd_other_gen_between_join = JOIN fpasperd_other_gen BY (cd_istituto, ndg, datacont) LEFT, fpasperd_between_out BY (cd_istituto, ndg, datacont);

/*prendo solo quelli senza record dentro ciclo*/ 
fpasperd_other_filter = FILTER fpasperd_other_gen_between_join 
 BY fpasperd_between_out::cd_istituto IS NULL
;

/*ottengo la mapping finale*/
fpasperd_other_out =  FOREACH fpasperd_other_filter                          
				GENERATE 			                                          
						  fpasperd_other_gen::cd_istituto     AS  cd_istituto  
						 ,fpasperd_other_gen::ndg	          AS  ndg	          
						 ,fpasperd_other_gen::datacont	      AS  datacont	      
						 ,fpasperd_other_gen::causale	      AS  causale	      
						 ,fpasperd_other_gen::importo	      AS  importo	
						 ,fpasperd_other_gen::codicebanca	  AS  codicebanca
						 ,fpasperd_other_gen::ndgprincipale	  AS  ndgprincipale
						 ,fpasperd_other_gen::datainiziodef	  AS  datainiziodef
						;

--STORE fpasperd_other_out INTO '/app/drlgd/datarequest/dt/tmp/debug/fpasperd_other_out' USING PigStorage (';');

/*prendo quelli senza cicli agganciati*/
fpasperd_null = FILTER tlbcidef_tlbpaspe_filter_join 
 BY tlbcidef::codicebanca IS NULL
;

/*ottengo la mapping finale dei senza cicli*/
fpasperd_null_out =  FOREACH fpasperd_null                          
				GENERATE 			                                          
						  tlbpaspe_filter::cd_istituto    AS  cd_istituto  
						 ,tlbpaspe_filter::ndg	          AS  ndg	          
						 ,tlbpaspe_filter::datacont	      AS  datacont	      
						 ,tlbpaspe_filter::causale	      AS  causale	      
						 ,tlbpaspe_filter::importo	      AS  importo	
						 ,NULL		                      AS  codicebanca
						 ,NULL		                      AS  ndgprincipale
						 ,NULL		                      AS  datainiziodef
						;

--STORE fpasperd_null_out INTO '/app/drlgd/datarequest/dt/tmp/debug/fpasperd_null_out' USING PigStorage (';');

------------------------------------------------
/*riprovo la join per ndg principale tra cidef e pasperd senza ciclo 
qui fino alla union finale tutta la parte precedente ripetuta per ndg principale
*/			
princip_tlbcidef_tlbpaspe_filter_join = JOIN fpasperd_null_out BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca, ndgprincipale);

--STORE princip_tlbcidef_tlbpaspe_filter_join INTO '/app/drlgd/datarequest/dt/tmp/debug/princip_tlbcidef_tlbpaspe_filter_join' USING PigStorage (';');

princip_fpasperd_between = FILTER princip_tlbcidef_tlbpaspe_filter_join 
 BY (int)SUBSTRING((chararray)fpasperd_null_out::datacont,0,6) >= (int)SUBSTRING((chararray)tlbcidef::datainiziodef,0,6)  
--AND (int)SUBSTRING((chararray)fpasperd_null_out::datacont,0,6) <= (int)SUBSTRING( ToString( AddDuration( ToDate( (chararray)tlbcidef::datafinedef,'yyyyMMdd' ),'P2M' ),'yyyyMMdd' ),0,6 )
--AND ToDate((chararray)fpasperd_null_out::datacont,'yyyyMMdd') <= AddDuration( ToDate( (chararray)tlbcidef::datafinedef,'yyyyMMdd' ),'P2M' )
AND (int)SUBSTRING((chararray)fpasperd_null_out::datacont,0,6) < (int)SUBSTRING( (chararray)tlbcidef::datafinedef,0,6 )
;

/*ottengo la mapping finale*/
princip_fpasperd_between_gen =  FOREACH princip_fpasperd_between                          
				GENERATE 			                                          
						  fpasperd_null_out::cd_istituto    AS  cd_istituto  
						 ,fpasperd_null_out::ndg	          AS  ndg	          
						 ,fpasperd_null_out::datacont	      AS  datacont	      
						 ,fpasperd_null_out::causale	      AS  causale	      
						 ,fpasperd_null_out::importo	      AS  importo	
						 ,tlbcidef::codicebanca		      AS  codicebanca
						 ,tlbcidef::ndgprincipale		  AS  ndgprincipale
						 ,tlbcidef::datainiziodef		  AS  datainiziodef
						 ,tlbcidef::datafinedef		      AS  datafinedef
						 ,DaysBetween( ToDate((chararray)tlbcidef::datafinedef,'yyyyMMdd' ), ToDate((chararray)fpasperd_null_out::datacont,'yyyyMMdd' ) ) as days_diff
						;
						
--STORE princip_fpasperd_between_gen INTO '/app/drlgd/datarequest/dt/tmp/debug/princip_fpasperd_between_gen' USING PigStorage (';');
						
/* Estraggo il ciclo con la minima differenza tra fine ciclo e datacont */                           
princip_fpasperd_between_grp = GROUP princip_fpasperd_between_gen BY ( cd_istituto, ndg, datacont, causale, codicebanca, ndgprincipale );
princip_fpasperd_between_limit = FOREACH princip_fpasperd_between_grp 
                                {
                                 sorted = ORDER princip_fpasperd_between_gen by days_diff ASC;
						         top    = LIMIT sorted 1;
						         generate group, FLATTEN(top);
								};

princip_fpasperd_between_out =  FOREACH princip_fpasperd_between_limit 
				GENERATE 
						  group.cd_istituto     AS  cd_istituto 
						 ,group.ndg	            AS  ndg 
						 ,group.datacont	    AS  datacont 
						 ,group.causale	        AS  causale 
						 ,top::importo	        AS  importo 
						 ,group.codicebanca	    AS  codicebanca 
						 ,group.ndgprincipale	AS  ndgprincipale 
						 ,top::datainiziodef	AS  datainiziodef 
						;

--STORE princip_fpasperd_between_out INTO '/app/drlgd/datarequest/dt/tmp/debug/princip_fpasperd_between_out' USING PigStorage (';');
						
/*tutti quelli con cicli agganciati per ndg*/
princip_fpasperd_other = FILTER princip_tlbcidef_tlbpaspe_filter_join 
 BY tlbcidef::codicebanca IS NOT NULL
;

/*ottengo la mapping finale*/
princip_fpasperd_other_gen =  FOREACH princip_fpasperd_other                          
				GENERATE 			                                          
						  fpasperd_null_out::cd_istituto    AS  cd_istituto  
						 ,fpasperd_null_out::ndg	          AS  ndg	          
						 ,fpasperd_null_out::datacont	      AS  datacont	      
						 ,fpasperd_null_out::causale	      AS  causale	      
						 ,fpasperd_null_out::importo	      AS  importo	
						 ,NULL		                      AS  codicebanca
						 ,NULL		                      AS  ndgprincipale
						 ,NULL		                      AS  datainiziodef
						;

--STORE princip_fpasperd_other_gen INTO '/app/drlgd/datarequest/dt/tmp/debug/princip_fpasperd_other_gen' USING PigStorage (';');

/*rijoin per trovare quelli completamente fuori ciclo*/
princip_fpasperd_other_gen_between_join = JOIN princip_fpasperd_other_gen BY (cd_istituto, ndg, datacont) LEFT, princip_fpasperd_between_out BY (cd_istituto, ndg, datacont);

/*prendo solo quelli senza record dentro ciclo*/ 
princip_fpasperd_other_filter = FILTER princip_fpasperd_other_gen_between_join 
 BY princip_fpasperd_between_out::cd_istituto IS NULL
;

/*ottengo la mapping finale*/
princip_fpasperd_other_out =  FOREACH princip_fpasperd_other_filter                          
				GENERATE 			                                          
						  princip_fpasperd_other_gen::cd_istituto     AS  cd_istituto  
						 ,princip_fpasperd_other_gen::ndg	          AS  ndg	          
						 ,princip_fpasperd_other_gen::datacont	      AS  datacont	      
						 ,princip_fpasperd_other_gen::causale	      AS  causale	      
						 ,princip_fpasperd_other_gen::importo	      AS  importo	
						 ,princip_fpasperd_other_gen::codicebanca	  AS  codicebanca
						 ,princip_fpasperd_other_gen::ndgprincipale	  AS  ndgprincipale
						 ,princip_fpasperd_other_gen::datainiziodef	  AS  datainiziodef
						;
						
--STORE princip_fpasperd_other_out INTO '/app/drlgd/datarequest/dt/tmp/debug/princip_fpasperd_other_out' USING PigStorage (';');

/*prendo quelli senza cicli agganciati*/
princip_fpasperd_null = FILTER princip_tlbcidef_tlbpaspe_filter_join 
 BY tlbcidef::codicebanca IS NULL
;

/*ottengo la mapping finale dei senza cicli*/
princip_fpasperd_null_out =  FOREACH princip_fpasperd_null                          
				GENERATE 			                                          
						  fpasperd_null_out::cd_istituto    AS  cd_istituto  
						 ,fpasperd_null_out::ndg	          AS  ndg	          
						 ,fpasperd_null_out::datacont	      AS  datacont	      
						 ,fpasperd_null_out::causale	      AS  causale	      
						 ,fpasperd_null_out::importo	      AS  importo	
						 ,NULL		                      AS  codicebanca
						 ,NULL		                      AS  ndgprincipale
						 ,NULL		                      AS  datainiziodef
						;

--STORE princip_fpasperd_null_out INTO '/app/drlgd/datarequest/dt/tmp/debug/princip_fpasperd_null_out' USING PigStorage (';');
------------------------------------------------

fpasperd_out = UNION fpasperd_between_out
                ,fpasperd_other_out
                ,princip_fpasperd_between_out
                ,princip_fpasperd_other_out
                ,princip_fpasperd_null_out
                ;

fpasperd_out_distinct = DISTINCT ( 
                          FOREACH fpasperd_out                          
				          GENERATE 			                                          
						  cd_istituto  
						 ,ndg	          
						 ,datacont	      
						 ,causale	      
						 ,importo	
						 ,codicebanca
						 ,ndgprincipale
						 ,datainiziodef)
						;

/*
carico la tabella pasperd alla data di osservazione
*/
tlbpaspeoss = LOAD '$tlbpaspe_path'
			USING PigStorage (';')
			AS (
			     cd_istituto     :chararray
			     ,ndg            :chararray
			     ,datacont       :int
			     ,causale        :chararray
			     ,importo        :chararray
				);

paspe_paspeoss_join = JOIN fpasperd_out_distinct BY (cd_istituto, ndg, datacont) FULL OUTER, 
                           tlbpaspeoss BY (cd_istituto, ndg, datacont);

/*ottengo la mappatura finale dove
1. se esiste solo soff tengo soff
2. se esistono entrambi prendo soffoss tranne chiave ciclo
3. se esiste solo soffoss tengo soffoss e chiave ciclo nulla
*/
paspe_paspeoss_gen = FOREACH paspe_paspeoss_join
			GENERATE
	         ( fpasperd_out_distinct::cd_istituto is not null? ( tlbpaspeoss::cd_istituto is not null? tlbpaspeoss::cd_istituto : fpasperd_out_distinct::cd_istituto ) : tlbpaspeoss::cd_istituto ) as cd_istituto 
			,( fpasperd_out_distinct::cd_istituto is not null? ( tlbpaspeoss::cd_istituto is not null? tlbpaspeoss::ndg : fpasperd_out_distinct::ndg ) : tlbpaspeoss::ndg ) as ndg 
			,( fpasperd_out_distinct::cd_istituto is not null? ( tlbpaspeoss::cd_istituto is not null? tlbpaspeoss::datacont : fpasperd_out_distinct::datacont ) : tlbpaspeoss::datacont ) as datacont 
			,( fpasperd_out_distinct::cd_istituto is not null? ( tlbpaspeoss::cd_istituto is not null? tlbpaspeoss::causale : fpasperd_out_distinct::causale ) : tlbpaspeoss::causale ) as causale 
			,( fpasperd_out_distinct::cd_istituto is not null? ( tlbpaspeoss::cd_istituto is not null? tlbpaspeoss::importo : fpasperd_out_distinct::importo ) : tlbpaspeoss::importo ) as importo 
			,( fpasperd_out_distinct::cd_istituto is not null? fpasperd_out_distinct::codicebanca : NULL ) as codicebanca
			,( fpasperd_out_distinct::cd_istituto is not null? fpasperd_out_distinct::ndgprincipale : NULL ) as ndgprincipale
			,( fpasperd_out_distinct::cd_istituto is not null? fpasperd_out_distinct::datainiziodef : NULL ) as datainiziodef 
		    ;
		    
paspe_paspeoss_gen_dist = DISTINCT (
            FOREACH paspe_paspeoss_gen
			GENERATE
	         cd_istituto 
			,ndg 
			,datacont 
			,causale 
			,importo 
			,codicebanca
			,ndgprincipale
			,datainiziodef)
		    ;

STORE paspe_paspeoss_gen_dist INTO '$fpasperd_outdir' USING PigStorage (';');
