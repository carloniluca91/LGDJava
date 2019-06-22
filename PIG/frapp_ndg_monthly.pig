/*

$LastChangedDate: 2016-10-28 14:49:31 +0200 (ven, 28 ott 2016) $
$Rev: 585 $		
$Author: arzelenko $	

Sono coinvolti i seguenti parametri :
		-cicli_ndg_path
		-tlburtt_path
		-frapp_ndg_outdir
		-periodo
*/

register LeastDate.jar;

/*
Programma bancapopolare su mapping RAPP
*/

/*
carico il file di cicli
*/



tlbcidef = LOAD '$cicli_ndg_path'
      USING PigStorage(';') 
  	  AS (
			codicebanca           : chararray
		   ,ndgprincipale         : chararray
		   ,datainiziodef         : int
		   ,datafinedef           : int
		   ,datainiziopd          : chararray
		   ,datainizioristrutt    : chararray
		   ,datainizioinc         : chararray
		   ,datainiziosoff        : chararray 
		   ,c_key                 : chararray
		   ,tipo_segmne           : chararray
		   ,sae_segm              : chararray
		   ,rae_segm              : chararray
		   ,segmento              : chararray
		   ,tp_ndg                : chararray
		   ,provincia_segm        : chararray
		   ,databilseg            : chararray
		   ,strbilseg             : chararray
		   ,attivobilseg          : chararray
		   ,fatturbilseg          : chararray
           ,ndg_collegato         : chararray
		   ,codicebanca_collegato : chararray
		   ,cd_collegamento       : chararray
		   ,cd_fiscale            : chararray
		   ,dt_rif_udct           : int
         );

/* filtro per range di mesi per leggere solo i dati della finestra temporale del ciclo corrente */
--tlbcidef = FILTER tlbcidef1 BY SUBSTRING((chararray)datainiziodef,0,6) >= SUBSTRING('$periodo',0,6) AND SUBSTRING((chararray)datainiziodef,0,6) <= ( SUBSTRING('$periodo',7,13) is null ? SUBSTRING('$periodo',0,6) : SUBSTRING('$periodo',7,13) );

cicli_ndg_princ = FILTER tlbcidef BY cd_collegamento IS NULL;

cicli_ndg_coll = FILTER tlbcidef BY cd_collegamento IS NOT NULL;

/*
Carico la tabella TLBURTT
DT_RIFERIMENTO = DT_INIZIO_CICLO
CD_ISTITUTO = CD_ISTI
NDG =NDG 
*/

tlburtt =  LOAD '$tlburtt_path' 
     USING PigStorage(';') 
     AS (
			 cd_istituto         :chararray
			,ndg                 :chararray
			,sportello           :chararray
			,conto               :chararray
			,progr_segmento      :int
			,dt_riferimento      :int
			,conto_esteso        :chararray
			,forma_tecnica       :chararray
			,flag_durata_contr   :chararray
			,cred_agevolato      :chararray
			,operazione_pool     :chararray
			,dt_accensione       :chararray
			,dt_estinzione       :chararray
			,dt_scadenza         :chararray
			,organo_deliber      :chararray
			,dt_delibera         :chararray
			,dt_scad_fido        :chararray
			,origine_rapporto    :chararray
			,tp_ammortamento     :chararray
			,tp_rapporto         :chararray
			,period_liquid       :chararray
			,freq_remargining    :chararray
			,cd_prodotto_ris     :chararray
			,durata_originaria   :chararray
			,divisa              :chararray
			,score_erogaz        :chararray
			,durata_residua      :chararray
			,categoria_sof       :chararray
			,categoria_inc       :chararray
			,dur_res_default     :chararray
			,flag_margine        :chararray
			,dt_entrata_def      :chararray
			,tp_contr_rapp       :chararray
			,cd_eplus            :chararray
			,r792_tipocartol     :chararray
		);
		
/*filtro con il progr_segmento uguale a zero*/

tlburtt_filter = FILTER tlburtt BY progr_segmento == 0;

/*
JOIN tra le due tabelle TLBCIDE e TLBURTT_FILTER per principali
*/
tlbcidef_princ_tlburtt_filter_join = JOIN cicli_ndg_princ BY (codicebanca_collegato, ndg_collegato), tlburtt_filter BY (cd_istituto, ndg);

/* 
join dei collegati per ist,ndg e data per prendere le anagrafiche per le sole date presenti in ucol 
*/
tlbcidef_coll_tlburtt_join = JOIN  cicli_ndg_coll BY (codicebanca_collegato, ndg_collegato), tlburtt_filter BY (cd_istituto, ndg);

/* 
prende solo i PRINCIPALI in base alla presenza del codice collegamento 
per i quali il range dell'anagrafica deve essere tra inizio e fine ciclo 
*/
tlbcidef_princ_tlburtt_filter_join_filt = FILTER tlbcidef_princ_tlburtt_filter_join 
 BY ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')  
AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(ToString(AddDuration( ToDate( (chararray)LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd') ,$data_a),'yyyyMMdd' ),'$numero_mesi_2' ),'yyyyMMdd'),0,6 ) 
;

tlbcidef_urtt_princ = FOREACH tlbcidef_princ_tlburtt_filter_join_filt
                              GENERATE 
		                      cicli_ndg_princ::codicebanca              AS  codicebanca     
							 ,cicli_ndg_princ::ndgprincipale            AS  ndgprincipale     
							 ,cicli_ndg_princ::codicebanca_collegato    AS  codicebanca_collegato     
							 ,cicli_ndg_princ::ndg_collegato            AS  ndg_collegato  
							 ,cicli_ndg_princ::datainiziodef            AS  datainiziodef     
							 ,cicli_ndg_princ::datafinedef              AS  datafinedef     
							 ,tlburtt_filter::cd_istituto        AS  cd_istituto     
							 ,tlburtt_filter::ndg	             AS  ndg
							 ,tlburtt_filter::sportello          AS  sportello      
							 ,tlburtt_filter::conto              AS  conto       
							 ,tlburtt_filter::dt_riferimento     AS  dt_riferimento     
							 ,tlburtt_filter::conto_esteso       AS  conto_esteso     
							 ,tlburtt_filter::forma_tecnica      AS  forma_tecnica      
							 ,tlburtt_filter::dt_accensione      AS  dt_accensione     
							 ,tlburtt_filter::dt_estinzione      AS  dt_estinzione     
							 ,tlburtt_filter::dt_scadenza        AS  dt_scadenza     
							 ,tlburtt_filter::tp_ammortamento    AS  tp_ammortamento    
							 ,tlburtt_filter::tp_rapporto        AS  tp_rapporto     
							 ,tlburtt_filter::period_liquid      AS  period_liquid     
							 ,tlburtt_filter::cd_prodotto_ris    AS  cd_prodotto_ris    
							 ,tlburtt_filter::durata_originaria  AS  durata_originaria    
							 ,tlburtt_filter::divisa             AS  divisa       
							 ,tlburtt_filter::durata_residua     AS  durata_residua     
							 ,tlburtt_filter::tp_contr_rapp      AS  tp_contr_rapp
                            ;

/* 
filtro per range di anagrafica esteso anche sui collegati 
*/
tlbcidef_coll_tlburtt_filter_join_filt = FILTER tlbcidef_coll_tlburtt_join 
 BY ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')  
AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(ToString(AddDuration( ToDate( (chararray)LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd') ,$data_a),'yyyyMMdd' ),'$numero_mesi_2' ),'yyyyMMdd'),0,6 ) 
;


tlbcidef_urtt_coll = FOREACH tlbcidef_coll_tlburtt_filter_join_filt
                              GENERATE 
		                      cicli_ndg_coll::codicebanca              AS  codicebanca     
							 ,cicli_ndg_coll::ndgprincipale            AS  ndgprincipale     
							 ,cicli_ndg_coll::codicebanca_collegato    AS  codicebanca_collegato     
							 ,cicli_ndg_coll::ndg_collegato            AS  ndg_collegato  
							 ,cicli_ndg_coll::datainiziodef            AS  datainiziodef     
							 ,cicli_ndg_coll::datafinedef              AS  datafinedef     
							 ,tlburtt_filter::cd_istituto        AS  cd_istituto     
							 ,tlburtt_filter::ndg	             AS  ndg
							 ,tlburtt_filter::sportello          AS  sportello      
							 ,tlburtt_filter::conto              AS  conto       
							 ,tlburtt_filter::dt_riferimento     AS  dt_riferimento     
							 ,tlburtt_filter::conto_esteso       AS  conto_esteso     
							 ,tlburtt_filter::forma_tecnica      AS  forma_tecnica      
							 ,tlburtt_filter::dt_accensione      AS  dt_accensione     
							 ,tlburtt_filter::dt_estinzione      AS  dt_estinzione     
							 ,tlburtt_filter::dt_scadenza        AS  dt_scadenza     
							 ,tlburtt_filter::tp_ammortamento    AS  tp_ammortamento    
							 ,tlburtt_filter::tp_rapporto        AS  tp_rapporto     
							 ,tlburtt_filter::period_liquid      AS  period_liquid     
							 ,tlburtt_filter::cd_prodotto_ris    AS  cd_prodotto_ris    
							 ,tlburtt_filter::durata_originaria  AS  durata_originaria    
							 ,tlburtt_filter::divisa             AS  divisa       
							 ,tlburtt_filter::durata_residua     AS  durata_residua     
							 ,tlburtt_filter::tp_contr_rapp      AS  tp_contr_rapp
                            ;

cicli_all = UNION tlbcidef_urtt_princ
                 ,tlbcidef_urtt_coll
                 ;

tlbcidef_tlburtt = DISTINCT (
                   FOREACH cicli_all
                   GENERATE
					  codicebanca     
					 ,ndgprincipale     
					 ,codicebanca_collegato     
					 ,ndg_collegato  
					 ,datainiziodef     
					 ,datafinedef     
					 ,cd_istituto     
					 ,ndg
					 ,sportello      
					 ,conto       
					 ,dt_riferimento     
					 ,conto_esteso     
					 ,forma_tecnica      
					 ,dt_accensione     
					 ,dt_estinzione     
					 ,dt_scadenza     
					 ,tp_ammortamento    
					 ,tp_rapporto     
					 ,period_liquid     
					 ,cd_prodotto_ris    
					 ,durata_originaria    
					 ,divisa       
					 ,durata_residua     
					 ,tp_contr_rapp)
				;    

STORE tlbcidef_tlburtt INTO '$frapp_ndg_outdir/$periodo' USING PigStorage(';');
