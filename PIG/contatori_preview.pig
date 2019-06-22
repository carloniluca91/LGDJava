/*
$LastChangedDate:  $
$Rev:  $		
$Author:  $	

Sono coinvolti i seguenti parametri:
		-fposi_outdir
		-periodo
		-data_da;
        -data_a;
        -data_osservazione;
        -ufficio;
*/

/*
Carico il file fposi
*/
fposi_load = LOAD '$fposi_outdir' 
      USING PigStorage(';') 
      AS (
       codicebanca        : chararray
	  ,ndgprincipale      : chararray
	  ,datainiziodef      : chararray
      );

fposi_grp = GROUP fposi_load BY codicebanca;

fposi_count = FOREACH fposi_grp 
                    GENERATE 
                             '$ufficio' as ufficio
                            ,ToString(ToDate('$data_a','yyyyMMdd'),'yyyy-MM-dd') as datainiziodef
                            ,'FPOSI' as nome_file
                            ,group as codicebanca
                            ,COUNT(fposi_load) as numero_records
;


/*
Carico il file frapp
*/
frapp_load = LOAD '$frapp_outdir' 
      USING PigStorage(';') 
      AS (     
	       codicebanca     : chararray
	      ,ndg             : chararray
	      ,sportello       : chararray
	      ,conto           : chararray
	      ,datariferimento : chararray
      );

frapp_grp = GROUP frapp_load BY codicebanca;

frapp_count = FOREACH frapp_grp
                    GENERATE 
                             '$ufficio' as ufficio
                            ,ToString(ToDate('$data_a','yyyyMMdd'),'yyyy-MM-dd') as datariferimento
                            ,'FRAPP' as nome_file
                            ,group as codicebanca
                            ,COUNT(frapp_load) as numero_records
;

/*
Carico il file fanag
*/

fanag_load = LOAD '$fanag_outdir' 
      USING PigStorage(';') 
      AS (
           codicebanca      : chararray
          ,ndg              : chararray
          ,datariferimento  : chararray
      );

fanag_grp = GROUP fanag_load BY codicebanca;

fanag_count = FOREACH fanag_grp
                    GENERATE 
                             '$ufficio' as ufficio
                            ,ToString(ToDate('$data_a','yyyyMMdd'),'yyyy-MM-dd') as datariferimento
                            ,'FANAG' as nome_file
                            ,group as codicebanca
                            ,COUNT(fanag_load) as numero_records
;

/*
Carico il file fpasperd
*/

fpasperd_load = LOAD '$fpasperd_outdir' 
      USING PigStorage(';') 
      AS (
           cd_istituto    : chararray
          ,ndg	          : chararray
          ,datacont	      : chararray
          ,causale	      : chararray
          ,importo	      : chararray
          ,ndgprincipale  : chararray
          ,datainiziodef  : chararray
      );

fpasperd_grp = GROUP fpasperd_load BY cd_istituto;

fpasperd_count = FOREACH fpasperd_grp
                    GENERATE 
                             '$ufficio' as ufficio
                            ,ToString(ToDate('$data_osservazione','yyyyMMdd'),'yyyy-MM-dd') as datainiziodef
                            ,'FPASPERD' as nome_file
                            ,group as cd_istituto
                            ,COUNT(fpasperd_load) as numero_records
;

/*
Carico il file frapp_puma
*/

frapp_puma_load = LOAD '$frapp_puma_outdir' 
      USING PigStorage(';') 
      AS (
           cd_isti          : chararray
          ,ndg				: chararray
          ,sportello		: chararray
          ,dt_riferimento   : chararray
      );

frapp_puma_grp = GROUP frapp_puma_load BY cd_isti;

frapp_puma_count = FOREACH frapp_puma_grp
                    GENERATE 
                             '$ufficio' as ufficio
                            ,ToString(ToDate('$data_a','yyyyMMdd'),'yyyy-MM-dd') as dt_riferimento
                            ,'FRAPP_PUMA' as nome_file
                            ,group as cd_isti
                            ,COUNT(frapp_puma_load) as numero_records
;

/*
Carico il file sofferenze
*/

sofferenze_load = LOAD '$soff_outdir' 
      USING PigStorage(';') 
      AS (
           istituto				: chararray										
          ,ndg					: chararray							
          ,numerosofferenza		: chararray											
          ,datainizio	        : chararray
      );

sofferenze_grp = GROUP sofferenze_load BY istituto;

sofferenze_count = FOREACH sofferenze_grp
                    GENERATE 
                             '$ufficio' as ufficio
                            ,ToString(ToDate('$data_osservazione','yyyyMMdd'),'yyyy-MM-dd') as datainizio
                            ,'SOFFERENZE' as nome_file
                            ,group as istituto
                            ,COUNT(sofferenze_load) as numero_records
;

/*
Carico il file movimenti
*/

movimenti_load = LOAD '$mov_outdir' 
      USING PigStorage(';') 
      AS (
           istituto          : chararray
          ,ndg 				 : chararray
          ,datariferimento   : chararray
      );

movimenti_grp = GROUP movimenti_load BY istituto;

movimenti_count = FOREACH movimenti_grp
                    GENERATE 
                             '$ufficio' as ufficio
                            ,ToString(ToDate('$data_osservazione','yyyyMMdd'),'yyyy-MM-dd') as datariferimento
                            ,'MOVIMENTI' as nome_file
                            ,group as istituto
                            ,COUNT(movimenti_load) as numero_records
;

/*
Carico il file racc_inc
*/

racc_inc_load = LOAD '$racc_inc_outdir' 
      USING PigStorage(';') 
      AS (
           ist_ric_inc  : chararray            
          ,ndg_ric_inc  : chararray
      );

racc_inc_grp = GROUP racc_inc_load BY ist_ric_inc;

racc_inc_count = FOREACH racc_inc_grp
                    GENERATE 
                             '$ufficio' as ufficio
                            ,ToString(CurrentTime(),'yyyy-MM-dd') as dataelab
                            ,'RACC_INC' as nome_file
                            ,group as ist_ric_inc
                            ,COUNT(racc_inc_load) as numero_records
;

/*
Carico il file racc_soff
*/

racc_soff_load = LOAD '$racc_soff_outdir' 
      USING PigStorage(';') 
      AS (
           IST_RIC_SOF  : chararray
          ,NDG_RIC_SOF  : chararray
      );

racc_soff_grp = GROUP racc_soff_load BY IST_RIC_SOF;

racc_soff_count = FOREACH racc_soff_grp
                    GENERATE 
                             '$ufficio' as ufficio
                            ,ToString(CurrentTime(),'yyyy-MM-dd') as dataelab
                            ,'RACC_SOFF' as nome_file
                            ,group as IST_RIC_SOF
                            ,COUNT(racc_soff_load) as numero_records
;

contatori_all_out = UNION fposi_count, frapp_count, fanag_count, fpasperd_count, frapp_puma_count, 
                          sofferenze_count, movimenti_count, racc_inc_count, racc_soff_count
;

STORE contatori_all_out  INTO '$contatori_export_dir' USING PigStorage(';');
