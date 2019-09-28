/*
$LastChangedDate:  $
$Rev:  $		
$Author:  $	

Sono coinvolti i seguenti parametri:
		-soff_outdir
        -data_a;
        -ufficio;
        -soff_prev_dett_outdir
        -soff_prev_sint_outdir
*/

--register DoubleConverter.jar;
register DoubleConverter.jar;


/*
Carico il file sofferenze
*/
soff_load = LOAD '$soff_outdir' 
      USING PigStorage(';') 
      AS (
           istituto               : chararray 
          ,ndg                    : chararray 
          ,numerosofferenza       : chararray
          ,datainizio             : chararray
          ,datafine               : chararray
          ,statopratica           : chararray
          ,saldoposizione         : chararray
          ,saldoposizionecontab   : chararray
      );

/*
preparazione della base per la produzione dei files successivi
*/
soff_base = FOREACH soff_load 
                     GENERATE
                              '$ufficio'             as ufficio
                             ,ToString(ToDate('$data_a','yyyyMMdd'),'yyyy-MM-dd') as datarif
                             ,istituto               as istituto
                             ,ndg                    as ndg	
                             ,numerosofferenza       as numerosofferenza
                             ,datainizio             as datainizio
                             ,datafine               as datafine
                             ,statopratica           as statopratica
                             ,(double)REPLACE(saldoposizione,',','.')         as saldoposizione
                             ,(double)REPLACE(saldoposizionecontab,',','.')   as saldoposizionecontab
;

soff_grp = GROUP soff_base BY ( istituto, ndg, numerosofferenza );

/*
proiezione intermedia file dettaglio con somma di saldi
*/
soff_gen = FOREACH soff_grp 
                     GENERATE
                             FLATTEN(soff_base.ufficio)           as ufficio
                            ,FLATTEN(soff_base.datarif)          as datarif
                            ,group.istituto                       as istituto
                            ,group.ndg                            as ndg
                            ,group.numerosofferenza               as numerosofferenza
                            ,FLATTEN(soff_base.datainizio)        as datainizio
                            ,FLATTEN(soff_base.datafine)          as datafine
                            ,FLATTEN(soff_base.statopratica)      as statopratica
                            ,SUM(soff_base.saldoposizione)        as saldoposizione
                            ,SUM(soff_base.saldoposizionecontab)  as saldoposizionecontab
;

/*
proiezione finale file dettaglio con trasformazioni varie per il caricamento sul db
*/
soff_gen2 = FOREACH soff_gen 
                     GENERATE
                              ufficio
                             ,datarif
                             ,istituto
                             ,ndg
                             ,numerosofferenza
                             ,ToString(ToDate(datainizio,'yyyyMMdd'),'yyyy-MM-dd') as datainizio
                             ,ToString(ToDate(datafine,'yyyyMMdd'),'yyyy-MM-dd')   as datafine
                             ,statopratica
                             ,DoubleConverter(saldoposizione)       as saldoposizione
                             ,DoubleConverter(saldoposizionecontab) as saldoposizionecontab
;

STORE soff_gen2  INTO '$soff_prev_dett' USING PigStorage(';');

soff_sint_grp = GROUP soff_base BY ( ufficio, datarif, istituto, SUBSTRING(datainizio,0,6), SUBSTRING(datafine,0,6), statopratica );

/*
proiezione intermedia per file di sintesi con somma dei saldi e conteggio di records
*/
soff_sint_gen = FOREACH soff_sint_grp 
                     GENERATE
                             group.ufficio           as ufficio
                            ,group.datarif           as datarif
                            ,group.istituto          as istituto
                            ,SUBSTRING(group.$3,0,6) as mese_inizio
                            ,SUBSTRING(group.$4,0,6)   as mese_fine
                            ,group.statopratica      as statopratica
                            ,COUNT(soff_base)        as row_count
                            ,SUM(soff_base.saldoposizione)        as saldoposizione
                            ,SUM(soff_base.saldoposizionecontab)  as saldoposizionecontab 
;

/*
proiezione finale per file di sintesi con alcuni trasformazioni per il caricamento sul db
*/
soff_sint_gen2 = FOREACH soff_sint_gen 
                     GENERATE
                             ufficio
                            ,datarif
                            ,istituto
                            ,mese_inizio
                            ,mese_fine
                            ,statopratica
                            ,row_count
                            ,DoubleConverter(saldoposizione)       as saldoposizione
                            ,DoubleConverter(saldoposizionecontab) as saldoposizionecontab
;

STORE soff_sint_gen2  INTO '$soff_prev_sint' USING PigStorage(';');
