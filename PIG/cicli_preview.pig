/*
$LastChangedDate:  $
$Rev:  $		
$Author:  $	

Sono coinvolti i seguenti parametri:
		-fposi_outdir
        -data_a;
        -ufficio;
        -fposi_prev_dett_outdir
        -fposi_prev_sint_outdir
*/

--register DoubleConverter.jar;
register DoubleConverter.jar;


/*
Carico il file fposi
*/
fposi_load = LOAD '$fposi_outdir' 
      USING PigStorage(';') 
      AS (
           codicebanca          : chararray
          ,ndgprincipale        : chararray
          ,datainiziodef        : chararray
          ,datafinedef          : chararray
          ,datainiziopd         : chararray
          ,datainizioinc        : chararray
          ,datainizioristrutt   : chararray
          ,datasofferenza       : chararray
          ,totaccordatodatdef   : double
          ,totutilizzdatdef     : double
          ,segmento             : chararray
          ,naturagiuridica_segm : chararray
      );

/*
preparazione della base per la produzione dei files successivi
si precalcolano alcuni campi :
segmento_calc - segmento
ciclo_soff - ciclo a sofferenza
flag_aperto - flag ciclo aperto
stato_anagrafico :
	Il default - PASTDUE.
	Se nessuna delle date DATAINIZIOPD, DATAINIZIOINC, DATAINIZIORISTRUTT, DATASOFFERENZA è impostata, lo stato è PASTDUE.
	Se impostata la DATAINIZIOPD, e le altre date sono inferiori o non valorizzate, lo stato è PASTDUE.
	Se impostata la DATAINIZIOINC e le altre date sono inferiori o non valorizzate, lo stato è INCA. 
	Se impostata la DATAINIZIORISTRUTT e le altre date sono inferiori o non valorizzate, lo stato è RISTR. 
	Se impostata la DATASOFFERENZA e le altre date sono inferiori o non valorizzate, lo stato è SOFF. 

*/
fposi_base = FOREACH fposi_load 
                     GENERATE
                              '$ufficio'           as ufficio
                             ,codicebanca          as codicebanca
                             ,ToString(ToDate('$data_a','yyyyMMdd'),'yyyy-MM-dd') as datarif
                             ,ndgprincipale        as ndgprincipale
                             ,datainiziodef        as datainiziodef
                             ,datafinedef          as datafinedef
                             ,datainiziopd         as datainiziopd
                             ,datainizioinc        as datainizioinc
                             ,datainizioristrutt   as datainizioristrutt
                             ,datasofferenza       as datasofferenza
                             ,totaccordatodatdef   as totaccordatodatdef
                             ,totutilizzdatdef     as totutilizzdatdef
                             ,(naturagiuridica_segm != 'CO' AND segmento in ('01','02','03','21')?'IM': (segmento == '10'?'PR':'AL')) as segmento_calc
                             ,( datasofferenza is null?'N':'S') as ciclo_soff
                             ,( (datainiziopd is null and datainizioinc is null and datainizioristrutt is null and datasofferenza is null)?'PASTDUE':
                                ( datainiziopd is not null and (datainiziopd<(datasofferenza is null?'99999999':datasofferenza) and datainiziopd<(datainizioinc is null?'99999999':datainizioinc) and datainiziopd<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'PASTDUE': 
                                 ( datainizioinc is not null and (datainizioinc<(datainiziopd is null?'99999999':datainiziopd) and datainizioinc<(datasofferenza is null?'99999999':datasofferenza) and datainizioinc<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'INCA':
                                  ( datainizioristrutt is not null and (datainizioristrutt<(datainiziopd is null?'99999999':datainiziopd) and datainizioristrutt<(datainizioinc is null?'99999999':datainizioinc) and datainizioristrutt<(datasofferenza is null?'99999999':datasofferenza))? 'RISTR':
                                   ( datasofferenza is not null and (datasofferenza<(datainiziopd is null?'99999999':datainiziopd) and datasofferenza<(datainizioinc is null?'99999999':datainizioinc) and datasofferenza<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'SOFF': 'PASTDUE'
                                   )
                                  )
                                 )
                                )
                              ) as stato_anagrafico
                               -- se maggiore della data_a = A altrimenti C
                             ,( (int)datafinedef > $data_a ? 'A' : 'C' ) as flag_aperto
;

fposi_grp = GROUP fposi_base BY ( codicebanca, ndgprincipale, datainiziodef );

/*
proiezione intermedia file dettaglio con somma di saldi
*/
fposi_gen = FOREACH fposi_grp 
                     GENERATE
                             FLATTEN(fposi_base.ufficio)             as ufficio
                            ,group.codicebanca                       as codicebanca
                            ,FLATTEN(fposi_base.datarif)             as datarif
                            ,group.ndgprincipale                     as ndgprincipale
                            ,group.datainiziodef                     as datainiziodef
                            ,FLATTEN(fposi_base.datafinedef)         as datafinedef
                            ,FLATTEN(fposi_base.datainiziopd)        as datainiziopd
                            ,FLATTEN(fposi_base.datainizioinc)       as datainizioinc
                            ,FLATTEN(fposi_base.datainizioristrutt)  as datainizioristrutt
                            ,FLATTEN(fposi_base.datasofferenza)      as datasofferenza
                            ,SUM(fposi_base.totaccordatodatdef)      as totaccordatodatdef
                            ,SUM(fposi_base.totutilizzdatdef)        as totutilizzdatdef
                            ,FLATTEN(fposi_base.segmento_calc)       as segmento_calc
                            ,FLATTEN(fposi_base.ciclo_soff)          as ciclo_soff
                            ,FLATTEN(fposi_base.stato_anagrafico)    as stato_anagrafico
;

/*
proiezione finale file dettaglio con trasformazioni varie per il caricamento sul db
*/
fposi_gen2 = FOREACH fposi_gen 
                     GENERATE
                              ufficio
                             ,codicebanca
                             ,datarif
                             ,ndgprincipale
                             ,ToString(ToDate(datainiziodef,'yyyyMMdd'),'yyyy-MM-dd') as datainiziodef
                             ,ToString(ToDate(datafinedef,'yyyyMMdd'),'yyyy-MM-dd') as datafinedef
                             ,ToString(ToDate(datainiziopd,'yyyyMMdd'),'yyyy-MM-dd') as datainiziopd
                             ,ToString(ToDate(datainizioinc,'yyyyMMdd'),'yyyy-MM-dd') as datainizioinc
                             ,ToString(ToDate(datainizioristrutt,'yyyyMMdd'),'yyyy-MM-dd') as datainizioristrutt
                             ,ToString(ToDate(datasofferenza,'yyyyMMdd'),'yyyy-MM-dd') as datasofferenza
                             ,DoubleConverter(totaccordatodatdef) as totaccordatodatdef
                             ,DoubleConverter(totutilizzdatdef) as totutilizzdatdef
                             ,segmento_calc
                             ,ciclo_soff
                             ,stato_anagrafico
;

STORE fposi_gen2  INTO '$fposi_prev_dett' USING PigStorage(';');

fposi_sint_grp = GROUP fposi_base BY ( ufficio, datarif, codicebanca, segmento_calc, SUBSTRING(datainiziodef,0,6), SUBSTRING(datafinedef,0,6), stato_anagrafico, ciclo_soff, flag_aperto );

/*
proiezione intermedia per file di sintesi con somma dei saldi e conteggio di records
*/
fposi_sint_gen = FOREACH fposi_sint_grp 
                     GENERATE
                             group.ufficio          as ufficio
                            ,group.datarif          as datarif
                            ,group.flag_aperto      as flag_aperto
                            ,group.codicebanca      as codicebanca
                            ,group.segmento_calc    as segmento_calc
                            ,SUBSTRING(group.$4,0,6) as mese_apertura
                            ,SUBSTRING(group.$5,0,6)   as mese_chiusura
                            ,group.stato_anagrafico as stato_anagrafico
                            ,group.ciclo_soff       as ciclo_soff
                            ,COUNT(fposi_base)      as row_count
                            ,SUM(fposi_base.totaccordatodatdef) as totaccordatodatdef
                            ,SUM(fposi_base.totutilizzdatdef)   as totutilizzdatdef 
;

/*
proiezione finale per file di sintesi con alcuni trasformazioni per il caricamento sul db
*/
fposi_sint_gen2 = FOREACH fposi_sint_gen 
                     GENERATE
                             ufficio
                            ,datarif
                            ,flag_aperto
                            ,codicebanca
                            ,segmento_calc
                            ,mese_apertura
                            ,mese_chiusura
                            ,stato_anagrafico
                            ,ciclo_soff
                            ,row_count
                            ,DoubleConverter(totaccordatodatdef) as totaccordatodatdef
                            ,DoubleConverter(totutilizzdatdef) as totutilizzdatdef
;

STORE fposi_sint_gen2  INTO '$fposi_prev_sint' USING PigStorage(';');
