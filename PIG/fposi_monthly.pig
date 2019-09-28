/*
$LastChangedDate: 2017-03-02 12:18:25 +0100 (gio, 02 mar 2017) $
$Rev: 610 $		
$Author: arzelenko $	

Sono coinvolti i seguenti parametri:
		-cicli_ndg_path
		-tlbungr_path
		-tlbposi_path
		-tlbposi_path_mese_prec
		-tlbuact_path
		-fposi_outdir
		-periodo

*/

register DoubleConverter.jar;
/*
Carico il file tmp_cicli_ndg
*/



tlbcidef1 = LOAD '$cicli_ndg_path'
      USING PigStorage(';') 
  	  AS (
			codicebanca           : chararray
		   ,ndgprincipale         : chararray
		   ,datainiziodef         : int
		   ,datafinedef           : chararray
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
		   ,util_cassa            : double
		   ,imp_accordato         : double
         );

ciclilav_listadef = LOAD '$ciclindg_listadef'
      USING PigStorage(';') 
  	  AS (
  	       cd_isti         : chararray
          ,ndg_principale  : chararray
          ,dt_inizio_ciclo : int
          ,cd_isti_coll    : chararray
         );

/* filtro per un mese. usare solo per processi con ciclo d'elaborazione mensile
   in piu contiene filtraggio per prendere i records riferiti al mese del ciclo in elaborazione
 */
--tlbcidef = FILTER tlbcidef1 BY SUBSTRING((chararray)datainiziodef,0,6) == '$periodo'
--                            AND datainiziodef == dt_rif_udct;

/* filtro per range di mesi per leggere solo i dati della finestra temporale del ciclo corrente */
tlbcidef_filter1 = FILTER tlbcidef1 BY SUBSTRING((chararray)datainiziodef,0,6) >= SUBSTRING('$periodo',0,6) 
AND SUBSTRING((chararray)datainiziodef,0,6) <= ( SUBSTRING('$periodo',7,13) is null ? SUBSTRING('$periodo',0,6) : SUBSTRING('$periodo',7,13) )
;

cicli_lista_join = JOIN  tlbcidef_filter1   BY ( codicebanca, ndgprincipale, datainiziodef, codicebanca_collegato )
                        ,ciclilav_listadef  BY ( cd_isti, ndg_principale, dt_inizio_ciclo, cd_isti_coll );

tlbcidef_filter = FOREACH cicli_lista_join
                  GENERATE 
                tlbcidef_filter1::codicebanca           as codicebanca
		   	   ,tlbcidef_filter1::ndgprincipale         as ndgprincipale
		   	   ,tlbcidef_filter1::datainiziodef         as datainiziodef
			   ,tlbcidef_filter1::datafinedef           as datafinedef
			   ,tlbcidef_filter1::datainiziopd          as datainiziopd
			   ,tlbcidef_filter1::datainizioristrutt    as datainizioristrutt
			   ,tlbcidef_filter1::datainizioinc         as datainizioinc
			   ,tlbcidef_filter1::datainiziosoff        as datainiziosoff
			   ,tlbcidef_filter1::c_key                 as c_key
			   ,tlbcidef_filter1::tipo_segmne           as tipo_segmne
			   ,tlbcidef_filter1::sae_segm              as sae_segm
			   ,tlbcidef_filter1::rae_segm              as rae_segm
			   ,tlbcidef_filter1::segmento              as segmento
			   ,tlbcidef_filter1::tp_ndg                as tp_ndg
			   ,tlbcidef_filter1::provincia_segm        as provincia_segm
			   ,tlbcidef_filter1::databilseg            as databilseg
			   ,tlbcidef_filter1::strbilseg             as strbilseg
			   ,tlbcidef_filter1::attivobilseg          as attivobilseg
			   ,tlbcidef_filter1::fatturbilseg          as fatturbilseg
	           ,tlbcidef_filter1::ndg_collegato         as ndg_collegato
			   ,tlbcidef_filter1::codicebanca_collegato as codicebanca_collegato
			   ,tlbcidef_filter1::cd_collegamento       as cd_collegamento
			   ,tlbcidef_filter1::cd_fiscale            as cd_fiscale
			   ,tlbcidef_filter1::dt_rif_udct           as dt_rif_udct
			   ,tlbcidef_filter1::util_cassa            as util_cassa
			   ,tlbcidef_filter1::imp_accordato         as imp_accordato
               ;

cicli_group = GROUP tlbcidef_filter 
  BY (codicebanca_collegato, ndg_collegato, datainiziodef );
/*
crea la lista con minima data udct a parita di ogni singolo componente e dt_inizio ciclo per filtrare file cicli 
eliminando i doppi causati da diverse date presenti in udct ottenendo cosi un record per componente
*/
cicli_minudct_lista = FOREACH cicli_group
                      GENERATE 
                        group.codicebanca_collegato       as codicebanca_collegato
			           ,group.ndg_collegato               as ndg_collegato
					   ,group.datainiziodef               as datainiziodef
			   		   ,MIN(tlbcidef_filter.dt_rif_udct)  as dt_rif_udct
                       ;

cicli_lista_join = JOIN tlbcidef_filter     BY ( codicebanca_collegato, ndg_collegato, datainiziodef, dt_rif_udct )
                       ,cicli_minudct_lista BY ( codicebanca_collegato, ndg_collegato, datainiziodef, dt_rif_udct );

tlbcidef = FOREACH cicli_lista_join
           GENERATE 
            tlbcidef_filter::codicebanca            as codicebanca
		   ,tlbcidef_filter::ndgprincipale          as ndgprincipale
		   ,tlbcidef_filter::datainiziodef          as datainiziodef
		   ,tlbcidef_filter::datafinedef            as datafinedef
		   ,tlbcidef_filter::datainiziopd           as datainiziopd
		   ,tlbcidef_filter::datainizioristrutt     as datainizioristrutt
		   ,tlbcidef_filter::datainizioinc          as datainizioinc
		   ,tlbcidef_filter::datainiziosoff         as datainiziosoff
		   ,tlbcidef_filter::c_key                  as c_key
		   ,tlbcidef_filter::tipo_segmne            as tipo_segmne
		   ,tlbcidef_filter::sae_segm               as sae_segm
		   ,tlbcidef_filter::rae_segm               as rae_segm
		   ,tlbcidef_filter::segmento               as segmento
		   ,tlbcidef_filter::tp_ndg                 as tp_ndg
		   ,tlbcidef_filter::provincia_segm         as provincia_segm
		   ,tlbcidef_filter::databilseg             as databilseg
		   ,tlbcidef_filter::strbilseg              as strbilseg
		   ,tlbcidef_filter::attivobilseg           as attivobilseg
		   ,tlbcidef_filter::fatturbilseg           as fatturbilseg
           ,tlbcidef_filter::ndg_collegato          as ndg_collegato
		   ,tlbcidef_filter::codicebanca_collegato  as codicebanca_collegato
		   ,tlbcidef_filter::cd_collegamento        as cd_collegamento
		   ,tlbcidef_filter::cd_fiscale             as cd_fiscale
		   ,tlbcidef_filter::dt_rif_udct            as  dt_rif_udct
		   ,tlbcidef_filter::util_cassa             as util_cassa
		   ,tlbcidef_filter::imp_accordato          as imp_accordato
           ;

/*
Carico il file TLBUNGR.txt
*/
tlbungr = LOAD '$tlbungr_path'
          USING  PigStorage(';') 
          AS (
   				dt_riferimento: int
			   ,cd_istituto: chararray
			   ,ndg: chararray
			   ,ndg_gruppo: chararray
);


/*
Join per codicebanca, datainiziodef, ndgprincipale su cd_istituto, dt_riferimento, ndg
*/

tlbcidef_tlbungr_join = JOIN tlbcidef BY (codicebanca, datainiziodef, ndgprincipale) LEFT, tlbungr BY (cd_istituto, dt_riferimento, ndg);
tlbcidef_tlbungr = FOREACH tlbcidef_tlbungr_join
      GENERATE  tlbcidef::codicebanca as codicebanca
			   ,tlbcidef::ndgprincipale as ndgprincipale
			   ,tlbcidef::datainiziodef as datainiziodef
			   ,tlbcidef::datafinedef as datafinedef
			   ,tlbcidef::datainiziopd as datainiziopd
			   ,tlbcidef::datainizioristrutt as datainizioristrutt
			   ,tlbcidef::datainizioinc as datainizioinc
			   ,tlbcidef::datainiziosoff as datainiziosoff
			   ,tlbcidef::c_key as c_key
			   ,tlbcidef::tipo_segmne as tipo_segmne
			   ,tlbcidef::sae_segm as sae_segm
			   ,tlbcidef::rae_segm as rae_segm
			   ,tlbcidef::segmento as segmento
			   ,tlbcidef::tp_ndg as tp_ndg
			   ,tlbcidef::provincia_segm as provincia_segm
			   ,tlbcidef::databilseg as databilseg
			   ,tlbcidef::strbilseg as strbilseg
			   ,tlbcidef::attivobilseg as attivobilseg
			   ,tlbcidef::fatturbilseg as fatturbilseg
			   ,tlbcidef::ndg_collegato as ndg_collegato
			   ,tlbcidef::codicebanca_collegato as codicebanca_collegato
			   ,tlbcidef::cd_fiscale as cd_fiscale
			   ,tlbcidef::cd_fiscale as partita_iva
			   ,tlbungr::ndg_gruppo as ndg_gruppo
			   ,tlbcidef::util_cassa as totutilizzdatdef
			   ,tlbcidef::imp_accordato as totaccordatodatdef
;


tlbcidef_tlbungr_grp = GROUP tlbcidef_tlbungr BY (codicebanca, ndgprincipale, datainiziodef);
tlbcidef_tlbungr_tlbposi_sum = FOREACH  tlbcidef_tlbungr_grp
                               GENERATE 
										 group.codicebanca
										,group.ndgprincipale
										,group.datainiziodef
										,MAX(tlbcidef_tlbungr.datafinedef) as datafinedef
										,MAX(tlbcidef_tlbungr.datainiziopd) as datainiziopd
										,MAX(tlbcidef_tlbungr.datainizioristrutt) as datainizioristrutt
										,MAX(tlbcidef_tlbungr.datainizioinc) as datainizioinc
										,MAX(tlbcidef_tlbungr.datainiziosoff) as datainiziosoff
										,MAX(tlbcidef_tlbungr.c_key) as c_key
										,MAX(tlbcidef_tlbungr.tipo_segmne) as tipo_segmne
										,MAX(tlbcidef_tlbungr.sae_segm) as sae_segm
										,MAX(tlbcidef_tlbungr.rae_segm) as rae_segm
										,MAX(tlbcidef_tlbungr.segmento) as segmento
										,MAX(tlbcidef_tlbungr.tp_ndg) as tp_ndg
										,MAX(tlbcidef_tlbungr.provincia_segm) as provincia_segm
										,MAX(tlbcidef_tlbungr.databilseg) as databilseg
										,MAX(tlbcidef_tlbungr.strbilseg) as strbilseg
										,MAX(tlbcidef_tlbungr.attivobilseg) as attivobilseg
										,MAX(tlbcidef_tlbungr.fatturbilseg) as fatturbilseg
										,MAX(tlbcidef_tlbungr.ndg_gruppo) as ndg_gruppo
										,MAX(tlbcidef_tlbungr.cd_fiscale) as cd_fiscale
										,MAX(tlbcidef_tlbungr.partita_iva) as partita_iva
										,SUM(tlbcidef_tlbungr.totaccordatodatdef) as totaccordatodatdef
										,SUM(tlbcidef_tlbungr.totutilizzdatdef) as  totutilizzdatdef
                                   ;

  
tlbuact_load = LOAD '$tlbuact_path'
  	      USING PigStorage(';')
  	      AS (
				 cd_istituto        :chararray
				,ndg                :chararray
				,dt_riferimento     :int
				,cab_prov_com_stato :chararray
				,provincia          :chararray
				,sae                :chararray
				,rae                :chararray
				,ciae               :chararray
				,sportello          :chararray
				,area_affari        :chararray
				,tp_ndg             :chararray
				,intestazione       :chararray
				,partita_iva        :chararray
				,cd_fiscale         :chararray
				,ctp_cred_proc_conc :chararray
				,cliente_protestato :chararray
				,status_sygei       :chararray
				,ndg_caponucleo     :chararray
				,specie_giuridica   :chararray
				,dt_inizio_rischio  :chararray
				,dt_revoca_fidi     :chararray
				,dt_cens_anagrafe   :chararray
				,cab_luogo_nascita  :chararray
				,dt_costituz_nascit :chararray
				,tp_contr_basilea1  :chararray
				,flag_fallibile     :chararray
				,stato_controparte  :chararray
				,dt_estinz_ctp      :chararray
				,cliente_fido_rev   :chararray
				,tp_controparte     :chararray
				,grande_sett_attiv  :chararray
				,grande_ramo_attiv  :chararray
				,n058_interm_vigil  :chararray
				,n160_cod_ateco     :chararray		 
			  )	
           ;
           
tlbuact = FOREACH tlbuact_load
          GENERATE 
               dt_riferimento
              ,cd_istituto
              ,ndg 
              ,intestazione
              ,ciae
              ,n160_cod_ateco
          ;           
           
tlbcidef_tlbungr_tlbposi_tlbuact_join = JOIN tlbcidef_tlbungr_tlbposi_sum BY (datainiziodef, codicebanca, ndgprincipale) LEFT
                                            ,tlbuact BY (dt_riferimento, cd_istituto, ndg);

fposi_out = FOREACH tlbcidef_tlbungr_tlbposi_tlbuact_join 
            GENERATE
                 tlbcidef_tlbungr_tlbposi_sum::codicebanca as codicebanca
				,tlbcidef_tlbungr_tlbposi_sum::ndgprincipale as ndgprincipale
                ,tlbcidef_tlbungr_tlbposi_sum::datainiziodef as datainiziodef
                ,( tlbcidef_tlbungr_tlbposi_sum::datafinedef > '$data_a'?'99991231':tlbcidef_tlbungr_tlbposi_sum::datafinedef ) as datafinedef
				,( tlbcidef_tlbungr_tlbposi_sum::datainiziopd > '$data_a'?NULL:tlbcidef_tlbungr_tlbposi_sum::datainiziopd ) as datainiziopd
				,( tlbcidef_tlbungr_tlbposi_sum::datainizioinc > '$data_a'?NULL:tlbcidef_tlbungr_tlbposi_sum::datainizioinc ) as datainizioinc
				,( tlbcidef_tlbungr_tlbposi_sum::datainizioristrutt > '$data_a'?NULL:tlbcidef_tlbungr_tlbposi_sum::datainizioristrutt ) as datainizioristrutt
				,( tlbcidef_tlbungr_tlbposi_sum::datainiziosoff > '$data_a'?NULL:tlbcidef_tlbungr_tlbposi_sum::datainiziosoff ) as datainiziosoff
				,DoubleConverter(tlbcidef_tlbungr_tlbposi_sum::totaccordatodatdef) as totaccordatodatdef
				,DoubleConverter(tlbcidef_tlbungr_tlbposi_sum::totutilizzdatdef) as totutilizzdatdef
				,tlbcidef_tlbungr_tlbposi_sum::segmento as segmento
				,tlbcidef_tlbungr_tlbposi_sum::tp_ndg as naturagiuridica_segm
            ; 
            
fposi_out_dist = DISTINCT (
            FOREACH fposi_out 
            GENERATE
                 codicebanca
				,ndgprincipale
                ,datainiziodef
                ,datafinedef
				,datainiziopd
				,datainizioinc
				,datainizioristrutt
				,datainiziosoff
				,totaccordatodatdef
				,totutilizzdatdef
				,segmento
				,naturagiuridica_segm
		    )
            ; 
            
STORE fposi_out_dist  INTO '$fposi_outdir/$periodo' USING PigStorage(';');
