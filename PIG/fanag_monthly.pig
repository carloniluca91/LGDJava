/*
$LastChangedDate: 2016-10-28 14:49:31 +0200 (ven, 28 ott 2016) $
$Rev: 585 $		
$Author: arzelenko $	

Sono coinvolti i segunti parametri:
		-cicli_ndg_path
		-tlbuact_path
		-tlbudtc_path
		-fanag_outdir
		-periodo 
*/

register LeastDate.jar;

/*
Carico il file tmp_cicli_ndg
*/

cicli_ndg = LOAD '$cicli_ndg_path' 
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
--cicli_ndg = FILTER cicli_ndg1 BY SUBSTRING((chararray)datainiziodef,0,6) >= SUBSTRING('$periodo',0,6) AND SUBSTRING((chararray)datainiziodef,0,6) <= ( SUBSTRING('$periodo',7,13) is null ? SUBSTRING('$periodo',0,6) : SUBSTRING('$periodo',7,13) );

cicli_ndg_princ = FILTER cicli_ndg BY cd_collegamento IS NULL;

cicli_ndg_coll = FILTER cicli_ndg BY cd_collegamento IS NOT NULL;

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
			   ,tp_ndg
			   ,intestazione
			   ,cd_fiscale
			   ,partita_iva
			   ,sae
			   ,rae
			   ,ciae         
			   ,provincia 
			   ,sportello
			   ,ndg_caponucleo
              ;

/* 
join dei principali solo per ist e ndg per il filtro between successivo 
*/
tlbcidef_princ_tlbuact_join = JOIN  tlbuact BY (cd_istituto, ndg), cicli_ndg_princ BY (codicebanca_collegato, ndg_collegato);

/* 
join dei collegati per ist,ndg e data per prendere le anagrafiche per le sole date presenti in ucol 
*/
tlbcidef_coll_tlbuact_join = JOIN  tlbuact BY (cd_istituto, ndg), cicli_ndg_coll BY (codicebanca_collegato, ndg_collegato);

/* 
prende solo i PRINCIPALI in base alla presenza del codice collegamento 
per i quali il range dell'anagrafica deve essere tra inizio e fine ciclo 
*/
tlbcidef_princ_tlbuact_join_filt = FILTER tlbcidef_princ_tlbuact_join 
 BY ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')  
AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(ToString(AddDuration( ToDate( (chararray)LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'), $data_a ),'yyyyMMdd' ),'$numero_mesi_2' ),'yyyyMMdd'),0,6 )
;

tlbcidef_tlbuact_princ = FOREACH tlbcidef_princ_tlbuact_join_filt
                              GENERATE 
		                      cicli_ndg_princ::codicebanca_collegato as codicebanca_collegato
				 	 		 ,cicli_ndg_princ::ndg_collegato  as ndg_collegato     
							 ,cicli_ndg_princ::datainiziodef  as datainiziodef     
							 ,cicli_ndg_princ::datafinedef  as datafinedef     
							 ,tlbuact::dt_riferimento as datariferimento
							 ,tlbuact::tp_ndg as naturagiuridica 
							 ,tlbuact::intestazione as intestazione
							 ,tlbuact::cd_fiscale as codicefiscale 
							 ,tlbuact::partita_iva as partitaiva
							 ,tlbuact::sae as sae
							 ,tlbuact::rae as rae
							 ,tlbuact::ciae as ciae
							 ,cicli_ndg_princ::provincia_segm as  provincia
							 ,tlbuact::provincia as provincia_cod
							 ,tlbuact::sportello as  sportello
							 ,cicli_ndg_princ::segmento as segmento
							 ,cicli_ndg_princ::cd_collegamento as cd_collegamento
							 ,tlbuact::ndg_caponucleo as ndg_caponucleo
							 ,cicli_ndg_princ::codicebanca  as codicebanca
							 ,cicli_ndg_princ::ndgprincipale  as ndgprincipale
                            ;


/* 
filtro per range di anagrafica esteso anche sui collegati 
*/
tlbcidef_coll_tlbuact_join_filt = FILTER tlbcidef_coll_tlbuact_join 
 BY ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')  
AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(ToString(AddDuration( ToDate( (chararray)LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'), $data_a ),'yyyyMMdd' ),'$numero_mesi_2' ),'yyyyMMdd'),0,6 )
;

tlbcidef_tlbuact_coll = FOREACH tlbcidef_coll_tlbuact_join_filt
                              GENERATE 
		                      cicli_ndg_coll::codicebanca_collegato as codicebanca_collegato
				 	 		 ,cicli_ndg_coll::ndg_collegato  as ndg_collegato     
							 ,cicli_ndg_coll::datainiziodef  as datainiziodef     
							 ,cicli_ndg_coll::datafinedef  as datafinedef     
							 ,tlbuact::dt_riferimento as datariferimento
							 ,tlbuact::tp_ndg as naturagiuridica 
							 ,tlbuact::intestazione as intestazione
							 ,tlbuact::cd_fiscale as codicefiscale 
							 ,tlbuact::partita_iva as partitaiva
							 ,tlbuact::sae as sae
							 ,tlbuact::rae as rae
							 ,tlbuact::ciae as ciae
							 ,cicli_ndg_coll::provincia_segm as  provincia
							 ,tlbuact::provincia as provincia_cod
							 ,tlbuact::sportello as  sportello
							 ,cicli_ndg_coll::segmento as segmento
							 ,cicli_ndg_coll::cd_collegamento as cd_collegamento
							 ,tlbuact::ndg_caponucleo as ndg_caponucleo
							 ,cicli_ndg_coll::codicebanca  as codicebanca
							 ,cicli_ndg_coll::ndgprincipale  as ndgprincipale
                            ;

cicli_all = UNION tlbcidef_tlbuact_princ
                 ,tlbcidef_tlbuact_coll
                 ;

tlbcidef_tlbuact = DISTINCT ( FOREACH cicli_all
                              GENERATE 
		                      codicebanca_collegato
				 	 		 ,ndg_collegato     
							 ,datainiziodef     
							 ,datafinedef     
							 ,datariferimento
							 ,naturagiuridica 
							 ,intestazione
							 ,codicefiscale 
							 ,partitaiva
							 ,sae
							 ,rae
							 ,ciae
							 ,provincia
							 ,provincia_cod
							 ,sportello
							 ,segmento
							 ,cd_collegamento
							 ,ndg_caponucleo
							 ,codicebanca
							 ,ndgprincipale)
							 ;

tlbudtc = LOAD '$tlbudtc_path'
  	      USING PigStorage(';')
  	      AS
  	        (
             cd_istituto         : chararray
			,ndg                 : chararray
			,dt_riferimento      : int
			,reddito             : chararray
			,totale_utilizzi     : chararray
			,totale_fidi_delib   : chararray
			,totale_accordato    : chararray
			,patrimonio          : chararray
			,n_dipendenti        : chararray
			,tot_rischi_indir    : chararray
			,status_ndg          : chararray
			,status_basilea2     : chararray
			,posiz_soff_inc      : chararray
			,dubb_esito_inc_ndg  : chararray
			,status_cliente_lab  : chararray
			,tot_util_mortgage   : chararray
			,tot_acco_mortgage   : chararray
			,dt_entrata_default  : chararray
			,cd_stato_def_t0     : chararray
			,cd_tipo_def_t0      : chararray
			,cd_stato_def_a_t12  : chararray
			,cd_rap_ristr        : chararray
			,tp_ristrutt         : chararray
			,tp_cli_scad_scf     : chararray
			,tp_cli_scad_scf_b2  : chararray
			,totale_saldi_0063   : chararray
			,totale_saldi_0260   : chararray
		 );	

tlbcidef_tlbuact_join = JOIN  tlbudtc BY (cd_istituto, ndg, dt_riferimento), tlbcidef_tlbuact BY (codicebanca_collegato,ndg_collegato, datariferimento) ;

fanag_out = FOREACH tlbcidef_tlbuact_join
            GENERATE
                     tlbcidef_tlbuact::codicebanca_collegato as codicebanca
					,tlbcidef_tlbuact::ndg_collegato  as ndg
					,tlbcidef_tlbuact::datariferimento as datariferimento
					,tlbudtc::totale_accordato as totaccordato
					,tlbudtc::totale_utilizzi as totutilizzo
					,tlbudtc::tot_acco_mortgage as totaccomortgage
					,tlbudtc::tot_util_mortgage as totutilmortgage
					,tlbudtc::totale_saldi_0063 as totsaldi0063
					,tlbudtc::totale_saldi_0260 as totsaldi0260
					,tlbcidef_tlbuact::naturagiuridica as naturagiuridica 
					,tlbcidef_tlbuact::intestazione as intestazione
					,tlbcidef_tlbuact::codicefiscale as codicefiscale 
					,tlbcidef_tlbuact::partitaiva as partitaiva
					,tlbcidef_tlbuact::sae as sae
					,tlbcidef_tlbuact::rae as rae
					,tlbcidef_tlbuact::ciae as ciae
					,tlbcidef_tlbuact::provincia as  provincia
					,tlbcidef_tlbuact::provincia_cod as provincia_cod
					,tlbcidef_tlbuact::sportello as  sportello
					,tlbudtc::posiz_soff_inc as attrn011
					,tlbudtc::status_ndg as attrn175
					,tlbudtc::tp_cli_scad_scf as attrn186
					,tlbudtc::cd_rap_ristr as cdrapristr 
					,tlbcidef_tlbuact::segmento as segmento
					,tlbcidef_tlbuact::cd_collegamento as cd_collegamento
					,tlbcidef_tlbuact::ndg_caponucleo as ndg_caponucleo
					,(tlbudtc::tp_ristrutt != '0' ? 'S' : 'N') as flag_ristrutt
					,tlbcidef_tlbuact::codicebanca    as codicebanca_princ
					,tlbcidef_tlbuact::ndgprincipale  as ndgprincipale
					,tlbcidef_tlbuact::datainiziodef  as datainiziodef
            ;

STORE fanag_out  INTO '$fanag_outdir/$periodo' USING PigStorage(';');
