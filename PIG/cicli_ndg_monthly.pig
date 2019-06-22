/*
$LastChangedDate: 2017-04-14 15:13:05 +0200 (ven, 14 apr 2017) $
$Rev: 629 $		
$Author: arzelenko $	

Sono coinvolti i seguenti parametri:
		-TLBCICLILAV_PATH
		-TLBCOMP_PATH
		-TLBAGGR_PATH
		-CONF_DIR
		-FILTRO_SEGMENTO

		-CICLI_NDG_PATH
		-PERIODO

*/

register FiltroSegmento.jar;
/*
Carico il file ciclilav
Nel file, se non esiste un ndg migrato, i campi cd_isti_ced e ndg_ced sono valorizzati come cd_isti e ndg_principale.
In questo modo posso fare la join con la tlbcomp usando i campi "_ced"
*/
ciclilav1 = LOAD '$tlbciclilav_path' 
      USING PigStorage(';') 
      AS (
           cd_isti              : chararray
          ,ndg_principale       : chararray
          ,dt_inizio_ciclo      : int
          ,dt_fine_ciclo        : int
          ,datainiziopd         : int
          ,datainizioristrutt   : int
          ,datainizioinc        : int
          ,datainiziosoff       : int
          ,progr                : int
          ,cd_isti_coll         : chararray
          ,ndg_coll             : chararray
          ,dt_riferimento_udct  : chararray
          ,dt_riferimento_urda  : int
		  ,util_cassa           : chararray
		  ,imp_accordato        : chararray
		  ,tot_add_sosp         : chararray
          ,cd_collegamento      : chararray
          ,status_ndg           : chararray
	      ,posiz_soff_inc       : chararray
	      ,cd_rap_ristr         : chararray
	      ,tp_cli_scad_scf      : chararray
		);

/* filtro per un mese. usare solo per processi con ciclo d'elaborazione mensile */
--ciclilav = FILTER ciclilav1 BY SUBSTRING((chararray)dt_inizio_ciclo,0,6) == '$periodo';

/* filtro per range di mesi per leggere solo i dati della finestra temporale del periodo di ciclo corrente */
ciclilav = FILTER ciclilav1 BY SUBSTRING((chararray)dt_inizio_ciclo,0,6) >= SUBSTRING('$periodo',0,6) AND SUBSTRING((chararray)dt_inizio_ciclo,0,6) <= ( SUBSTRING('$periodo',7,13) is null ? SUBSTRING('$periodo',0,6) : SUBSTRING('$periodo',7,13) );

/*
Carico la tblcomp
*/
tblcomp =  LOAD '$tlbcomp_path' 
           		USING PigStorage(';') 
           		AS (
           		    dt_riferimento  : int
					,c_key          : chararray
					,tipo_segmne    : chararray
					,cd_istituto    : chararray
					,ndg            : chararray
				   );

/*
Si accede alla tabella TLBCOMP per indice univoco: 
dt_riferimento = dt_inizio_ciclo
cd_istituto = cd_isti_ced
ndg = ndg_ced

N.B. per ogni record originario della ciclilav qua avrò tutti gli ndg migrati che ho trovato sulla MIGW.
*/				   

tlbcidef_tblcomp_join = JOIN ciclilav BY ( cd_isti_coll, ndg_coll, dt_inizio_ciclo ) 
                            ,tblcomp  BY ( cd_istituto, ndg, dt_riferimento );		   				   
tlbcidef_tblcomp_1 = FOREACH tlbcidef_tblcomp_join 
                     GENERATE 
                            ciclilav::cd_isti             as cd_isti
                           ,ciclilav::ndg_principale      as ndg_principale
                           ,ciclilav::dt_inizio_ciclo     as dt_inizio_ciclo
                           ,ciclilav::progr               as progr
                           ,ciclilav::cd_isti_coll        as cd_isti_coll
						   ,ciclilav::ndg_coll            as ndg_coll
						   ,tblcomp::c_key                as c_key
                           ,tblcomp::tipo_segmne          as tipo_segmne
                           ;
                           
--STORE tlbcidef_tblcomp_1  INTO '/user/drlgd/datarequest/dt/tmp/debug/tlbcidef_tblcomp_1' USING PigStorage(';');

/* Estraggo il minimo progressivo che ho trovato sulla tlbcomp */                           
tlbcidef_tblcomp_group = GROUP tlbcidef_tblcomp_1 BY (cd_isti, dt_inizio_ciclo, ndg_principale);

tlbcidef_tblcomp_min_ndg = FOREACH tlbcidef_tblcomp_group 
                                {
                                 sorted = ORDER tlbcidef_tblcomp_1 by progr ASC, c_key ASC, tipo_segmne ASC;
						         top    = LIMIT sorted 1;
						         generate group, FLATTEN(top);
								};

tlbcidef_tblcomp= FOREACH  tlbcidef_tblcomp_min_ndg
                  GENERATE 	
                       group.cd_isti   		    as cd_isti 			
                      ,group.ndg_principale     as ndg_principale
                      ,group.dt_inizio_ciclo    as dt_inizio_ciclo
	                  ,top::c_key               as c_key
	                  ,top::tipo_segmne         as tipo_segmne
	                  ,top::cd_isti_coll        as cd_isti_coll
					  ,top::ndg_coll            as ndg_coll
                     ;      

/*
Carico il file tlbaggr
*/
tlbaggr_load = LOAD  '$tlbaggr_path'  
          USING PigStorage(';')
          AS (
                 dt_riferimento    : int
				,c_key_aggr        : chararray
				,ndg_gruppo        : chararray
				,cod_fiscale       : chararray
				,tipo_segmne_aggr  : chararray
				,segmento          : chararray
				,tipo_motore       : chararray
				,cd_istituto       : chararray
				,ndg               : chararray
				,rae               : chararray
				,sae               : chararray
				,tp_ndg            : chararray
				,prov_segm         : chararray
				,fonte_segmento    : chararray
				,utilizzo_cr       : chararray
				,accordato_cr      : chararray
				,databil           : chararray
				,strutbil          : chararray
				,fatturbil         : chararray
				,attivobil         : chararray
				,codimp_cebi       : chararray
				,tot_acco_agr      : chararray
				,tot_util_agr      : chararray
				,n058_int_vig      : chararray
             );

tlbaggr = FOREACH  tlbaggr_load
          GENERATE 
                dt_riferimento
               ,c_key_aggr
               ,cd_istituto
               ,tipo_segmne_aggr                          
 			   ,sae as sae_segm
			   ,rae as rae_segm
			   ,segmento as segmento
			   ,tp_ndg	 as tp_ndg
			   ,prov_segm as provincia_segm
			   ,databil as databilseg
			   ,strutbil as strbilseg
			   ,attivobil as attivobilseg
			   ,fatturbil as fatturbilseg
			   ,cod_fiscale as cod_fiscale    
			  ;          

/* Carico il dominio del segmento */
domini_segmento = LOAD  '$conf_dir/DominiSegmento.csv' USING PigStorage(';') 
                  AS (
                        segmento: chararray
                       ,segmento_desc: chararray
                     );   
tlbaggr_segmento_dominio_join =  JOIN tlbaggr BY segmento, domini_segmento BY segmento using 'replicated';

/* Filtro segmento */
tlbaggr_filtered = FILTER tlbaggr_segmento_dominio_join BY FiltroSegmento(domini_segmento::segmento_desc, tlbaggr::tp_ndg,'$filtro_segmento');   
             
             
/*
Join con il risultato della join precedente per 
dt_riferimento = dt_inizio_ciclo
c_key = c_key da tlbcomp
tipo_segmne = tipo_segmne 
*/
tblcomp_tblaggr_join = JOIN  tlbcidef_tblcomp BY (dt_inizio_ciclo,c_key,tipo_segmne) 
                            ,tlbaggr_filtered BY (dt_riferimento,c_key_aggr,tipo_segmne_aggr );
tblcomp_tblaggr = FOREACH tblcomp_tblaggr_join
                           GENERATE tlbcidef_tblcomp::cd_isti                        as cd_isti
						           ,tlbcidef_tblcomp::ndg_principale                 as ndg_principale
								   ,tlbcidef_tblcomp::dt_inizio_ciclo                as dt_inizio_ciclo
								   ,tlbcidef_tblcomp::tipo_segmne                    as tipo_segmne
								   ,tlbcidef_tblcomp::c_key                          as c_key
								   ,tlbcidef_tblcomp::cd_isti_coll                   as cd_isti_coll
								   ,tlbcidef_tblcomp::ndg_coll                       as ndg_coll
								   ,tlbaggr_filtered::tlbaggr::sae_segm              as sae_segm
								   ,tlbaggr_filtered::tlbaggr::rae_segm              as rae_segm
								   ,tlbaggr_filtered::tlbaggr::segmento              as segmento
								   ,tlbaggr_filtered::tlbaggr::tp_ndg	             as tp_ndg
								   ,tlbaggr_filtered::tlbaggr::provincia_segm        as provincia_segm
								   ,tlbaggr_filtered::tlbaggr::databilseg            as databilseg
								   ,tlbaggr_filtered::tlbaggr::strbilseg             as strbilseg
								   ,tlbaggr_filtered::tlbaggr::attivobilseg          as attivobilseg
								   ,tlbaggr_filtered::tlbaggr::fatturbilseg          as fatturbilseg
								   ,tlbaggr_filtered::tlbaggr::cod_fiscale           as cod_fiscale
								   ,tlbaggr_filtered::domini_segmento::segmento_desc as segmento_desc
								   ;

/* distinct per successiva ricerca degli ndg principali con segmenti diversi */
/*
tblcomp_tblaggr_listasegm_dist = DISTINCT (
                                 FOREACH tblcomp_tblaggr 
		                         GENERATE 
		                          cd_isti 
                                 ,ndg_principale 
                                 ,dt_inizio_ciclo
                                 ,segmento
		                       );
*/
--STORE tblcomp_tblaggr_listasegm_dist  INTO '/app/drlgd/datarequest/dt/tmp/debug/tblcomp_tblaggr_listasegm_dist' USING PigStorage(';');
/*
tblcomp_tblaggr_listasegm_group = GROUP tblcomp_tblaggr_listasegm_dist BY (cd_isti, ndg_principale, dt_inizio_ciclo);

tblcomp_tblaggr_listasegm = FOREACH tblcomp_tblaggr_listasegm_group 
                            GENERATE 
                             group.cd_isti         as cd_isti
                            ,group.ndg_principale  as ndg_principale
                            ,group.dt_inizio_ciclo as dt_inizio_ciclo
                            ,COUNT(tblcomp_tblaggr_listasegm_dist) as num_recs
;
*/
--STORE tblcomp_tblaggr_listasegm  INTO '/app/drlgd/datarequest/dt/tmp/debug/tblcomp_tblaggr_listasegm' USING PigStorage(';');

--tblcomp_tblaggr_listasegm_filter = FILTER tblcomp_tblaggr_listasegm BY num_recs > 1;

--STORE tblcomp_tblaggr_listasegm_filter  INTO '/app/drlgd/datarequest/dt/tmp/debug/tblcomp_tblaggr_listasegm_filter' USING PigStorage(';');

--------------------------------------------------------------------------------------------------------------------------
--scrittura file con chiavi aventi + di uno segmenti differenti
/*
out_tblcomp_tblaggr_listasegm_filter_join = JOIN tblcomp_tblaggr_listasegm_filter  BY (cd_isti, ndg_principale,dt_inizio_ciclo)
                                                ,tblcomp_tblaggr                   BY (cd_isti, ndg_principale,dt_inizio_ciclo); 
                                                
out_tblcomp_tblaggr_listasegm_filter = FOREACH out_tblcomp_tblaggr_listasegm_filter_join 
			                            GENERATE 
			                             tblcomp_tblaggr_listasegm_filter::cd_isti         as cd_isti
			                            ,tblcomp_tblaggr_listasegm_filter::ndg_principale  as ndg_principale
			                            ,tblcomp_tblaggr_listasegm_filter::dt_inizio_ciclo as dt_inizio_ciclo
			                            ,tblcomp_tblaggr::tipo_segmne                      as tipo_segmne
								  	    ,tblcomp_tblaggr::c_key                            as c_key
									    ,tblcomp_tblaggr::cd_isti_coll                     as cd_isti_coll
									    ,tblcomp_tblaggr::ndg_coll                         as ndg_coll
									    ,tblcomp_tblaggr::sae_segm                         as sae_segm
									    ,tblcomp_tblaggr::rae_segm                         as rae_segm
									    ,tblcomp_tblaggr::segmento                         as segmento
									    ,tblcomp_tblaggr::tp_ndg	                       as tp_ndg
									    ,tblcomp_tblaggr::provincia_segm                   as provincia_segm
									    ,tblcomp_tblaggr::databilseg                       as databilseg
									    ,tblcomp_tblaggr::strbilseg                        as strbilseg
									    ,tblcomp_tblaggr::attivobilseg                     as attivobilseg
									    ,tblcomp_tblaggr::fatturbilseg                     as fatturbilseg
									    ,tblcomp_tblaggr::cod_fiscale                      as cod_fiscale
									    ,tblcomp_tblaggr::segmento_desc                    as segmento_desc
;

STORE out_tblcomp_tblaggr_listasegm_filter  INTO '$listasegm_doppi/$periodo' USING PigStorage(';');
*/
-------------------------------------------------------------------------------------------------------------------------- 
--STORE tblcomp_tblaggr  INTO '/user/drlgd/datarequest/dt/tmp/debug/tblcomp_tblaggr' USING PigStorage(';');
								   
/* Rileggo la ciclilav per riesplodere i cicli su tutti i migrati */                     
tlbcidef_tblcomp_tblaggr_join = JOIN ciclilav        BY (cd_isti, ndg_principale,dt_inizio_ciclo) LEFT
                                    ,tblcomp_tblaggr BY (cd_isti, ndg_principale,dt_inizio_ciclo);          						   
tlbcidef_tblcomp_tblaggr = FOREACH tlbcidef_tblcomp_tblaggr_join
                           GENERATE 
                                    ciclilav::cd_isti                 as cd_isti
						           ,ciclilav::ndg_principale          as ndg_principale
								   ,ciclilav::dt_inizio_ciclo         as dt_inizio_ciclo
								   ,ciclilav::dt_fine_ciclo           as dt_fine_ciclo
								   ,ciclilav::datainiziopd            as datainiziopd
								   ,ciclilav::datainizioristrutt      as datainizioristrutt
								   ,ciclilav::datainizioinc           as datainizioinc
								   ,ciclilav::datainiziosoff          as datainiziosoff
                           		   ,ciclilav::cd_isti_coll            as cd_isti_coll
						   		   ,ciclilav::ndg_coll                as ndg_coll
						   		   ,ciclilav::dt_riferimento_udct     as dt_riferimento_udct
						   		   ,ciclilav::dt_riferimento_urda     as dt_riferimento_urda
						   		   ,(double)( ciclilav::tot_add_sosp  is null? '0' : ciclilav::tot_add_sosp  ) as tot_add_sosp
						   		   ,(double)( ciclilav::util_cassa    is null? '0' : ciclilav::util_cassa    ) as util_cassa
						   		   ,(double)( ciclilav::imp_accordato is null? '0' : ciclilav::imp_accordato ) as imp_accordato
						   		   ,ciclilav::cd_collegamento         as cd_collegamento
								   ,tblcomp_tblaggr::tipo_segmne      as tipo_segmne
								   ,tblcomp_tblaggr::c_key            as c_key
								   ,tblcomp_tblaggr::sae_segm         as sae_segm
								   ,tblcomp_tblaggr::rae_segm         as rae_segm
								   ,(tblcomp_tblaggr::c_key is null? 'NF' : tblcomp_tblaggr::segmento) as segmento
								   --,tblcomp_tblaggr::segmento         as segmento
								   ,tblcomp_tblaggr::tp_ndg	          as tp_ndg
								   ,tblcomp_tblaggr::provincia_segm   as provincia_segm
								   ,tblcomp_tblaggr::databilseg       as databilseg
								   ,tblcomp_tblaggr::strbilseg        as strbilseg
								   ,tblcomp_tblaggr::attivobilseg     as attivobilseg
								   ,tblcomp_tblaggr::fatturbilseg     as fatturbilseg
								   ,tblcomp_tblaggr::cod_fiscale      as cod_fiscale
                                   ,( tblcomp_tblaggr::segmento_desc is null? 'X' : tblcomp_tblaggr::segmento_desc ) as segmento_desc 
                                   ,ciclilav::status_ndg              as status_ndg
							       ,ciclilav::posiz_soff_inc          as posiz_soff_inc
							       ,ciclilav::cd_rap_ristr            as cd_rap_ristr
							       ,ciclilav::tp_cli_scad_scf         as tp_cli_scad_scf
                               ;
                               
/* Rileggo la generate precedente per assegnare il segmento 99 per cicli con piu' c_key differenti */
/*
tlbcidef_tblcomp_tblaggr_join = JOIN tlbcidef_tblcomp_tblaggr_pre     BY (cd_isti, ndg_principale,dt_inizio_ciclo) LEFT
                                    ,tblcomp_tblaggr_listasegm_filter BY (cd_isti, ndg_principale,dt_inizio_ciclo);          						   
tlbcidef_tblcomp_tblaggr = FOREACH tlbcidef_tblcomp_tblaggr_join
                           GENERATE 
                                    tlbcidef_tblcomp_tblaggr_pre::cd_isti                 as cd_isti
						           ,tlbcidef_tblcomp_tblaggr_pre::ndg_principale          as ndg_principale
								   ,tlbcidef_tblcomp_tblaggr_pre::dt_inizio_ciclo         as dt_inizio_ciclo
								   ,tlbcidef_tblcomp_tblaggr_pre::dt_fine_ciclo           as dt_fine_ciclo
								   ,tlbcidef_tblcomp_tblaggr_pre::datainiziopd            as datainiziopd
								   ,tlbcidef_tblcomp_tblaggr_pre::datainizioristrutt      as datainizioristrutt
								   ,tlbcidef_tblcomp_tblaggr_pre::datainizioinc           as datainizioinc
								   ,tlbcidef_tblcomp_tblaggr_pre::datainiziosoff          as datainiziosoff
                           		   ,tlbcidef_tblcomp_tblaggr_pre::cd_isti_coll            as cd_isti_coll
						   		   ,tlbcidef_tblcomp_tblaggr_pre::ndg_coll                as ndg_coll
						   		   ,tlbcidef_tblcomp_tblaggr_pre::dt_riferimento_udct     as dt_riferimento_udct
						   		   ,tlbcidef_tblcomp_tblaggr_pre::dt_riferimento_urda     as dt_riferimento_urda
						   		   ,tlbcidef_tblcomp_tblaggr_pre::tot_add_sosp            as tot_add_sosp
						   		   ,tlbcidef_tblcomp_tblaggr_pre::util_cassa              as util_cassa
						   		   ,tlbcidef_tblcomp_tblaggr_pre::imp_accordato           as imp_accordato
						   		   ,tlbcidef_tblcomp_tblaggr_pre::cd_collegamento         as cd_collegamento
								   ,tlbcidef_tblcomp_tblaggr_pre::tipo_segmne             as tipo_segmne
								   ,tlbcidef_tblcomp_tblaggr_pre::c_key                   as c_key
								   ,tlbcidef_tblcomp_tblaggr_pre::sae_segm                as sae_segm
								   ,tlbcidef_tblcomp_tblaggr_pre::rae_segm                as rae_segm
								   ,(tblcomp_tblaggr_listasegm_filter::cd_isti is null? tlbcidef_tblcomp_tblaggr_pre::segmento : '99') as segmento
								   ,tlbcidef_tblcomp_tblaggr_pre::tp_ndg	       as tp_ndg
								   ,tlbcidef_tblcomp_tblaggr_pre::provincia_segm   as provincia_segm
								   ,tlbcidef_tblcomp_tblaggr_pre::databilseg       as databilseg
								   ,tlbcidef_tblcomp_tblaggr_pre::strbilseg        as strbilseg
								   ,tlbcidef_tblcomp_tblaggr_pre::attivobilseg     as attivobilseg
								   ,tlbcidef_tblcomp_tblaggr_pre::fatturbilseg     as fatturbilseg
								   ,tlbcidef_tblcomp_tblaggr_pre::cod_fiscale      as cod_fiscale
                                   ,tlbcidef_tblcomp_tblaggr_pre::segmento_desc    as segmento_desc 
                               ;
*/
--STORE tlbcidef_tblcomp_tblaggr  INTO '/user/drlgd/datarequest/dt/tmp/debug/tlbcidef_tblcomp_tblaggr' USING PigStorage(';');

/* filtro i soggetti per soglia d'importo */		  
cicli_filtro_soglia = FILTER tlbcidef_tblcomp_tblaggr 
BY (util_cassa + tot_add_sosp) >= (double)(segmento_desc=='IMPRESE'?'$soglia_past_due_imprese':(segmento_desc=='PRIVATI'?'$soglia_past_due_privati':'0.00'))
AND (int)dt_riferimento_udct >= (int)dt_inizio_ciclo
AND SUBSTRING( (chararray)dt_riferimento_udct,0,6 ) <= SUBSTRING( (chararray)LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)dt_fine_ciclo,'yyyyMMdd' ),'P1M'),'yyyyMMdd'), $data_a ),0,6 )
;


/* otteniamo la lista dei cicli dove almeno un soggetto ha superato la soglia */
cicli_soglia_lista = DISTINCT (FOREACH cicli_filtro_soglia 
		                        GENERATE 
		                          cd_isti 
                                 ,ndg_principale 
                                 ,dt_inizio_ciclo 
		                       ); 

--STORE cicli_soglia_lista  INTO '/user/drlgd/datarequest/dt/tmp/debug/cicli_soglia_lista' USING PigStorage(';');

/* filtriamo la lista completa con il file-lista ottenuto dallo step precedente
   per prendere solo i cicli completi dove almeno un soggetto ha superato la soglia 
*/
tlbcidef_tblcomp_tblaggr_lista_join = JOIN tlbcidef_tblcomp_tblaggr BY ( cd_isti, ndg_principale, dt_inizio_ciclo ) 
                                          ,cicli_soglia_lista       BY ( cd_isti, ndg_principale, dt_inizio_ciclo );

tlbcidef_tblcomp_tblaggr_lista = FOREACH tlbcidef_tblcomp_tblaggr_lista_join
                           GENERATE 
                                    tlbcidef_tblcomp_tblaggr::cd_isti              as cd_isti
						           ,tlbcidef_tblcomp_tblaggr::ndg_principale       as ndg_principale
								   ,tlbcidef_tblcomp_tblaggr::dt_inizio_ciclo      as dt_inizio_ciclo
								   ,tlbcidef_tblcomp_tblaggr::dt_fine_ciclo        as dt_fine_ciclo
								   ,tlbcidef_tblcomp_tblaggr::datainiziopd         as datainiziopd
								   ,tlbcidef_tblcomp_tblaggr::datainizioristrutt   as datainizioristrutt
								   ,tlbcidef_tblcomp_tblaggr::datainizioinc        as datainizioinc
								   ,tlbcidef_tblcomp_tblaggr::datainiziosoff       as datainiziosoff
								   ,tlbcidef_tblcomp_tblaggr::c_key                as c_key
								   ,tlbcidef_tblcomp_tblaggr::tipo_segmne          as tipo_segmne
								   ,tlbcidef_tblcomp_tblaggr::sae_segm             as sae_segm
								   ,tlbcidef_tblcomp_tblaggr::rae_segm             as rae_segm
								   ,tlbcidef_tblcomp_tblaggr::segmento             as segmento
								   ,tlbcidef_tblcomp_tblaggr::tp_ndg	              as tp_ndg
								   ,tlbcidef_tblcomp_tblaggr::provincia_segm       as provincia_segm
								   ,tlbcidef_tblcomp_tblaggr::databilseg           as databilseg
								   ,tlbcidef_tblcomp_tblaggr::strbilseg            as strbilseg
								   ,tlbcidef_tblcomp_tblaggr::attivobilseg         as attivobilseg
								   ,tlbcidef_tblcomp_tblaggr::fatturbilseg         as fatturbilseg
								   ,tlbcidef_tblcomp_tblaggr::ndg_coll             as ndg_coll
								   ,tlbcidef_tblcomp_tblaggr::cd_isti_coll         as cd_isti_coll
								   ,tlbcidef_tblcomp_tblaggr::cd_collegamento      as cd_collegamento
								   ,tlbcidef_tblcomp_tblaggr::cod_fiscale          as cod_fiscale
								   ,tlbcidef_tblcomp_tblaggr::dt_riferimento_udct  as dt_riferimento_udct
						   		   ,tlbcidef_tblcomp_tblaggr::dt_riferimento_urda  as dt_riferimento_urda
						   		   ,tlbcidef_tblcomp_tblaggr::util_cassa           as util_cassa
						   		   ,tlbcidef_tblcomp_tblaggr::imp_accordato        as imp_accordato
						   		   ,tlbcidef_tblcomp_tblaggr::status_ndg              as status_ndg
							       ,tlbcidef_tblcomp_tblaggr::posiz_soff_inc          as posiz_soff_inc
							       ,tlbcidef_tblcomp_tblaggr::cd_rap_ristr            as cd_rap_ristr
							       ,tlbcidef_tblcomp_tblaggr::tp_cli_scad_scf         as tp_cli_scad_scf
                               ;
                               
--STORE tlbcidef_tblcomp_tblaggr_lista  INTO '/user/drlgd/datarequest/dt/tmp/debug/tlbcidef_tblcomp_tblaggr_lista' USING PigStorage(';');

/* filtro i soggetti per soglia d'importo */		  
tlbcidef_tblcomp_tblaggr_lista_filter = FILTER tlbcidef_tblcomp_tblaggr_lista
BY (int)dt_riferimento_urda >= (int)dt_inizio_ciclo
AND SUBSTRING( (chararray)dt_riferimento_urda,0,6 ) <= SUBSTRING( (chararray)LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)dt_fine_ciclo,'yyyyMMdd' ),'P1M'),'yyyyMMdd'), $data_a ),0,6 )
;

tlbcidef_tblcomp_tblaggr_lista_join_group = GROUP tlbcidef_tblcomp_tblaggr_lista_filter 
  BY (cd_isti, ndg_principale, dt_inizio_ciclo);

cicli_minposi_lista = FOREACH tlbcidef_tblcomp_tblaggr_lista_join_group
                      GENERATE 
                      group.cd_isti              as cd_isti
		             ,group.ndg_principale       as ndg_principale
				     ,group.dt_inizio_ciclo      as dt_inizio_ciclo
		   		     ,MIN( tlbcidef_tblcomp_tblaggr_lista_filter.dt_riferimento_urda ) as dt_riferimento_urda
               ;

tlbcidef_tblcomp_tblaggr_minposi_join = JOIN tlbcidef_tblcomp_tblaggr_lista BY ( cd_isti, ndg_principale, dt_inizio_ciclo ) LEFT
                                            ,cicli_minposi_lista            BY ( cd_isti, ndg_principale, dt_inizio_ciclo );


/* genera modello del file finale prima di applicare la logica di valorizzazione delle date */
cicli_filtrati = FOREACH tlbcidef_tblcomp_tblaggr_minposi_join
                           GENERATE 
                                    tlbcidef_tblcomp_tblaggr_lista::cd_isti              as cd_isti
						           ,tlbcidef_tblcomp_tblaggr_lista::ndg_principale       as ndg_principale
								   ,tlbcidef_tblcomp_tblaggr_lista::dt_inizio_ciclo      as dt_inizio_ciclo
								   ,tlbcidef_tblcomp_tblaggr_lista::dt_fine_ciclo        as dt_fine_ciclo
								   ,tlbcidef_tblcomp_tblaggr_lista::datainiziopd         as datainiziopd
								   ,tlbcidef_tblcomp_tblaggr_lista::datainizioristrutt   as datainizioristrutt
								   ,tlbcidef_tblcomp_tblaggr_lista::datainizioinc        as datainizioinc
								   ,tlbcidef_tblcomp_tblaggr_lista::datainiziosoff       as datainiziosoff
								   ,tlbcidef_tblcomp_tblaggr_lista::c_key                as c_key
								   ,tlbcidef_tblcomp_tblaggr_lista::tipo_segmne          as tipo_segmne
								   ,tlbcidef_tblcomp_tblaggr_lista::sae_segm             as sae_segm
								   ,tlbcidef_tblcomp_tblaggr_lista::rae_segm             as rae_segm
								   ,tlbcidef_tblcomp_tblaggr_lista::segmento             as segmento
								   ,tlbcidef_tblcomp_tblaggr_lista::tp_ndg	             as tp_ndg
								   ,tlbcidef_tblcomp_tblaggr_lista::provincia_segm       as provincia_segm
								   ,tlbcidef_tblcomp_tblaggr_lista::databilseg           as databilseg
								   ,tlbcidef_tblcomp_tblaggr_lista::strbilseg            as strbilseg
								   ,tlbcidef_tblcomp_tblaggr_lista::attivobilseg         as attivobilseg
								   ,tlbcidef_tblcomp_tblaggr_lista::fatturbilseg         as fatturbilseg
								   ,tlbcidef_tblcomp_tblaggr_lista::ndg_coll             as ndg_coll
								   ,tlbcidef_tblcomp_tblaggr_lista::cd_isti_coll         as cd_isti_coll
								   ,tlbcidef_tblcomp_tblaggr_lista::cd_collegamento      as cd_collegamento
								   ,tlbcidef_tblcomp_tblaggr_lista::cod_fiscale          as cod_fiscale
								   ,tlbcidef_tblcomp_tblaggr_lista::dt_riferimento_udct  as dt_riferimento_udct
						   		   ,tlbcidef_tblcomp_tblaggr_lista::util_cassa           as util_cassa
						   		   ,tlbcidef_tblcomp_tblaggr_lista::imp_accordato        as imp_accordato
						   		   ,cicli_minposi_lista::dt_riferimento_urda             as dt_riferimento_urda
						   		   ,tlbcidef_tblcomp_tblaggr_lista::status_ndg              as status_ndg
							       ,tlbcidef_tblcomp_tblaggr_lista::posiz_soff_inc          as posiz_soff_inc
							       ,tlbcidef_tblcomp_tblaggr_lista::cd_rap_ristr            as cd_rap_ristr
							       ,tlbcidef_tblcomp_tblaggr_lista::tp_cli_scad_scf         as tp_cli_scad_scf
                               ;
                               
--STORE cicli_filtrati  INTO '/user/drlgd/datarequest/dt/tmp/debug/cicli_filtrati' USING PigStorage(';');

---------------------------------------------------------------------------------------------------------------------------

/*
1.	Record con solo DATAINIZIOPD, senza nessuna altra data impostata e con data soglia valorizzata. 
Se la data soglia non è valorizzata scarto il record.
Se è valorizzata, si verifica che sommando ad essa il n° di mesi del filtro 7 non si raggiunga la data di fine ciclo
se la soglia=zero allora non scrive nulla
*/

cicli_filtrati_1 = FILTER cicli_filtrati 
                   BY  datainiziopd        is not null 
                   AND datainizioristrutt  is null
                   AND datainizioinc       is null
                   AND datainiziosoff      is null
                   AND dt_riferimento_urda is not null
                   AND AddDuration(ToDate((chararray)dt_riferimento_urda,'yyyyMMdd'),'$mesi_permanenzapd') 
                       < ToDate((chararray)dt_fine_ciclo,'yyyyMMdd')
                   ;
                      
/* 
Se il controllo viene superato allora si modificano le date di DATAINIZIODEF e DATAINIZIOPD impostandole con la data di superamento della soglia.
Le altre date sono nulle.
*/
cicli_filtrati_1_a = FOREACH  cicli_filtrati_1 
                                GENERATE
                                    cd_isti              as cd_isti
						           ,ndg_principale       as ndg_principale
								   ,dt_riferimento_urda  as dt_inizio_ciclo
								   ,dt_fine_ciclo        as dt_fine_ciclo
								   ,dt_riferimento_urda  as datainiziopd
								   ,datainizioristrutt   as datainizioristrutt
								   ,datainizioinc        as datainizioinc
								   ,datainiziosoff       as datainiziosoff
                           		   ,c_key                as c_key
                           		   ,tipo_segmne          as tipo_segmne								   
								   ,sae_segm             as sae_segm
								   ,rae_segm             as rae_segm
								   ,segmento             as segmento
								   ,tp_ndg	             as tp_ndg
								   ,provincia_segm       as provincia_segm
								   ,databilseg           as databilseg
								   ,strbilseg            as strbilseg
								   ,attivobilseg         as attivobilseg
								   ,fatturbilseg         as fatturbilseg
								   ,ndg_coll             as ndg_coll
								   ,cd_isti_coll         as cd_isti_coll
						   		   ,cd_collegamento      as cd_collegamento
								   ,cod_fiscale          as cod_fiscale
								   ,dt_riferimento_udct  as dt_riferimento_udct
                                   ,util_cassa           as util_cassa
						   		   ,imp_accordato        as imp_accordato
						   		   ,dt_riferimento_urda  as dt_riferimento_urda
						   		   ,status_ndg              as status_ndg
							       ,posiz_soff_inc          as posiz_soff_inc
							       ,cd_rap_ristr            as cd_rap_ristr
							       ,tp_cli_scad_scf         as tp_cli_scad_scf
								 ;	   

--STORE cicli_filtrati_1_a  INTO '/user/drlgd/datarequest/dt/tmp/debug/cicli_filtrati_1_a' USING PigStorage(';');

/*
2.	Record con DATAINIZIOPD impostata ed almeno una tra le altre date (DATAINIZOINC, DATAINIZIORISTRUTT, DATASOFFERENZA) impostata e maggiore della DATAINIZIO PD
"	Si cerca la data di superamento soglia nella tabella di appoggio SUPSOGL
	o	se non è valorizzata si imposta DATAINIZIODEF con la minima tra le date DATAINIZIOINC, DATAINIZIORISTRUTT, DATASOFFERENZA.
	o	e è valorizzata, si verifica che sia inferiore a data di inizio ciclo  + n° di mesi del filtro 7.  
"	Se il controllo non viene superato si imposta DATAINIZIODEF con la minima tra le date DATAINIZIOINC, DATAINIZIORISTRUTT, DATASOFFERENZA.
"	Se il controllo viene superato si modificano le  date DATAINIZIODEF e DATAINIZIOPD impostandole con la minima tra ,data di superamento della soglia, DATAINIZIOINC, DATAINIZIORISTRUTT, DATASOFFERENZA.

In questo caso non si scarta nulla.
*/
	cicli_filtrati_2 = FILTER cicli_filtrati 
	                                 BY datainiziopd is not null 
	                                 AND datainiziopd == dt_inizio_ciclo
	                                    AND 
	                                    ( 
	                                      (
	                                        datainizioristrutt is not null
	                                        AND datainizioristrutt > datainiziopd  
	                                      )
	                                     OR 
	                                      (
	                                        datainizioinc is not null
	                                        AND datainizioinc > datainiziopd
	                                      )
	                                     OR 
	                                      ( datainiziosoff is not null
	                                        AND datainiziosoff > datainiziopd
	                                      )  
	                                    )
                                   ;
                                    

cicli_filtrati_2_a = FOREACH cicli_filtrati_2 
GENERATE 
cd_isti              as cd_isti
,ndg_principale       as ndg_principale
/* Se la data superamento soglia è nulla */
  ,(dt_riferimento_urda is null?
    /* Prendo la minima data tra incaglio, etc. */ 
    LeastDate(datainizioinc, datainizioristrutt, datainiziosoff):
      /* Altrimenti devo controllare se la data di sup. + il parametro mesi permanenza pd è minore o uguale della data fine ciclo  */ 
      ( AddDuration(ToDate((chararray)dt_riferimento_urda,'yyyyMMdd'),'$mesi_permanenzapd') < ToDate((chararray)dt_fine_ciclo,'yyyyMMdd') ?
        /* Se si allora è la data minima data tra DATA_SUPERAMENTO_SOGLIA, incaglio, etc.  */  
        LeastDate(dt_inizio_ciclo,dt_riferimento_urda, datainizioinc, datainizioristrutt, datainiziosoff):
          /* Se no allora è la data minima data tra data incaglio, etc. */  
          LeastDate(datainizioinc, datainizioristrutt, datainiziosoff)
      )         
   ) as dt_inizio_ciclo
,dt_fine_ciclo        as dt_fine_ciclo
,(dt_riferimento_urda is null?
    /* se la soglia non è superata allora null */ 
    null:
      /* Altrimenti devo controllare se la data di sup. + il parametro mesi permanenza pd è minore o uguale della data fine ciclo  */ 
      ( AddDuration(ToDate((chararray)dt_riferimento_urda,'yyyyMMdd'),'$mesi_permanenzapd') < ToDate((chararray)dt_fine_ciclo,'yyyyMMdd') ?
        /* Se si allora è la data minima data tra DATA_SUPERAMENTO_SOGLIA, incaglio, etc.  */  
        LeastDate(dt_riferimento_urda, datainizioinc, datainizioristrutt, datainiziosoff):
          /* Se no allora è null. */  
            null
      )         
 ) as datainiziopd 
,datainizioristrutt   as datainizioristrutt
,datainizioinc        as datainizioinc
,datainiziosoff       as datainiziosoff
,c_key                as c_key
,tipo_segmne          as tipo_segmne
,sae_segm             as sae_segm
,rae_segm             as rae_segm
,segmento             as segmento
,tp_ndg	              as tp_ndg
,provincia_segm       as provincia_segm
,databilseg           as databilseg
,strbilseg            as strbilseg
,attivobilseg         as attivobilseg
,fatturbilseg         as fatturbilseg
,ndg_coll             as ndg_coll
,cd_isti_coll         as cd_isti_coll
,cd_collegamento      as cd_collegamento
,cod_fiscale          as cod_fiscale
,dt_riferimento_udct  as dt_riferimento_udct
,util_cassa           as util_cassa
,imp_accordato        as imp_accordato
,dt_riferimento_urda  as dt_riferimento_urda
,status_ndg              as status_ndg
,posiz_soff_inc          as posiz_soff_inc
,cd_rap_ristr            as cd_rap_ristr
,tp_cli_scad_scf         as tp_cli_scad_scf
;	                                     								 

--STORE cicli_filtrati_2_a  INTO '/user/drlgd/datarequest/dt/tmp/debug/cicli_filtrati_2_a' USING PigStorage(';');

/*
Estraggo anche i cicli che non iniziano in PD 
*/
cicli_filtrati_3 = FILTER cicli_filtrati 
                                 BY (datainiziopd is null 
                                     AND 
                                     ( 
                                       datainizioristrutt is not null
                                       OR datainizioinc  is  not null
                                       OR datainiziosoff  is  not null
                                     )
                                    )
                                 OR (
                                     datainiziopd is not null 
                                     AND datainiziopd != dt_inizio_ciclo
                                     AND 
				                       ( 
					                    datainizioristrutt is not null
					                    OR datainizioinc  is  not null
					                    OR datainiziosoff  is  not null
                                        )
                                     )
                                 OR
                                   (
                                    datainiziopd is not null
                                    and datainizioristrutt  is null
					                and datainizioinc       is null
					                and datainiziosoff      is null
					                and dt_riferimento_urda is null
                                   )
                                   ;

--STORE cicli_filtrati_3  INTO '/user/drlgd/datarequest/dt/tmp/debug/cicli_filtrati_3' USING PigStorage(';');

/*
Unione delle tre casistiche 
*/								 
cicli_filtrati_all = UNION cicli_filtrati_1_a
                          ,cicli_filtrati_2_a
                          ,cicli_filtrati_3
                     ;
                     
file_out = DISTINCT ( 
                     FOREACH cicli_filtrati_all
                     GENERATE
                                    cd_isti
						           ,ndg_principale
								   ,dt_inizio_ciclo
								   ,dt_fine_ciclo
								   ,datainiziopd
								   ,datainizioristrutt
								   ,datainizioinc
								   ,datainiziosoff
								   ,c_key
								   ,tipo_segmne
								   ,sae_segm
								   ,rae_segm
								   ,segmento
								   ,tp_ndg
								   ,provincia_segm
								   ,databilseg
								   ,strbilseg
								   ,attivobilseg
								   ,fatturbilseg
								   ,ndg_coll
								   ,cd_isti_coll
								   ,cd_collegamento
								   ,cod_fiscale
								   ,dt_riferimento_udct
						   		   ,util_cassa
						   		   ,imp_accordato
						   		   ,dt_riferimento_urda
					)
;

/* filtro i soggetti per soglia d'importo */		  
cicli_filtrati_all_filter = FILTER cicli_filtrati_all
BY (int)dt_riferimento_udct >= (int)dt_inizio_ciclo
AND SUBSTRING( (chararray)dt_riferimento_udct,0,6 ) <= SUBSTRING( (chararray)LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)dt_fine_ciclo,'yyyyMMdd' ),'P1M'),'yyyyMMdd'), $data_a ),0,6 )
;
                     
/* vengono filtrati i cicli non default, se almeno un indicatore > 0 allora default */
cicli_default_filter = FILTER cicli_filtrati_all_filter 
                       BY (int)status_ndg       > 0
                       OR (int)posiz_soff_inc   > 0
                       OR (int)cd_rap_ristr     > 0
                       OR (int)tp_cli_scad_scf  > 0
                   ;

/* otteniamo la lista dei cicli con tutte le banche dei collegati */
cicli_default_lista = DISTINCT (FOREACH cicli_default_filter 
		                        GENERATE 
		                          cd_isti 
                                 ,ndg_principale 
                                 ,dt_inizio_ciclo 
                                 ,cd_isti_coll 
		                       ); 

STORE file_out  INTO '$cicli_ndg_path/$periodo' USING PigStorage(';');

STORE cicli_default_lista INTO '$ciclindg_listadef/$periodo' USING PigStorage(';');
