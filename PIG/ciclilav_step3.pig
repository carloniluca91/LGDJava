/*

$LastChangedDate: 2017-03-02 12:18:25 +0100 (gio, 02 mar 2017) $
$Rev: 610 $		
$Author: arzelenko $	

Sono coinvolti i seguenti parametri:
		-ciclilav_step2_outdir
		-suppsogl_outdir
		-ciclilav_outdir

*/

/*
Carico il file xxx (suppsogl) che contiene cicli e migrati filtrati per udct con posi agganciata
*/
cicli_xxx_load = LOAD '$suppsogl_outdir' USING PigStorage(';')
		                   AS (
                                cd_isti               : chararray
                               ,ndg_principale        : chararray
                               ,dt_inizio_ciclo       : chararray
                               ,dt_fine_ciclo         : chararray
                               ,datainiziopd          : chararray
                               ,datainizioristrutt    : chararray
                               ,datainizioinc         : chararray
                               ,datainiziosoff        : chararray
                               ,progr                 : chararray
                               ,cd_isti_coll          : chararray
                               ,ndg_coll              : chararray
                               ,dt_riferimento_udct   : chararray
                               ,status_ndg            : int
					           ,posiz_soff_inc        : int
					           ,cd_rap_ristr          : int
					           ,tp_cli_scad_scf       : int
					           ,dt_riferimento_urda   : chararray
					           ,util_cassa            : chararray
					           ,imp_accordato         : chararray
					           ,tot_add_sosp          : chararray
                               --campo non presente fisicamente nel tracciato, solo per compatibilita'
                               ,cd_collegamento       : chararray
							  )
							 ;

/*
Carico il file yyy-kkk (step2) che contiene ndg_collegato e ndg della ucol con i loro migrati filtrati per udct con posi agganciata
*/
cicli_yyy = LOAD '$ciclilav_step2_outdir' USING PigStorage(';')
		                   AS (
                                cd_isti               : chararray
                               ,ndg_principale        : chararray
                               ,dt_inizio_ciclo       : chararray
                               ,dt_fine_ciclo         : chararray
                               ,datainiziopd          : chararray
                               ,datainizioristrutt    : chararray
                               ,datainizioinc         : chararray
                               ,datainiziosoff        : chararray
                               ,progr                 : chararray
                               ,cd_isti_coll          : chararray
                               ,ndg_coll              : chararray
                               ,dt_riferimento_udct   : chararray
                               ,status_ndg            : int
					           ,posiz_soff_inc        : int
					           ,cd_rap_ristr          : int
					           ,tp_cli_scad_scf       : int
					           ,dt_riferimento_urda   : chararray
					           ,util_cassa            : chararray
					           ,imp_accordato         : chararray
					           ,tot_add_sosp          : chararray
                               ,cd_collegamento       : chararray
							  )
							 ;
							 
cicli_princ_coll_join = JOIN  cicli_xxx_load   BY ( cd_isti_coll, ndg_coll, dt_inizio_ciclo, dt_riferimento_udct ) LEFT
                             ,cicli_yyy        BY ( cd_isti_coll, ndg_coll, dt_inizio_ciclo, dt_riferimento_udct );

/* vengono selezionati soli ndg per ciclo non ritrovati come collegati */
cicli_princ_no_coll_filt = FILTER cicli_princ_coll_join 
BY cicli_yyy::cd_isti_coll is null;

cicli_xxx = FOREACH cicli_princ_no_coll_filt
                   GENERATE
                                cicli_xxx_load::cd_isti              as cd_isti
                               ,cicli_xxx_load::ndg_principale       as ndg_principale
                               ,cicli_xxx_load::dt_inizio_ciclo      as dt_inizio_ciclo
                               ,cicli_xxx_load::dt_fine_ciclo        as dt_fine_ciclo
                               ,cicli_xxx_load::datainiziopd         as datainiziopd
                               ,cicli_xxx_load::datainizioristrutt   as datainizioristrutt
                               ,cicli_xxx_load::datainizioinc        as datainizioinc
                               ,cicli_xxx_load::datainiziosoff       as datainiziosoff
                               ,cicli_xxx_load::progr                as progr
                               ,cicli_xxx_load::cd_isti_coll         as cd_isti_coll
                               ,cicli_xxx_load::ndg_coll             as ndg_coll
                               ,cicli_xxx_load::dt_riferimento_udct  as dt_riferimento_udct
                               ,cicli_xxx_load::status_ndg           as status_ndg
					           ,cicli_xxx_load::posiz_soff_inc       as posiz_soff_inc
					           ,cicli_xxx_load::cd_rap_ristr         as cd_rap_ristr
					           ,cicli_xxx_load::tp_cli_scad_scf      as tp_cli_scad_scf
					           ,cicli_xxx_load::dt_riferimento_urda  as dt_riferimento_urda
					           ,cicli_xxx_load::util_cassa           as util_cassa
					           ,cicli_xxx_load::imp_accordato        as imp_accordato
					           ,cicli_xxx_load::tot_add_sosp         as tot_add_sosp
                               ,cicli_xxx_load::cd_collegamento      as cd_collegamento
                           ; 

/* fondiamo entrambi i files con i cicli e collegati */
cicli_all = UNION cicli_xxx
                 ,cicli_yyy
                 ;

/* vengono filtrati i cicli non default, se almeno un indicatore > 0 allora default */
/*
cicli_default_filter = FILTER cicli_all 
                       BY status_ndg       > 0
                       OR posiz_soff_inc   > 0
                       OR cd_rap_ristr     > 0
                       OR tp_cli_scad_scf  > 0
                   ;
*/
/* otteniamo la lista dei cicli con tutte le banche dei collegati */
/*
cicli_default_lista = DISTINCT (FOREACH cicli_default_filter 
		                        GENERATE 
		                          cd_isti 
                                 ,ndg_principale 
                                 ,dt_inizio_ciclo 
                                 ,cd_isti_coll 
		                       ); 
*/

/* filtriamo la lista completa con il file-lista ottenuto dallo step precedente per prendere solo i collegati delle banche utili */
--cicli_all_lista_join = JOIN  cicli_all           BY ( cd_isti, ndg_principale, dt_inizio_ciclo, cd_isti_coll )
--                            ,cicli_default_lista BY ( cd_isti, ndg_principale, dt_inizio_ciclo, cd_isti_coll );

/* costruzione file finale pronto per filtro soglia e impostazione delle date inizio e pd */

file_out = DISTINCT ( 
                     FOREACH cicli_all
                     GENERATE
                        cd_isti
                       ,ndg_principale
                       ,dt_inizio_ciclo
                       ,dt_fine_ciclo
                       ,datainiziopd
                       ,datainizioristrutt
                       ,datainizioinc
                       ,datainiziosoff
                       ,progr
                       ,cd_isti_coll
                       ,ndg_coll
                       ,dt_riferimento_udct
				       ,dt_riferimento_urda
				       ,util_cassa
				       ,imp_accordato
				       ,tot_add_sosp
				       ,cd_collegamento
				       ,status_ndg
				       ,posiz_soff_inc
				       ,cd_rap_ristr
				       ,tp_cli_scad_scf
					)
;


STORE file_out INTO '$ciclilav_outdir' USING PigStorage(';');

--STORE cicli_default_lista INTO '$ciclilav_listadef' USING PigStorage(';');
