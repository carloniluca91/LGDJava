/*

$LastChangedDate: 2016-09-29 15:41:55 +0200 (gio, 29 set 2016) $
$Rev: 573 $		
$Author: arzelenko $	

Sono coinvolti i seguenti parametri:
        -suppsogl_outdir
		-ciclilav_step2_outdir
		-tlbucol_path
		-tlbmignw_pivoted_path
		-tlbudtc_path
		-tlbposi_path
		-periodo

*/



register FiltroSegmento.jar;
register LeastDate.jar;
/*
Carico il file. 
*/


tlbcidef_max1 = LOAD '$suppsogl_outdir'
                USING PigStorage(';') 
                AS (
                     cd_isti              : chararray
                    ,ndg_principale       : chararray
                    ,dt_inizio_ciclo      : chararray
                    ,dt_fine_ciclo        : chararray
                    ,datainiziopd         : chararray
                    ,datainizioristrutt   : chararray
                    ,datainizioinc        : chararray
                    ,datainiziosoff       : chararray
                    ,progr                : chararray
                    ,cd_isti_ced          : chararray
                    ,ndg_ced              : chararray
                    ,dt_riferimento_udct  : int
                   )
                  ;
                  
/* filtro per range di mesi per leggere solo i dati della finestra temporale del periodo di ciclo corrente */
tlbcidef_max = FILTER tlbcidef_max1 BY SUBSTRING((chararray)dt_inizio_ciclo,0,6) >= SUBSTRING('$periodo',0,6) AND SUBSTRING((chararray)dt_inizio_ciclo,0,6) <= ( SUBSTRING('$periodo',7,13) is null ? SUBSTRING('$periodo',0,6) : SUBSTRING('$periodo',7,13) );


/* Leggo la tlbucol */
tlbucol_load = LOAD '$tlbucol_path'   USING PigStorage(';')
          AS (
		      cd_istituto     : chararray
			 ,ndg             : chararray
			 ,cd_collegamento : chararray
			 ,ndg_collegato   : chararray
			 ,dt_riferimento  : int
			 );
			 
/* filtro il tipo di collegamento */
tlbucol_filtered = FILTER tlbucol_load BY cd_collegamento=='103' 
                                       OR cd_collegamento=='105' 
                                       OR cd_collegamento=='106' 
                                       OR cd_collegamento=='107' 
                                       OR cd_collegamento=='207';


/* PARTE UCOL DEL NDG_COLLEGATO_UCOL e si prende NDG nella generate */
cidef_ucol_join_coll = JOIN  tlbcidef_max BY (cd_isti_ced, ndg_ced, dt_riferimento_udct)
                            ,tlbucol_filtered BY (cd_istituto, ndg_collegato, dt_riferimento);
cidef_ucol_coll = FOREACH  cidef_ucol_join_coll
                  GENERATE 	
                     tlbcidef_max::cd_isti              as cd_isti
                    ,tlbcidef_max::ndg_principale       as ndg_principale
                    ,tlbcidef_max::dt_inizio_ciclo      as dt_inizio_ciclo
                    ,tlbcidef_max::dt_fine_ciclo        as dt_fine_ciclo
                    ,tlbcidef_max::datainiziopd         as datainiziopd
                    ,tlbcidef_max::datainizioristrutt   as datainizioristrutt
                    ,tlbcidef_max::datainizioinc        as datainizioinc
                    ,tlbcidef_max::datainiziosoff       as datainiziosoff
                    ,tlbcidef_max::progr                as progr
                    ,tlbucol_filtered::cd_istituto      as cd_isti_coll
                    ,tlbucol_filtered::ndg              as ndg_coll
                    ,tlbucol_filtered::dt_riferimento   as dt_riferimento_ucol
                    ,tlbucol_filtered::cd_collegamento  as cd_collegamento
                    ;  


/* PARTE UCOL DEL NDG_UCOL e si prende NDG_COLLEGATO nella generate  */
cidef_ucol_join_ndg = JOIN  tlbcidef_max BY (cd_isti_ced, ndg_ced, dt_riferimento_udct)
                           ,tlbucol_filtered BY (cd_istituto, ndg, dt_riferimento);    
cidef_ucol_ndg = FOREACH  cidef_ucol_join_ndg
                  GENERATE 	
                     tlbcidef_max::cd_isti              as cd_isti
                    ,tlbcidef_max::ndg_principale       as ndg_principale
                    ,tlbcidef_max::dt_inizio_ciclo      as dt_inizio_ciclo
                    ,tlbcidef_max::dt_fine_ciclo        as dt_fine_ciclo
                    ,tlbcidef_max::datainiziopd         as datainiziopd
                    ,tlbcidef_max::datainizioristrutt   as datainizioristrutt
                    ,tlbcidef_max::datainizioinc        as datainizioinc
                    ,tlbcidef_max::datainiziosoff       as datainiziosoff
                    ,tlbcidef_max::progr                as progr
                    ,tlbucol_filtered::cd_istituto      as cd_isti_coll
                    ,tlbucol_filtered::ndg_collegato    as ndg_coll
                    ,tlbucol_filtered::dt_riferimento   as dt_riferimento_ucol
                    ,tlbucol_filtered::cd_collegamento  as cd_collegamento
                    ;

cidef_ucol_all = UNION cidef_ucol_coll
                      ,cidef_ucol_ndg
                      ;

cicli_ucol = FOREACH  cidef_ucol_all
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
                    ,dt_riferimento_ucol
                    ,cd_collegamento
					;

/*
Carico la udct
*/
tlbudct_load = LOAD '$tlbudtc_path'
  	      USING PigStorage(';')
  	      AS
  	        (
             cd_istituto           : chararray
			,ndg                   : chararray
			,dt_riferimento        : int
			,REDDITO               : chararray
			,TOTALE_UTILIZZI       : chararray
			,TOTALE_FIDI_DELIB     : chararray
			,TOTALE_ACCORDATO      : chararray
			,PATRIMONIO            : chararray
			,N_DIPENDENTI          : chararray
			,TOT_RISCHI_INDIR      : chararray
			,status_ndg            : int
			,STATUS_BASILEA2       : chararray
			,posiz_soff_inc        : int
			,DUBB_ESITO_INC_NDG    : chararray
			,STATUS_CLIENTE_LAB    : chararray
			,TOT_UTIL_MORTGAGE     : chararray
			,TOT_ACCO_MORTGAGE     : chararray
			,DT_ENTRATA_DEFAULT    : chararray
			,CD_STATO_DEF_T0       : chararray
			,CD_TIPO_DEF_T0        : chararray
			,CD_STATO_DEF_A_T12    : chararray
			,cd_rap_ristr          : int
			,TP_RISTRUTT           : chararray
			,tp_cli_scad_scf       : int
		 );	

tlbudct = FOREACH  tlbudct_load
               GENERATE 
                     cd_istituto      as cd_istituto
  					,ndg              as ndg
					,dt_riferimento   as dt_riferimento
					,status_ndg       as status_ndg
					,posiz_soff_inc   as posiz_soff_inc
					,cd_rap_ristr     as cd_rap_ristr
					,tp_cli_scad_scf  as tp_cli_scad_scf
					;

/*
join tra cicli_s1 e tlbudct 
*/
cidef_ucol_tlbudct_join = JOIN cicli_ucol  BY ( cd_isti_coll, ndg_coll, dt_riferimento_ucol )
                              ,tlbudct         BY ( cd_istituto, ndg, dt_riferimento ); 

cicli_ucol_udct = FOREACH  cidef_ucol_tlbudct_join
               GENERATE 
                     cicli_ucol::cd_isti               as cd_isti
                    ,cicli_ucol::ndg_principale        as ndg_principale
                    ,cicli_ucol::dt_inizio_ciclo       as dt_inizio_ciclo
                    ,cicli_ucol::dt_fine_ciclo         as dt_fine_ciclo
                    ,cicli_ucol::datainiziopd          as datainiziopd
                    ,cicli_ucol::datainizioristrutt    as datainizioristrutt
                    ,cicli_ucol::datainizioinc         as datainizioinc
                    ,cicli_ucol::datainiziosoff        as datainiziosoff
                    ,cicli_ucol::progr                 as progr
                    ,cicli_ucol::cd_isti_coll          as cd_isti_coll
                    ,cicli_ucol::ndg_coll              as ndg_coll
                    ,cicli_ucol::dt_riferimento_ucol   as dt_riferimento_ucol
                    ,cicli_ucol::cd_collegamento       as cd_collegamento
                    ,tlbudct::dt_riferimento           as dt_riferimento_udct
                    ,tlbudct::status_ndg               as status_ndg
			        ,tlbudct::posiz_soff_inc           as posiz_soff_inc
			        ,tlbudct::cd_rap_ristr             as cd_rap_ristr
			        ,tlbudct::tp_cli_scad_scf          as tp_cli_scad_scf
				 	;
					
-------------------------------------------------------------------------------------------------------------

/*
Carico la tabella TABURDA
*/
tlburda_load = LOAD '$tlburda_path' 
   USING PigStorage (';')
   AS (
		  cd_istituto              :chararray
		 ,ndg                  	   :chararray
		 ,sportello            	   :chararray
		 ,conto                	   :chararray
		 ,progr_segmento       	   :int
		 ,dt_riferimento       	   :int
		 ,status_rapp          	   :chararray
		 ,margine_fido_rev     	   :chararray
		 ,margine_fido_irrev   	   :chararray
		 ,utilizz_cassa_firm   	   :chararray
		 ,imp_accordato        	   :chararray
		 ,flag_ristrutt        	   :chararray
		 ,imp_accor_ope        	   :chararray
		 ,imp_accor_non_ope    	   :chararray
		 ,gg_sconfino          	   :chararray
		 ,fido_n_ope_sbf_p_i   	   :chararray
		 ,fido_ope_sbf_p_ill   	   :chararray
		 ,fido_ope_firma       	   :chararray
		 ,fido_ope_cassa       	   :chararray
		 ,fido_non_oper_cas    	   :chararray
		 ,fido_non_oper_fir    	   :chararray
		 ,fido_n_ope_sbf_ced   	   :chararray
		 ,fido_ope_sbf_ced         :chararray
		 ,aut_sconf_cassa          :chararray
		 ,aut_sconf_sbf            :chararray
		 ,aut_sconf_firma          :chararray
		 ,gar_merci                :chararray
		 ,gar_ipoteca              :chararray
		 ,gar_personale            :chararray
		 ,gar_altre                :chararray
		 ,gar_obbl_pr_emiss        :chararray
		 ,gar_obbl_b_centr         :chararray
		 ,gar_obbl_altre           :chararray
		 ,gar_dep_denaro           :chararray
		 ,gar_cd_pr_emiss          :chararray
		 ,gar_altri_valori         :chararray
		 ,valore_dossier           :chararray
		 ,fido_deriv               :chararray
		 ,saldo_cont               :chararray
		 ,saldo_cont_dare          :chararray
		 ,saldo_cont_avere         :chararray
		 ,part_illiq_sbf           :chararray
		 ,ind_util_fido_med        :chararray
		 ,imp_fido_acc_firma       :chararray
		 ,imp_utilizzo_firma       :chararray
		 ,imp_gar_pers_cred        :chararray
		 ,imp_sal_lq_med_dar       :chararray
		 ,imp_utilizzo_cassa       :chararray
		 );

tlburda_proj = FOREACH  tlburda_load
               GENERATE 
                     cd_istituto         as cd_istituto
		            ,ndg                 as ndg
		            ,dt_riferimento      as dt_riferimento
		            ,progr_segmento      as progr_segmento
		            ,(double)REPLACE(imp_accordato, ',','.')       as imp_accordato
		            ,(double)REPLACE(imp_utilizzo_cassa, ',','.')  as imp_utilizzo_cassa
					;
					
tlburda_filter = FILTER tlburda_proj BY progr_segmento == 0;

tlburda_group = GROUP tlburda_filter BY (cd_istituto, ndg, dt_riferimento);
  
tlburda = FOREACH  tlburda_group
               GENERATE 
                     group.cd_istituto              as cd_istituto
		            ,group.ndg                      as ndg
		            ,group.dt_riferimento           as dt_riferimento
		            ,SUM(tlburda_filter.imp_accordato)       as imp_accordato
		            ,SUM(tlburda_filter.imp_utilizzo_cassa)  as imp_utilizzo_cassa
					;

/*aggiunti accordato e utilizzato_cassa*/
cicli_s1_tlbudct_urda_join = JOIN cicli_ucol_udct  BY (cd_isti_coll, ndg_coll, dt_riferimento_ucol ) LEFT
                            ,tlburda          BY (cd_istituto, ndg, dt_riferimento ); 

cicli_ucol_udct_urda = FOREACH  cicli_s1_tlbudct_urda_join
               GENERATE 
                     cicli_ucol_udct::cd_isti               as cd_isti
                    ,cicli_ucol_udct::ndg_principale        as ndg_principale
                    ,cicli_ucol_udct::dt_inizio_ciclo       as dt_inizio_ciclo
                    ,cicli_ucol_udct::dt_fine_ciclo         as dt_fine_ciclo
                    ,cicli_ucol_udct::datainiziopd          as datainiziopd
                    ,cicli_ucol_udct::datainizioristrutt    as datainizioristrutt
                    ,cicli_ucol_udct::datainizioinc         as datainizioinc
                    ,cicli_ucol_udct::datainiziosoff        as datainiziosoff
                    ,cicli_ucol_udct::progr                 as progr
                    ,cicli_ucol_udct::cd_isti_coll          as cd_isti_coll
                    ,cicli_ucol_udct::ndg_coll              as ndg_coll
                    ,cicli_ucol_udct::dt_riferimento_ucol   as dt_riferimento_ucol
                    ,cicli_ucol_udct::cd_collegamento       as cd_collegamento
                    ,cicli_ucol_udct::dt_riferimento_udct   as dt_riferimento_udct
                    ,cicli_ucol_udct::status_ndg            as status_ndg
			        ,cicli_ucol_udct::posiz_soff_inc        as posiz_soff_inc
			        ,cicli_ucol_udct::cd_rap_ristr          as cd_rap_ristr
			        ,cicli_ucol_udct::tp_cli_scad_scf       as tp_cli_scad_scf
			        ,tlburda::dt_riferimento                as dt_riferimento_urda
					,tlburda::imp_utilizzo_cassa            as util_cassa
					,tlburda::imp_accordato                 as imp_accordato
				 	;

tlbucoc_load = LOAD '$tlbucoc_path' 
               USING PigStorage(';')
					  AS (
							cd_istituto              :chararray
							,ndg                     :chararray
							,sportello               :chararray
							,conto                   :chararray
							,progr_segmento          :int
							,dt_riferimento          :int
							,fido_non_oper_cas       :chararray
							,fido_ope_cassa          :chararray
							,fido_op_pf_ns_mut       :chararray
							,fido_ope_sbf            :chararray
							,mass_util_sbf           :chararray
							,sal_con_ced_ef_sbf      :chararray
							,saldo_cont_dare         :chararray
							,saldo_cont_avere        :chararray
							,part_illiq_sbf          :chararray
							,assegni_pr_ins_on       :chararray
							,ass_trz_ins_ng_sbf      :chararray
							,addebiti_in_sosp        :chararray
							);

tlbucoc_proj = FOREACH tlbucoc_load				
          GENERATE
              cd_istituto as cd_istituto
             ,ndg as ndg
             ,dt_riferimento as dt_riferimento
             ,progr_segmento as progr_segmento
             ,(double)REPLACE(addebiti_in_sosp, ',','.')  as addebiti_in_sosp
          	;

tlbucoc_filter = FILTER tlbucoc_proj BY progr_segmento==0;

tlbucoc_group = GROUP tlbucoc_filter BY (cd_istituto, ndg, dt_riferimento);

tlbucoc = FOREACH  tlbucoc_group
               GENERATE 
                     group.cd_istituto              as cd_istituto
		            ,group.ndg                      as ndg
		            ,group.dt_riferimento           as dt_riferimento
		            ,SUM(tlbucoc_filter.addebiti_in_sosp)    as addebiti_in_sosp
					;

-------------------------------------------------------------------------------------------------------------
/* aggiunto addebiti_in_sospeso */
cicli_ucol_tlbudct_urda_ucoc = JOIN cicli_ucol_udct_urda  BY (cd_isti_coll, ndg_coll, dt_riferimento_ucol ) LEFT
                                   ,tlbucoc               BY (cd_istituto, ndg, dt_riferimento ); 
 
/*
generate finale
*/
file_out = FOREACH cicli_ucol_tlbudct_urda_ucoc
                   GENERATE
                            cicli_ucol_udct_urda::cd_isti              as cd_isti
                           ,cicli_ucol_udct_urda::ndg_principale       as ndg_principale
                           ,cicli_ucol_udct_urda::dt_inizio_ciclo      as dt_inizio_ciclo
                           ,cicli_ucol_udct_urda::dt_fine_ciclo        as dt_fine_ciclo
                           ,cicli_ucol_udct_urda::datainiziopd         as datainiziopd
                           ,cicli_ucol_udct_urda::datainizioristrutt   as datainizioristrutt
                           ,cicli_ucol_udct_urda::datainizioinc        as datainizioinc
                           ,cicli_ucol_udct_urda::datainiziosoff       as datainiziosoff
                           ,cicli_ucol_udct_urda::progr                as progr
                           ,cicli_ucol_udct_urda::cd_isti_coll         as cd_isti_coll
                           ,cicli_ucol_udct_urda::ndg_coll             as ndg_coll
                           ,cicli_ucol_udct_urda::dt_riferimento_udct  as dt_riferimento_udct
                           ,cicli_ucol_udct_urda::status_ndg           as status_ndg
					       ,cicli_ucol_udct_urda::posiz_soff_inc       as posiz_soff_inc
					       ,cicli_ucol_udct_urda::cd_rap_ristr         as cd_rap_ristr
					       ,cicli_ucol_udct_urda::tp_cli_scad_scf      as tp_cli_scad_scf
					       ,cicli_ucol_udct_urda::dt_riferimento_urda  as dt_riferimento_urda
					       ,cicli_ucol_udct_urda::util_cassa           as util_cassa
					       ,cicli_ucol_udct_urda::imp_accordato        as imp_accordato
					       ,tlbucoc::addebiti_in_sosp                  as tot_add_sosp
					       ,cicli_ucol_udct_urda::cd_collegamento      as cd_collegamento
                           ;
                           
file_out_dist = DISTINCT ( 
                          FOREACH file_out                          
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
                           ,status_ndg
					       ,posiz_soff_inc
					       ,cd_rap_ristr
					       ,tp_cli_scad_scf
					       ,dt_riferimento_urda
					       ,util_cassa
					       ,imp_accordato
					       ,tot_add_sosp
					       ,cd_collegamento)
						    ;  

STORE file_out_dist INTO '$ciclilav_step2_outdir/$periodo'  USING PigStorage(';');
