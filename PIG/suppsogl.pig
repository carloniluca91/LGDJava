/*
$LastChangedDate: 2016-10-28 14:49:31 +0200 (ven, 28 ott 2016) $
$Rev: 585 $		
$Author: arzelenko $	

Riceve in input le sogli minime e crea una tabella temporanea con gli ndg e le soglie

Sono coinvolti i seguenti parametri:
	-suppsogl_outdir
	-ciclilav_step1_outdir
    -tlbudtc_path
    -tlbposi_path
    -data_a
*/	

register LeastDate.jar;

/*
caricamento file cicli prodotto dallo step_1 di prelavorato
*/
cicli_s1 = LOAD '$ciclilav_step1_outdir' USING PigStorage(';') 
          AS
          (   
            cd_isti              : chararray
           ,ndg_principale       : chararray
           ,dt_inizio_ciclo      : int
           ,dt_fine_ciclo        : int
           ,datainiziopd         : int
           ,datainizioristrutt   : int
           ,datainizioinc        : int
           ,datainiziosoff       : int
           ,progr                : int
           ,cd_isti_ced          : chararray
           ,ndg_ced              : chararray
          );

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
cicli_s1_tlbudct_join = JOIN cicli_s1  BY (cd_isti_ced, ndg_ced )
                            ,tlbudct   BY (cd_istituto, ndg ); 

cicli_s1_tlbudct_join_filter = FILTER cicli_s1_tlbudct_join 
  BY ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)dt_inizio_ciclo,'yyyyMMdd'),'$numero_mesi_1') 
  --AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING( (chararray)LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)dt_fine_ciclo,'yyyyMMdd' ),'P1M'),'yyyyMMdd'),$data_a),0,6 );
  AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(ToString(AddDuration( ToDate( (chararray)LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)dt_fine_ciclo,'yyyyMMdd' ),'P1M'),'yyyyMMdd'), $data_a ),'yyyyMMdd' ),'$numero_mesi_2' ),'yyyyMMdd'),0,6 )
;

cicli_udct = FOREACH  cicli_s1_tlbudct_join_filter
               GENERATE 
                    cicli_s1::cd_isti              as cd_isti
                   ,cicli_s1::ndg_principale       as ndg_principale
                   ,cicli_s1::dt_inizio_ciclo      as dt_inizio_ciclo
                   ,cicli_s1::dt_fine_ciclo        as dt_fine_ciclo
                   ,cicli_s1::datainiziopd         as datainiziopd
                   ,cicli_s1::datainizioristrutt   as datainizioristrutt
                   ,cicli_s1::datainizioinc        as datainizioinc
                   ,cicli_s1::datainiziosoff       as datainiziosoff
                   ,cicli_s1::progr                as progr
                   ,cicli_s1::cd_isti_ced          as cd_isti_ced
                   ,cicli_s1::ndg_ced              as ndg_ced
                   ,tlbudct::dt_riferimento        as dt_riferimento_udct
                   ,tlbudct::status_ndg            as status_ndg
			       ,tlbudct::posiz_soff_inc        as posiz_soff_inc
			       ,tlbudct::cd_rap_ristr          as cd_rap_ristr
			       ,tlbudct::tp_cli_scad_scf       as tp_cli_scad_scf
                   ;

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
/*join cicli_udct e urda*/
cicli_udct_urda_join = JOIN cicli_udct  BY (cd_isti_ced, ndg_ced, dt_riferimento_udct ) LEFT
                                 ,tlburda     BY (cd_istituto, ndg, dt_riferimento ); 

cicli_udct_urda = FOREACH  cicli_udct_urda_join
                  GENERATE 
                    cicli_udct::cd_isti              as cd_isti
                   ,cicli_udct::ndg_principale       as ndg_principale
                   ,cicli_udct::dt_inizio_ciclo      as dt_inizio_ciclo
                   ,cicli_udct::dt_fine_ciclo        as dt_fine_ciclo
                   ,cicli_udct::datainiziopd         as datainiziopd
                   ,cicli_udct::datainizioristrutt   as datainizioristrutt
                   ,cicli_udct::datainizioinc        as datainizioinc
                   ,cicli_udct::datainiziosoff       as datainiziosoff
                   ,cicli_udct::progr                as progr
                   ,cicli_udct::cd_isti_ced          as cd_isti_ced
                   ,cicli_udct::ndg_ced              as ndg_ced
                   ,cicli_udct::dt_riferimento_udct  as dt_riferimento_udct
                   ,cicli_udct::status_ndg           as status_ndg
			       ,cicli_udct::posiz_soff_inc       as posiz_soff_inc
			       ,cicli_udct::cd_rap_ristr         as cd_rap_ristr
			       ,cicli_udct::tp_cli_scad_scf      as tp_cli_scad_scf
			       ,tlburda::dt_riferimento          as dt_riferimento_urda
			       ,tlburda::imp_utilizzo_cassa      as util_cassa
			       ,tlburda::imp_accordato           as imp_accordato
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
/*join cicli_udct_urda e ucoc*/
cicli_udct_urda_ucoc_join = JOIN cicli_udct_urda  BY (cd_isti_ced, ndg_ced, dt_riferimento_udct ) LEFT
                                ,tlbucoc          BY (cd_istituto, ndg, dt_riferimento ); 

/*generate finale*/
file_out = FOREACH cicli_udct_urda_ucoc_join
                   GENERATE
                            cicli_udct_urda::cd_isti              as cd_isti
                           ,cicli_udct_urda::ndg_principale       as ndg_principale
                           ,cicli_udct_urda::dt_inizio_ciclo      as dt_inizio_ciclo
                           ,cicli_udct_urda::dt_fine_ciclo        as dt_fine_ciclo
                           ,cicli_udct_urda::datainiziopd         as datainiziopd
                           ,cicli_udct_urda::datainizioristrutt   as datainizioristrutt
                           ,cicli_udct_urda::datainizioinc        as datainizioinc
                           ,cicli_udct_urda::datainiziosoff       as datainiziosoff
                           ,cicli_udct_urda::progr                as progr
                           ,cicli_udct_urda::cd_isti_ced          as cd_isti_ced
                           ,cicli_udct_urda::ndg_ced              as ndg_ced
                           ,cicli_udct_urda::dt_riferimento_udct  as dt_riferimento_udct
                           ,cicli_udct_urda::status_ndg           as status_ndg
					       ,cicli_udct_urda::posiz_soff_inc       as posiz_soff_inc
					       ,cicli_udct_urda::cd_rap_ristr         as cd_rap_ristr
					       ,cicli_udct_urda::tp_cli_scad_scf      as tp_cli_scad_scf
					       ,cicli_udct_urda::dt_riferimento_urda  as dt_riferimento_urda
					       ,cicli_udct_urda::util_cassa           as util_cassa
					       ,cicli_udct_urda::imp_accordato        as imp_accordato
					       ,tlbucoc::addebiti_in_sosp             as tot_add_sosp
                           ;  

STORE file_out INTO '$suppsogl_outdir' USING PigStorage(';');
