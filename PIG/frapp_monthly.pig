/* 
$LastChangedDate: 2016-08-01 15:54:46 +0200 (lun, 01 ago 2016) $
$Rev: 522 $		
$Author: arzelenko $	

Sono coinvolti i seguenti parametri:
		-frapp_ndg_outdir
		-tlburda_path
		-tlburat_path
		-tlbucoc_path
		-frapp_outdir
		-periodo
		
*/

/*
Programma bancapopolare su mapping FRAPP
C = join small by t, large by x;
*/

/*
SET mapred.child.java.opts "-Xmx2G"
SET mapreduce.map.java.opts "-Xmx2G"
SET mapreduce.map.memory.mb 2048
*/ 

/*
Carico il file con l'elenco dei rapporti
*/
frapp_ndg = LOAD '$frapp_ndg_outdir'
           USING PigStorage(';')
     	   AS
              (
				  codicebanca    : chararray
				 ,ndgprincipale  : chararray   
				 ,codicebanca_collegato : chararray     
				 ,ndg_collegato  : chararray 
				 ,datainiziodef  : chararray   
				 ,datafinedef    : chararray
				 ,cd_istituto    : chararray 
				 ,ndg            : chararray
				 ,sportello      : chararray
				 ,conto          : chararray
				 ,dt_riferimento : chararray    
				 ,conto_esteso   : chararray  
				 ,forma_tecnica  : chararray    
				 ,dt_accensione  : chararray   
				 ,dt_estinzione  : chararray   
				 ,dt_scadenza    : chararray 
				 ,tp_ammortamento: chararray    
				 ,tp_rapporto    : chararray 
				 ,period_liquid  : chararray   
				 ,cd_prodotto_ris   : chararray  
				 ,durata_originaria  : chararray   
				 ,divisa       : chararray
				 ,durata_residua     : chararray
				 ,tp_contr_rapp     : chararray
			  );

/* filtro per range di mesi per leggere solo i dati della finestra temporale del ciclo corrente */
--frapp_ndg = FILTER frapp_ndg1 BY SUBSTRING((chararray)datainiziodef,0,6) >= SUBSTRING('$periodo',0,6) AND SUBSTRING((chararray)datainiziodef,0,6) <= ( SUBSTRING('$periodo',7,13) is null ? SUBSTRING('$periodo',0,6) : SUBSTRING('$periodo',7,13) );

       
/*
Carico la tabella TABURDA
*/

tlburda = LOAD '$tlburda_path' 
   USING PigStorage (';')
   AS (
		 cd_istituto               :chararray
		 ,ndg                  	   :chararray
		 ,sportello            	   :chararray
		 ,conto                	   :chararray
		 ,progr_segmento       	   :int
		 ,dt_riferimento       	   :chararray
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
		 ,imp_der_val_int_ps       :chararray
		 ,imp_der_val_int_ng       :chararray
		 ,imp_gpm_alt_ban          :chararray
		 ,val_stima_bene_155       :chararray
		 ,val_mercato_immobi       :chararray
		 ,gar_titoli_6998          :chararray
		 ,gar_pers_6970            :chararray
		 ,d6750_fair_value         :chararray
		 ,d6755_costo_ammort       :chararray
		 ,utilizzo_titoli          :chararray

   );

/*filtro con progr_segmento uguale a 0*/

tlburda_filter = FILTER tlburda BY progr_segmento == 0;
   
/*
Faccio la JOIN tra le due tabelle TLBCIDE_TLBURTT e TLBURDA
*/

tlbcidef_tlburtt_tlburda_filter_join = JOIN frapp_ndg  BY (codicebanca_collegato, ndg_collegato, sportello, conto, dt_riferimento),
                                            tlburda_filter BY (cd_istituto, ndg, sportello, conto, dt_riferimento)
                                            ;

/*
tlbcidef_tlburtt_tlburda_filter_join_filt = FILTER tlbcidef_tlburtt_tlburda_filter_join 
 BY ToDate(tlburda_filter::dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate(datainiziodef,'yyyyMMdd'),'$numero_mesi_1')  
and ToDate(tlburda_filter::dt_riferimento,'yyyyMMdd') <= AddDuration(ToDate(datafinedef,'yyyyMMdd'),'$numero_mesi_2');
*/

tlbcidef_tlburtt_tlburda = FOREACH tlbcidef_tlburtt_tlburda_filter_join
					       GENERATE 
						   			 frapp_ndg::codicebanca      AS codicebanca     
									,frapp_ndg::ndgprincipale     AS  ndgprincipale     
									,frapp_ndg::codicebanca_collegato      AS codicebanca_collegato     
									,frapp_ndg::ndg_collegato     AS  ndg_collegato
									,frapp_ndg::datainiziodef     AS  datainiziodef     
									,frapp_ndg::datafinedef     AS  datafinedef
									,frapp_ndg::sportello       AS  sportello      
									,frapp_ndg::conto        AS  conto       
									,frapp_ndg::dt_riferimento     AS  dt_riferimento     
									,frapp_ndg::conto_esteso      AS  conto_esteso     
									,frapp_ndg::forma_tecnica      AS  forma_tecnica      
									,frapp_ndg::dt_accensione      AS  dt_accensione     
									,frapp_ndg::dt_estinzione      AS  dt_estinzione     
									,frapp_ndg::dt_scadenza      AS  dt_scadenza     
									,frapp_ndg::tp_ammortamento     AS  tp_ammortamento    
									,frapp_ndg::tp_rapporto      AS  tp_rapporto     
									,frapp_ndg::period_liquid      AS  period_liquid     
									,frapp_ndg::cd_prodotto_ris     AS  cd_prodotto_ris    
									,frapp_ndg::durata_originaria     AS  durata_originaria   
									,frapp_ndg::divisa       AS  divisa       
									,frapp_ndg::durata_residua     AS  durata_residua     
									,frapp_ndg::tp_contr_rapp      AS  tp_contr_rapp    
									,tlburda_filter::margine_fido_rev    AS  margine_fido_rev				
									,tlburda_filter::margine_fido_irrev    AS  margine_fido_irrev    
									,tlburda_filter::utilizz_cassa_firm    AS  utilizz_cassa_firm    
									,tlburda_filter::imp_accordato     AS  imp_accordato     
									,tlburda_filter::flag_ristrutt     AS  flag_ristrutt     
									,tlburda_filter::imp_accor_ope     AS  imp_accor_ope     
									,tlburda_filter::imp_accor_non_ope    AS  imp_accor_non_ope    
									,tlburda_filter::gg_sconfino     AS  gg_sconfino     
									,tlburda_filter::fido_n_ope_sbf_p_i    AS  fido_n_ope_sbf_p_i    
									,tlburda_filter::fido_ope_sbf_p_ill    AS  fido_ope_sbf_p_ill    
									,tlburda_filter::fido_ope_firma     AS  fido_ope_firma     
									,tlburda_filter::fido_ope_cassa     AS  fido_ope_cassa     
									,tlburda_filter::fido_non_oper_cas    AS  fido_non_oper_cas    
									,tlburda_filter::fido_non_oper_fir    AS  fido_non_oper_fir    
									,tlburda_filter::fido_n_ope_sbf_ced    AS  fido_n_ope_sbf_ced    
									,tlburda_filter::fido_ope_sbf_ced    AS  fido_ope_sbf_ced    
									,tlburda_filter::aut_sconf_cassa    AS  aut_sconf_cassa    
									,tlburda_filter::aut_sconf_sbf     AS  aut_sconf_sbf     
									,tlburda_filter::aut_sconf_firma    AS  aut_sconf_firma    
									,tlburda_filter::gar_merci      AS  gar_merci      
									,tlburda_filter::gar_ipoteca     AS  gar_ipoteca     
									,tlburda_filter::gar_personale     AS  gar_personale     
									,tlburda_filter::gar_altre      AS  gar_altre      
									,tlburda_filter::gar_obbl_pr_emiss    AS  gar_obbl_pr_emiss    
									,tlburda_filter::gar_obbl_b_centr    AS  gar_obbl_b_centr    
									,tlburda_filter::gar_obbl_altre     AS  gar_obbl_altre     
									,tlburda_filter::gar_dep_denaro     AS  gar_dep_denaro     
									,tlburda_filter::gar_cd_pr_emiss    AS  gar_cd_pr_emiss    
									,tlburda_filter::gar_altri_valori    AS  gar_altri_valori    
									,tlburda_filter::valore_dossier     AS  valore_dossier     
									,tlburda_filter::fido_deriv      AS  fido_deriv      
									,tlburda_filter::saldo_cont      AS  saldo_cont      
									,tlburda_filter::saldo_cont_dare    AS  saldo_cont_dare    
									,tlburda_filter::saldo_cont_avere    AS  saldo_cont_avere    
									,tlburda_filter::part_illiq_sbf     AS  part_illiq_sbf     
									,tlburda_filter::ind_util_fido_med    AS  ind_util_fido_med    
									,tlburda_filter::imp_fido_acc_firma    AS  imp_fido_acc_firma    
									,tlburda_filter::imp_utilizzo_firma    AS  imp_utilizzo_firma    
									,tlburda_filter::imp_gar_pers_cred    AS  imp_gar_pers_cred    
									,tlburda_filter::imp_sal_lq_med_dar    AS  imp_sal_lq_med_dar    
									,tlburda_filter::imp_utilizzo_cassa    AS  imp_utilizzo_cassa    
									,tlburda_filter::imp_der_val_int_ps    AS  imp_der_val_int_ps    
									,tlburda_filter::imp_der_val_int_ng    AS  imp_der_val_int_ng    
									,tlburda_filter::gar_titoli_6998          AS  gar_titoli_6998     
									,tlburda_filter::gar_pers_6970            AS  gar_pers_6970       				  
								  ;
			   
--STORE tlbcidef_tlburtt_tlburda INTO '/user/drlgd/datarequest/dt/tmp/debug/tlbcidef_tlburtt_tlburda/$periodo' USING PigStorage(';');

/*Carico la tabella TLBURAT*/

tlburat_load = LOAD '$tlburat_path' 
				  USING PigStorage (';')
				  AS ( 
				     cd_istituto    :chararray
				    ,ndg     :chararray
				    ,sportello    :chararray
				    ,conto     :chararray
				    ,progr_segmento   :int
				    ,dt_riferimento   :chararray
				    ,q_cap_rate_a_scad  :chararray
				    ,q_cap_rate_impag  :chararray
				    ,q_inter_rate_impag  :chararray
				    ,n_rate_impag   :chararray
				    ,dt_scd_pr_rata_imp  :chararray
				    ,q_cap_rate_mora  :chararray
				    ,q_inter_rate_mora  :chararray
				    ,acconti_rate_scad  :chararray
				    ,utilizz_cassa_firm  :chararray
				    ,imp_accordato   :chararray
				    ,cd_prodotto_rischi  :chararray
				    ,n_gg_rate_imp   :chararray
				    ,durata_originaria  :chararray
				    ,imp_orig_prestito  :chararray
				    ,durata_res_default  :chararray
				    ,durata_residua   :chararray
				    ,dt_scadenza   :chararray
				    ,rate_scad_imp   :chararray
				    ,oneri_acc_leasing  :chararray
				    ,dt_pr_rt_leas_imp  :chararray
				    ,imp_can_leas_ins  :chararray
				    ,gg_sconfino   :chararray
				    ,tp_mutuo    :chararray
				    ,rate_scad_imp_sosp  :chararray
					);	
		
tlburat = FOREACH tlburat_load
          GENERATE
               cd_istituto
              ,dt_riferimento
              ,ndg
              ,sportello
              ,conto
              ,q_cap_rate_a_scad     
			  ,q_cap_rate_impag      		
			  ,q_inter_rate_impag     
			  ,q_cap_rate_mora       
			  ,q_inter_rate_mora     
			  ,acconti_rate_scad      
			  ,imp_orig_prestito     
			  ,progr_segmento	
			 ;
			 
/*filtro con progr_segmento uguale a 0*/

tlburat_filter = FILTER tlburat BY progr_segmento == 0;

/*Faccio la JOIN tra le due tabelle TLBCIDE_TLBURTT_TLBURDA e TLBURAT_FILTER*/

tlbcidef_tlburtt_tlburda_talburat_filter_join = JOIN tlbcidef_tlburtt_tlburda BY (codicebanca_collegato, ndg_collegato, sportello, conto, dt_riferimento) LEFT, 
                                                     tlburat_filter BY (cd_istituto, ndg, sportello, conto, dt_riferimento)
                                                     ;

/*
tlbcidef_tlburtt_tlburda_talburat_filter_join_filt2 = FILTER tlbcidef_tlburtt_tlburda_talburat_filter_join 
 BY ToDate(tlburat_filter::dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate(datainiziodef,'yyyyMMdd'),'$numero_mesi_1')  
and ToDate(tlburat_filter::dt_riferimento,'yyyyMMdd') <= AddDuration(ToDate(datafinedef,'yyyyMMdd'),'$numero_mesi_2');
*/
tlbcidef_tlburtt_tlburda_tlburat = FOREACH tlbcidef_tlburtt_tlburda_talburat_filter_join
							       GENERATE
											 tlbcidef_tlburtt_tlburda::codicebanca      AS codicebanca     
											,tlbcidef_tlburtt_tlburda::ndgprincipale     AS  ndgprincipale    
                                            ,tlbcidef_tlburtt_tlburda::codicebanca_collegato      AS codicebanca_collegato     
									        ,tlbcidef_tlburtt_tlburda::ndg_collegato     AS  ndg_collegato 
											,tlbcidef_tlburtt_tlburda::datainiziodef     AS  datainiziodef     
											,tlbcidef_tlburtt_tlburda::datainiziodef     AS  datafinedef
											,tlbcidef_tlburtt_tlburda::sportello       AS  sportello      				
											,tlbcidef_tlburtt_tlburda::conto        AS  conto       				
											,tlbcidef_tlburtt_tlburda::dt_riferimento     AS  dt_riferimento     				
											,tlbcidef_tlburtt_tlburda::conto_esteso      AS  conto_esteso     				
											,tlbcidef_tlburtt_tlburda::forma_tecnica      AS  forma_tecnica      				
											,tlbcidef_tlburtt_tlburda::dt_accensione      AS  dt_accensione     				
											,tlbcidef_tlburtt_tlburda::dt_estinzione      AS  dt_estinzione     				
											,tlbcidef_tlburtt_tlburda::dt_scadenza      AS  dt_scadenza     				
											,tlbcidef_tlburtt_tlburda::tp_ammortamento     AS  tp_ammortamento    				
											,tlbcidef_tlburtt_tlburda::tp_rapporto      AS  tp_rapporto     				
											,tlbcidef_tlburtt_tlburda::period_liquid      AS  period_liquid     				
											,tlbcidef_tlburtt_tlburda::cd_prodotto_ris     AS  cd_prodotto_ris    				
											,tlbcidef_tlburtt_tlburda::durata_originaria     AS  durata_originaria   				
											,tlbcidef_tlburtt_tlburda::divisa       AS  divisa       				
											,tlbcidef_tlburtt_tlburda::durata_residua     AS  durata_residua     				
											,tlbcidef_tlburtt_tlburda::tp_contr_rapp      AS  tp_contr_rapp    				
											,tlbcidef_tlburtt_tlburda::margine_fido_rev    AS  margine_fido_rev    				
											,tlbcidef_tlburtt_tlburda::margine_fido_irrev    AS  margine_fido_irrev    				
											,tlbcidef_tlburtt_tlburda::utilizz_cassa_firm    AS  utilizz_cassa_firm    				
											,tlbcidef_tlburtt_tlburda::imp_accordato     AS  imp_accordato     				
											,tlbcidef_tlburtt_tlburda::flag_ristrutt     AS  flag_ristrutt     				
											,tlbcidef_tlburtt_tlburda::imp_accor_ope     AS  imp_accor_ope     				
											,tlbcidef_tlburtt_tlburda::imp_accor_non_ope    AS  imp_accor_non_ope    				
											,tlbcidef_tlburtt_tlburda::gg_sconfino     AS  gg_sconfino     				
											,tlbcidef_tlburtt_tlburda::fido_n_ope_sbf_p_i    AS  fido_n_ope_sbf_p_i    				
											,tlbcidef_tlburtt_tlburda::fido_ope_sbf_p_ill    AS  fido_ope_sbf_p_ill    				
											,tlbcidef_tlburtt_tlburda::fido_ope_firma     AS  fido_ope_firma     				
											,tlbcidef_tlburtt_tlburda::fido_ope_cassa     AS  fido_ope_cassa     				
											,tlbcidef_tlburtt_tlburda::fido_non_oper_cas    AS  fido_non_oper_cas    				
											,tlbcidef_tlburtt_tlburda::fido_non_oper_fir    AS  fido_non_oper_fir    				
											,tlbcidef_tlburtt_tlburda::fido_n_ope_sbf_ced    AS  fido_n_ope_sbf_ced    				
											,tlbcidef_tlburtt_tlburda::fido_ope_sbf_ced    AS  fido_ope_sbf_ced    				
											,tlbcidef_tlburtt_tlburda::aut_sconf_cassa    AS  aut_sconf_cassa    				
											,tlbcidef_tlburtt_tlburda::aut_sconf_sbf     AS  aut_sconf_sbf     				
											,tlbcidef_tlburtt_tlburda::aut_sconf_firma    AS  aut_sconf_firma    				
											,tlbcidef_tlburtt_tlburda::gar_merci      AS  gar_merci      				
											,tlbcidef_tlburtt_tlburda::gar_ipoteca     AS  gar_ipoteca     				
											,tlbcidef_tlburtt_tlburda::gar_personale     AS  gar_personale     				
											,tlbcidef_tlburtt_tlburda::gar_altre      AS  gar_altre      				
											,tlbcidef_tlburtt_tlburda::gar_obbl_pr_emiss    AS  gar_obbl_pr_emiss    				
											,tlbcidef_tlburtt_tlburda::gar_obbl_b_centr    AS  gar_obbl_b_centr    				
											,tlbcidef_tlburtt_tlburda::gar_obbl_altre     AS  gar_obbl_altre     				
											,tlbcidef_tlburtt_tlburda::gar_dep_denaro     AS  gar_dep_denaro     				
											,tlbcidef_tlburtt_tlburda::gar_cd_pr_emiss    AS  gar_cd_pr_emiss    				
											,tlbcidef_tlburtt_tlburda::gar_altri_valori    AS  gar_altri_valori    				
											,tlbcidef_tlburtt_tlburda::valore_dossier     AS  valore_dossier     				
											,tlbcidef_tlburtt_tlburda::fido_deriv      AS  fido_deriv      				
											,tlbcidef_tlburtt_tlburda::saldo_cont      AS  saldo_cont      				
											,tlbcidef_tlburtt_tlburda::saldo_cont_dare    AS  saldo_cont_dare    				
											,tlbcidef_tlburtt_tlburda::saldo_cont_avere    AS  saldo_cont_avere    				
											,tlbcidef_tlburtt_tlburda::part_illiq_sbf     AS  part_illiq_sbf     				
											,tlbcidef_tlburtt_tlburda::ind_util_fido_med    AS  ind_util_fido_med    				
											,tlbcidef_tlburtt_tlburda::imp_fido_acc_firma    AS  imp_fido_acc_firma    				
											,tlbcidef_tlburtt_tlburda::imp_utilizzo_firma    AS  imp_utilizzo_firma    				
											,tlbcidef_tlburtt_tlburda::imp_gar_pers_cred    AS  imp_gar_pers_cred    				
											,tlbcidef_tlburtt_tlburda::imp_sal_lq_med_dar    AS  imp_sal_lq_med_dar    				
											,tlbcidef_tlburtt_tlburda::imp_utilizzo_cassa    AS  imp_utilizzo_cassa    				
											,tlbcidef_tlburtt_tlburda::imp_der_val_int_ps    AS  imp_der_val_int_ps    				
											,tlbcidef_tlburtt_tlburda::imp_der_val_int_ng    AS  imp_der_val_int_ng    				
											,tlbcidef_tlburtt_tlburda::gar_titoli_6998          AS  gar_titoli_6998     				    
											,tlbcidef_tlburtt_tlburda::gar_pers_6970            AS  gar_pers_6970       				  				
											,tlburat_filter::q_cap_rate_a_scad           AS  q_cap_rate_a_scad   
											,tlburat_filter::q_cap_rate_impag           AS  q_cap_rate_impag				
											,tlburat_filter::q_inter_rate_impag           AS  q_inter_rate_impag     
											,tlburat_filter::q_cap_rate_mora           AS  q_cap_rate_mora     
											,tlburat_filter::q_inter_rate_mora           AS  q_inter_rate_mora     
											,tlburat_filter::acconti_rate_scad           AS  acconti_rate_scad       
											,tlburat_filter::imp_orig_prestito           AS  imp_orig_prestito      
										;																	

--STORE tlbcidef_tlburtt_tlburda_tlburat INTO '/user/drlgd/datarequest/dt/tmp/debug/tlbcidef_tlburtt_tlburda_tlburat/$periodo' USING PigStorage(';');

/*carico la tabella TLBUCOC*/

tlbucoc_load = LOAD '$tlbucoc_path' 
               USING PigStorage(';')
					  AS (
							cd_istituto              :chararray
							,ndg                     :chararray
							,sportello               :chararray
							,conto                   :chararray
							,progr_segmento          :int
							,dt_riferimento          :chararray
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
							,numeri_fido_cc          :chararray
							,numeri_fido_sbf         :chararray
							,num_max_imm_d_as_v      :chararray
							,gg_saldo_liq_cred       :chararray
							,num_sald_disp_dare      :chararray
							,gg_saldo_disp_dare      :chararray
							,imp_mov_dare            :chararray
							,n_mov_avere             :chararray
							,imp_mov_avere           :chararray
							,mov_av_vers_ass         :chararray
							,mov_avere_storni        :chararray
							,mov_avere_comp          :chararray
							,numeri_creditori        :chararray
							,numeri_debitori         :chararray
							,num_ass_ind_vers        :chararray
							,num_sal_cont_dare       :chararray
							,num_sdo_liq_dare        :chararray
							,mov_dare_assegni        :chararray
							,gg_saldo_liq_deb        :chararray
							,mov_d_ass_prop_ins      :chararray
							,mov_d_ass_trz_ins       :chararray
							,num_sdo_liq_dare_2      :chararray
							,utilizzo_cr_cc          :chararray
							,num_deb_util_sbf        :chararray
							,num_debi_util_cc        :chararray
							,tot_garanzie_reali      :chararray
							,imp_accordato           :chararray
							,utilizz_cassa_firm      :chararray
							,gg_di_vita_rapp         :chararray
							,margine_fido_irrev      :chararray
							,margine_fido_rev        :chararray
							,num_bon_accr_it         :chararray
							,imp_bon_accr_it         :chararray
							,num_bon_accr_est        :chararray
							,imp_bon_accr_est        :chararray
							,flag_mass_util_sbf      :chararray
							,eff_ins_in_sosp         :chararray
							,eff_ins_sbf_al_pr       :chararray
							,ef_ins_sbf_n_a_c_o      :chararray
							,ef_ins_sbf_s_d_c_c      :chararray
							,ef_ins_d_i_accr_in      :chararray
							,eff_ins_d_i_altri       :chararray
							,eff_ins_sc_altri        :chararray
							,eff_ins_sc_al_pr        :chararray
							,imp_ass_pr_m_aut        :chararray
							,imp_ass_pr_m_fondi      :chararray
							,imp_as_i_m_a_n_p_f      :chararray
							,imp_as_i_m_f_n_p_f      :chararray
							,imp_as_i_m_a_n_p_p      :chararray
							,imp_as_i_m_f_n_p_p      :chararray
							,data_revoca_az          :chararray
							,num_ass_pr_m_aut        :chararray
							,num_as_pr_m_f           :chararray
							,num_as_i_m_a_n_p_f      :chararray
							,num_as_i_m_f_n_p_f      :chararray
							,num_as_i_m_a_n_p        :chararray
							,num_as_i_m_f_n          :chararray
							,cd_prodotto_rischi      :chararray
							,imp_mov_avere_su        :chararray
							,gg_sconfino_tu          :chararray
							,n_mov_avere_tu          :chararray
							,saldo_cont_dare_su      :chararray
							,fido_n_ope_sbf_ced      :chararray
							,fido_medio              :chararray
							,fido_medio_sbf          :chararray
							,ind_util_fido_med       :chararray
							,imp_sal_lq_med_dar      :chararray
							,imp_utilizzo_cassa      :chararray
							,gg_periodo              :chararray
							,gg_sconfino             :chararray
							);

tlbucoc = FOREACH tlbucoc_load				
          GENERATE
              progr_segmento
             ,cd_istituto
             ,dt_riferimento
             ,ndg
             ,sportello
             ,conto
             ,addebiti_in_sosp             
          	;
          				
/*faccio il filtro alla tabella TLBUCOC*/

tlbucoc_filter = FILTER tlbucoc BY progr_segmento==0;

/*faccio la JOIN con la tabella TLBCIDE_TLBURTT_TLBURDA_TLBURAT e TLBUCOC*/

tlbcidef_tlburtt_tlburda_talburat_tlbucoc_filter_join = JOIN tlbcidef_tlburtt_tlburda_tlburat BY (codicebanca_collegato, ndg_collegato, sportello, conto, dt_riferimento) LEFT,
                                                             tlbucoc_filter BY (cd_istituto, ndg, sportello, conto, dt_riferimento)
                                                             ; -- using 'replicated';

/*
tlbcidef_tlburtt_tlburda_talburat_tlbucoc_filter_join_filt3 = FILTER tlbcidef_tlburtt_tlburda_talburat_tlbucoc_filter_join 
 BY ToDate(tlbucoc_filter::dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate(datainiziodef,'yyyyMMdd'),'$numero_mesi_1')  
and ToDate(tlbucoc_filter::dt_riferimento,'yyyyMMdd') < AddDuration(ToDate(datafinedef,'yyyyMMdd'),'$numero_mesi_2');
*/

frapp_out = FOREACH tlbcidef_tlburtt_tlburda_talburat_tlbucoc_filter_join
			GENERATE
					 tlbcidef_tlburtt_tlburda_tlburat::codicebanca_collegato      AS codicebanca_collegato
					,tlbcidef_tlburtt_tlburda_tlburat::ndg_collegato              AS  ndg_collegato 
					,tlbcidef_tlburtt_tlburda_tlburat::sportello                 AS   sportello
					,tlbcidef_tlburtt_tlburda_tlburat::conto                     AS   conto
					,tlbcidef_tlburtt_tlburda_tlburat::dt_riferimento            AS   datariferimento
					,tlbcidef_tlburtt_tlburda_tlburat::conto_esteso              AS   contoesteso
					,tlbcidef_tlburtt_tlburda_tlburat::forma_tecnica             AS   formatecnica	
					,tlbcidef_tlburtt_tlburda_tlburat::dt_accensione             AS   dataaccensione
					,tlbcidef_tlburtt_tlburda_tlburat::dt_estinzione             AS   dataestinzione 
					,tlbcidef_tlburtt_tlburda_tlburat::dt_scadenza               AS   datascadenza
					,tlbcidef_tlburtt_tlburda_tlburat::tp_ammortamento           AS   r046tpammortam
					,tlbcidef_tlburtt_tlburda_tlburat::tp_rapporto               AS   r071tprapporto
					,tlbcidef_tlburtt_tlburda_tlburat::period_liquid             AS   r209periodliquid
					,tlbcidef_tlburtt_tlburda_tlburat::cd_prodotto_ris           AS   cdprodottorischio
					,tlbcidef_tlburtt_tlburda_tlburat::durata_originaria         AS   durataoriginaria
					,tlbcidef_tlburtt_tlburda_tlburat::divisa                    AS   r004divisa 
					,tlbcidef_tlburtt_tlburda_tlburat::durata_residua            AS   duarataresidua
					,tlbcidef_tlburtt_tlburda_tlburat::tp_contr_rapp             AS   r239tpcontrrapp
					,tlbcidef_tlburtt_tlburda_tlburat::margine_fido_rev          AS   d2240marginefidorev
					,tlbcidef_tlburtt_tlburda_tlburat::margine_fido_irrev        AS   d239marginefidoirrev
					,tlbcidef_tlburtt_tlburda_tlburat::utilizz_cassa_firm        AS   d1788utilizzaccafirm
					,tlbcidef_tlburtt_tlburda_tlburat::imp_accordato             AS   d1780impaccordato
					,tlbcidef_tlburtt_tlburda_tlburat::flag_ristrutt             AS   r025flagristrutt
					,tlbcidef_tlburtt_tlburda_tlburat::imp_accor_ope             AS   d9322impaccorope
					,tlbcidef_tlburtt_tlburda_tlburat::imp_accor_non_ope         AS   d9323impaaccornonope
					,tlbcidef_tlburtt_tlburda_tlburat::gg_sconfino               AS   d6464ggsconfino
					,tlbcidef_tlburtt_tlburda_tlburat::fido_n_ope_sbf_p_i        AS   d0018fidonopesbfpi
					,tlbcidef_tlburtt_tlburda_tlburat::fido_ope_sbf_p_ill        AS   d0019fidopesbfpill
					,tlbcidef_tlburtt_tlburda_tlburat::fido_ope_firma            AS   d0623fidoopefirma
					,tlbcidef_tlburtt_tlburda_tlburat::fido_ope_cassa            AS   d0009fidodpecassa
					,tlbcidef_tlburtt_tlburda_tlburat::fido_non_oper_cas         AS   d0008fidononopercas
					,tlbcidef_tlburtt_tlburda_tlburat::fido_non_oper_fir         AS   d0622fidononoperfir
					,tlbcidef_tlburtt_tlburda_tlburat::fido_n_ope_sbf_ced        AS   d0058fidonopesbfced
					,tlbcidef_tlburtt_tlburda_tlburat::fido_ope_sbf_ced          AS   d0059fidoopesbfced
					,tlbcidef_tlburtt_tlburda_tlburat::aut_sconf_cassa           AS   d0007autosconfcassa
					,tlbcidef_tlburtt_tlburda_tlburat::aut_sconf_sbf             AS   d0017autosconfsbf
					,tlbcidef_tlburtt_tlburda_tlburat::aut_sconf_firma           AS   d0621autosconffirma
					,tlbcidef_tlburtt_tlburda_tlburat::gar_merci                 AS   d0967gamerci
					,tlbcidef_tlburtt_tlburda_tlburat::gar_ipoteca               AS   d0968garipoteca
					,tlbcidef_tlburtt_tlburda_tlburat::gar_personale             AS   d0970garpersonale
					,tlbcidef_tlburtt_tlburda_tlburat::gar_altre                 AS   d0971garaltre
					,tlbcidef_tlburtt_tlburda_tlburat::gar_obbl_pr_emiss         AS   d0985garobblpremiss
					,tlbcidef_tlburtt_tlburda_tlburat::gar_obbl_b_centr          AS   d0986garobblbcentr
					,tlbcidef_tlburtt_tlburda_tlburat::gar_obbl_altre            AS   d0987garobblaltre
					,tlbcidef_tlburtt_tlburda_tlburat::gar_dep_denaro            AS   d0988gardepdenaro
					,tlbcidef_tlburtt_tlburda_tlburat::gar_cd_pr_emiss           AS   d0989garcdpremiss
					,tlbcidef_tlburtt_tlburda_tlburat::gar_altri_valori          AS   d0990garaltrivalori
					,tlbcidef_tlburtt_tlburda_tlburat::valore_dossier            AS   d0260valoredossier
					,tlbcidef_tlburtt_tlburda_tlburat::fido_deriv                AS   d0021fidoderiv
					,tlbcidef_tlburtt_tlburda_tlburat::saldo_cont                AS   d0061saldocont
					,tlbcidef_tlburtt_tlburda_tlburat::saldo_cont_dare           AS   d0062aldocontdare
					,tlbcidef_tlburtt_tlburda_tlburat::saldo_cont_avere          AS   d0063saldocontavere
					,tlbcidef_tlburtt_tlburda_tlburat::part_illiq_sbf            AS   d0064partilliqsbf
					,tlbcidef_tlburtt_tlburda_tlburat::ind_util_fido_med         AS   d0088indutilfidomed
					,tlbcidef_tlburtt_tlburda_tlburat::imp_fido_acc_firma        AS   d0624impfidoaccfirma
					,tlbcidef_tlburtt_tlburda_tlburat::imp_utilizzo_firma        AS   d0652imputilizzofirma
					,tlbcidef_tlburtt_tlburda_tlburat::imp_gar_pers_cred         AS   d0982impgarperscred
					,tlbcidef_tlburtt_tlburda_tlburat::imp_sal_lq_med_dar        AS   d1509impsallqmeddar
					,tlbcidef_tlburtt_tlburda_tlburat::imp_utilizzo_cassa        AS   d1785imputilizzocassa
					,tlbcidef_tlburtt_tlburda_tlburat::imp_der_val_int_ps        AS   d0462impdervalintps
					,tlbcidef_tlburtt_tlburda_tlburat::imp_der_val_int_ng        AS   d0475impdervalintng
					,tlbcidef_tlburtt_tlburda_tlburat::q_cap_rate_a_scad         AS   qcaprateascad
					,tlbcidef_tlburtt_tlburda_tlburat::q_cap_rate_impag          AS   qcaprateimpag
					,tlbcidef_tlburtt_tlburda_tlburat::q_inter_rate_impag        AS   qintrateimpag
					,tlbcidef_tlburtt_tlburda_tlburat::q_cap_rate_mora           AS   qcapratemora
					,tlbcidef_tlburtt_tlburda_tlburat::q_inter_rate_mora         AS   qintratemora
					,tlbcidef_tlburtt_tlburda_tlburat::acconti_rate_scad         AS   accontiratescad
					,tlbcidef_tlburtt_tlburda_tlburat::imp_orig_prestito         AS   imporigprestito
					,tlbcidef_tlburtt_tlburda_tlburat::gar_titoli_6998           AS   d6998
					,tlbcidef_tlburtt_tlburda_tlburat::gar_pers_6970             AS   d6970
					,tlbucoc_filter::addebiti_in_sosp                            AS   d0075addebitiinsosp
					,tlbcidef_tlburtt_tlburda_tlburat::codicebanca               AS   codicebanca
					,tlbcidef_tlburtt_tlburda_tlburat::ndgprincipale             AS   ndgprincipale
					,tlbcidef_tlburtt_tlburda_tlburat::datainiziodef             AS   datainiziodef
					;												
					
STORE frapp_out INTO '$frapp_outdir/$periodo' USING PigStorage(';');
