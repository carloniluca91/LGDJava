/*

$LastChangedDate: 2017-03-03 16:11:27 +0100 (ven, 03 mar 2017) $
$Rev: 616 $		
$Author: arzelenko $	

Sono coinvolti i seguenti parametri:
		-cicli_ndg_path
		-tlbpratsoff_path
		-data_osservazione
		-soff_outdir
			
*/

outcracc_load = LOAD '$ciclilav_step1_outcracc'
      USING PigStorage(';') 
  	  AS ( cd_isti: chararray
		   ,ndg_principale: chararray
		   ,dt_inizio_ciclo: chararray
		   ,dt_fine_ciclo: chararray
		   ,cd_isti_ced: chararray
		   ,ndg_ced: chararray
		   ,dt_rif_cracc: chararray
        );
        
outcracc = FOREACH outcracc_load 
           GENERATE 
              cd_isti
             ,ndg_principale
             ,dt_inizio_ciclo
             ,dt_fine_ciclo
             ,cd_isti_ced
             ,ndg_ced
             ,SUBSTRING(dt_rif_cracc,0,6) as dt_rif_cracc
;

/*
carico il file di cicli
*/
tlbcidef = LOAD '$cicli_ndg_path'
      USING PigStorage(';') 
  	  AS (
			codicebanca: chararray
		   ,ndgprincipale: chararray
		   ,datainiziodef: int
		   ,datafinedef: int
		   ,datainiziopd: chararray
		   ,datainizioristrutt: chararray
		   ,datainizioinc: chararray
		   ,datainiziosoff: chararray 
		   ,c_key: chararray
		   ,tipo_segmne: chararray
		   ,sae_segm: chararray
		   ,rae_segm: chararray
		   ,segmento: chararray
		   ,tp_ndg: chararray
		   ,provincia_segm: chararray
		   ,databilseg: chararray
		   ,strbilseg: chararray
		   ,attivobilseg: chararray
		   ,fatturbilseg: chararray
           ,ndg_collegato : chararray
		   ,codicebanca_collegato : chararray
		   ,cd_collegamento: chararray
		   ,cd_fiscale: chararray
         );

                      
tlbcidef_group = DISTINCT  (FOREACH tlbcidef 
		                      GENERATE 
		                          codicebanca_collegato
		                         ,ndg_collegato
		                         ,codicebanca
		                         ,ndgprincipale
		                         ,datainiziodef
		                         ,datafinedef
		                      );                          
    
/*
carico la tabella PRATSOFF
*/

--tlbpratsoff = LOAD '$tlbpratsoff_path'
tlbpratsoff_filter = LOAD '$tlbpratsoff_path'
			USING PigStorage (';')
			AS (
				 pr_dt_riferimento                     :int
				 ,pr_istituto                          :chararray
				 ,pr_ndg                               :chararray
				 ,pr_tipo                              :chararray
				 ,pr_dt_inizio                         :int
				 ,pr_dt_riapertura                     :chararray
				 ,pr_dt_chiusura                       :chararray
				 ,pr_stato_finale                      :chararray
				 ,pr_num_soff                          :chararray
				 ,pr_saldo_posiz                       :chararray
				 ,pr_saldo_posiz_contab                :chararray
				 ,pr_num_mov_tipo_imp01                :chararray
				 ,pr_tot_mov_tipo_imp01                :chararray
				 ,pr_num_mov_tipo_imp02                :chararray
				 ,pr_tot_mov_tipo_imp02                :chararray
				 ,pr_num_mov_tipo_imp03                :chararray
				 ,pr_tot_mov_tipo_imp03                :chararray
				 ,pr_num_mov_tipo_imp04                :chararray
				 ,pr_tot_mov_tipo_imp04                :chararray
				 ,pr_num_mov_tipo_imp05                :chararray
				 ,pr_tot_mov_tipo_imp05                :chararray
				 ,pr_num_mov_tipo_imp06                :chararray
				 ,pr_tot_mov_tipo_imp06                :chararray
				 ,pr_num_mov_tipo_imp07                :chararray
				 ,pr_tot_mov_tipo_imp07                :chararray
				 ,pr_num_mov_tipo_imp08                :chararray
				 ,pr_tot_mov_tipo_imp08                :chararray
				 ,pr_num_mov_tipo_imp09                :chararray
				 ,pr_tot_mov_tipo_imp09                :chararray
				 ,pr_num_mov_tipo_imp10                :chararray
				 ,pr_tot_mov_tipo_imp10                :chararray
				 ,pr_num_mov_tipo_imp11                :chararray
				 ,pr_tot_mov_tipo_imp11                :chararray
				 ,pr_num_mov_tipo_imp12                :chararray
				 ,pr_tot_mov_tipo_imp12                :chararray
				 ,pr_num_mov_tipo_imp13                :chararray
				 ,pr_tot_mov_tipo_imp13                :chararray
				 ,pr_tipoerrore                        :chararray
				 ,pr_saldo_svalutazioni_sal90          :chararray
				 ,pr_flagtipoquadradura                :chararray
				 ,pr_istituto_orig                     :chararray
				 ,pr_ndg_orig                          :chararray
				);

/*filtro la tabella di input in base alla data di osservazione*/
--tlbpratsoff_filter = FILTER tlbpratsoff BY pr_dt_inizio <= $data_osservazione AND pr_dt_riferimento == $data_osservazione;

/*unisco le due tabelle TLBCIDEF_FILTER e PRATSOFF*/
--tlbcidef_tlbpratsoff_filter_join = JOIN tlbpratsoff_filter BY (pr_istituto, pr_ndg) LEFT, tlbcidef_group BY (codicebanca_collegato, ndg_collegato);
tlbcidef_tlbpratsoff_filter_join = JOIN tlbpratsoff_filter BY (pr_istituto, pr_ndg) LEFT, tlbcidef_group BY (codicebanca, ndgprincipale);

soff_between = FILTER tlbcidef_tlbpratsoff_filter_join 
 BY (int)SUBSTRING(tlbpratsoff_filter::pr_dt_riapertura,0,6) >= (int)SUBSTRING((chararray)tlbcidef_group::datainiziodef,0,6)  
AND (int)SUBSTRING(tlbpratsoff_filter::pr_dt_riapertura,0,6) < (int)SUBSTRING((chararray)tlbcidef_group::datafinedef,0,6)
;

/*ottengo la mappatura finale*/
soff_between_out = FOREACH soff_between
			GENERATE
					 tlbpratsoff_filter::pr_istituto               as istituto														
					,tlbpratsoff_filter::pr_ndg                    as ndg												
					,tlbpratsoff_filter::pr_num_soff               as numerosofferenza													
					,tlbpratsoff_filter::pr_dt_riapertura          as datainizio												
					,tlbpratsoff_filter::pr_dt_chiusura            as datafine												
					,tlbpratsoff_filter::pr_stato_finale      	   as statopratica 
					,tlbpratsoff_filter::pr_saldo_posiz            as saldoposizione
					,tlbpratsoff_filter::pr_saldo_posiz_contab     as saldoposizionecontab
					,tlbpratsoff_filter::pr_num_mov_tipo_imp01     as nummovimtipo01
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp01     as totmovimtipo01
					,tlbpratsoff_filter::pr_num_mov_tipo_imp02     as nummovimtipo02
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp02     as totmovimtipo02
					,tlbpratsoff_filter::pr_num_mov_tipo_imp03     as nummovimtipo03
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp03     as totmovimtipo03
					,tlbpratsoff_filter::pr_num_mov_tipo_imp04     as nummovimtipo04
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp04     as totmovimtipo04
					,tlbpratsoff_filter::pr_num_mov_tipo_imp05     as nummovimtipo05
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp05     as totmovimtipo05
					,tlbpratsoff_filter::pr_num_mov_tipo_imp06     as nummovimtipo06
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp06     as totmovimtipo06
					,tlbpratsoff_filter::pr_num_mov_tipo_imp07     as nummovimtipo07
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp07     as totmovimtipo07
					,tlbpratsoff_filter::pr_num_mov_tipo_imp08     as nummovimtipo08
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp08     as totmovimtipo08
					,tlbpratsoff_filter::pr_num_mov_tipo_imp09     as nummovimtipo09
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp09     as totmovimtipo09
					,tlbpratsoff_filter::pr_num_mov_tipo_imp10     as nummovimtipo10
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp10     as totmovimtipo10
					,tlbpratsoff_filter::pr_num_mov_tipo_imp11     as nummovimtipo11
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp11     as totmovimtipo11
					,tlbpratsoff_filter::pr_tipoerrore             as tipoerrore
					,tlbpratsoff_filter::pr_flagtipoquadradura     as flagtipoquadradura
					,tlbcidef_group::codicebanca                   as  codicebanca
					,tlbcidef_group::ndgprincipale                 as ndgprincipale
					,tlbcidef_group::datainiziodef                 as datainiziodef
                   ; 
                   
--STORE soff_between_out INTO '/app/drlgd/datarequest/dt/tmp/debug/soff_between_out' USING PigStorage (';');              

/*tutti quelli con cicli agganciati per ndg*/
soff_other = FILTER tlbcidef_tlbpratsoff_filter_join 
 BY tlbcidef_group::codicebanca IS NOT NULL
;

/*ottengo la mappatura */
soff_other_gen = FOREACH soff_other
			GENERATE
					 tlbpratsoff_filter::pr_istituto               as istituto														
					,tlbpratsoff_filter::pr_ndg                    as ndg												
					,tlbpratsoff_filter::pr_num_soff               as numerosofferenza													
					,tlbpratsoff_filter::pr_dt_riapertura          as datainizio												
					,tlbpratsoff_filter::pr_dt_chiusura            as datafine												
					,tlbpratsoff_filter::pr_stato_finale      	   as statopratica 
					,tlbpratsoff_filter::pr_saldo_posiz            as saldoposizione
					,tlbpratsoff_filter::pr_saldo_posiz_contab     as saldoposizionecontab
					,tlbpratsoff_filter::pr_num_mov_tipo_imp01     as nummovimtipo01
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp01     as totmovimtipo01
					,tlbpratsoff_filter::pr_num_mov_tipo_imp02     as nummovimtipo02
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp02     as totmovimtipo02
					,tlbpratsoff_filter::pr_num_mov_tipo_imp03     as nummovimtipo03
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp03     as totmovimtipo03
					,tlbpratsoff_filter::pr_num_mov_tipo_imp04     as nummovimtipo04
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp04     as totmovimtipo04
					,tlbpratsoff_filter::pr_num_mov_tipo_imp05     as nummovimtipo05
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp05     as totmovimtipo05
					,tlbpratsoff_filter::pr_num_mov_tipo_imp06     as nummovimtipo06
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp06     as totmovimtipo06
					,tlbpratsoff_filter::pr_num_mov_tipo_imp07     as nummovimtipo07
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp07     as totmovimtipo07
					,tlbpratsoff_filter::pr_num_mov_tipo_imp08     as nummovimtipo08
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp08     as totmovimtipo08
					,tlbpratsoff_filter::pr_num_mov_tipo_imp09     as nummovimtipo09
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp09     as totmovimtipo09
					,tlbpratsoff_filter::pr_num_mov_tipo_imp10     as nummovimtipo10
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp10     as totmovimtipo10
					,tlbpratsoff_filter::pr_num_mov_tipo_imp11     as nummovimtipo11
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp11     as totmovimtipo11
					,tlbpratsoff_filter::pr_tipoerrore             as tipoerrore
					,tlbpratsoff_filter::pr_flagtipoquadradura     as flagtipoquadradura
					,NULL                                          as codicebanca
					,NULL                                          as ndgprincipale
					,NULL                                          as datainiziodef
                   ; 
                   
--STORE soff_other_gen INTO '/app/drlgd/datarequest/dt/tmp/debug/soff_other_gen' USING PigStorage (';');
                   
/*rijoin per trovare quelli completamente fuori ciclo*/
soff_other_gen_between_join = JOIN soff_other_gen BY (istituto, ndg, numerosofferenza, datainizio) LEFT, soff_between_out BY (istituto, ndg, numerosofferenza, datainizio);

/*prendo solo quelli senza record dentro ciclo*/ 
soff_other_filter = FILTER soff_other_gen_between_join 
 BY soff_between_out::istituto IS NULL
;

/*ottengo la mappatura */
soff_other_out = FOREACH soff_other_filter
			GENERATE
					 soff_other_gen::istituto              as istituto														
					,soff_other_gen::ndg                   as ndg												
					,soff_other_gen::numerosofferenza      as numerosofferenza													
					,soff_other_gen::datainizio            as datainizio												
					,soff_other_gen::datafine              as datafine												
					,soff_other_gen::statopratica      	   as statopratica 
					,soff_other_gen::saldoposizione        as saldoposizione
					,soff_other_gen::saldoposizionecontab  as saldoposizionecontab
					,soff_other_gen::nummovimtipo01        as nummovimtipo01
					,soff_other_gen::totmovimtipo01        as totmovimtipo01
					,soff_other_gen::nummovimtipo02        as nummovimtipo02
					,soff_other_gen::totmovimtipo02        as totmovimtipo02
					,soff_other_gen::nummovimtipo03        as nummovimtipo03
					,soff_other_gen::totmovimtipo03        as totmovimtipo03
					,soff_other_gen::nummovimtipo04        as nummovimtipo04
					,soff_other_gen::totmovimtipo04        as totmovimtipo04
					,soff_other_gen::nummovimtipo05        as nummovimtipo05
					,soff_other_gen::totmovimtipo05        as totmovimtipo05
					,soff_other_gen::nummovimtipo06        as nummovimtipo06
					,soff_other_gen::totmovimtipo06        as totmovimtipo06
					,soff_other_gen::nummovimtipo07        as nummovimtipo07
					,soff_other_gen::totmovimtipo07        as totmovimtipo07
					,soff_other_gen::nummovimtipo08        as nummovimtipo08
					,soff_other_gen::totmovimtipo08        as totmovimtipo08
					,soff_other_gen::nummovimtipo09        as nummovimtipo09
					,soff_other_gen::totmovimtipo09        as totmovimtipo09
					,soff_other_gen::nummovimtipo10        as nummovimtipo10
					,soff_other_gen::totmovimtipo10        as totmovimtipo10
					,soff_other_gen::nummovimtipo11        as nummovimtipo11
					,soff_other_gen::totmovimtipo11        as totmovimtipo11
					,soff_other_gen::tipoerrore            as tipoerrore
					,soff_other_gen::flagtipoquadradura    as flagtipoquadradura
					,soff_other_gen::codicebanca           as codicebanca
					,soff_other_gen::ndgprincipale         as ndgprincipale
					,soff_other_gen::datainiziodef         as datainiziodef
                   ; 

--STORE soff_other_out INTO '/app/drlgd/datarequest/dt/tmp/debug/soff_other_out' USING PigStorage (';');

/*prendo quelli senza cicli agganciati*/
soff_null = FILTER tlbcidef_tlbpratsoff_filter_join 
 BY tlbcidef_group::codicebanca IS NULL
;

/*ottengo la mappatura finale*/
soff_null_out_null = FOREACH soff_null
			GENERATE
					 tlbpratsoff_filter::pr_istituto               as istituto														
					,tlbpratsoff_filter::pr_ndg                    as ndg												
					,tlbpratsoff_filter::pr_num_soff               as numerosofferenza													
					,tlbpratsoff_filter::pr_dt_riapertura          as datainizio												
					,tlbpratsoff_filter::pr_dt_chiusura            as datafine												
					,tlbpratsoff_filter::pr_stato_finale      	   as statopratica 
					,tlbpratsoff_filter::pr_saldo_posiz            as saldoposizione
					,tlbpratsoff_filter::pr_saldo_posiz_contab     as saldoposizionecontab
					,tlbpratsoff_filter::pr_num_mov_tipo_imp01     as nummovimtipo01
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp01     as totmovimtipo01
					,tlbpratsoff_filter::pr_num_mov_tipo_imp02     as nummovimtipo02
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp02     as totmovimtipo02
					,tlbpratsoff_filter::pr_num_mov_tipo_imp03     as nummovimtipo03
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp03     as totmovimtipo03
					,tlbpratsoff_filter::pr_num_mov_tipo_imp04     as nummovimtipo04
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp04     as totmovimtipo04
					,tlbpratsoff_filter::pr_num_mov_tipo_imp05     as nummovimtipo05
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp05     as totmovimtipo05
					,tlbpratsoff_filter::pr_num_mov_tipo_imp06     as nummovimtipo06
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp06     as totmovimtipo06
					,tlbpratsoff_filter::pr_num_mov_tipo_imp07     as nummovimtipo07
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp07     as totmovimtipo07
					,tlbpratsoff_filter::pr_num_mov_tipo_imp08     as nummovimtipo08
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp08     as totmovimtipo08
					,tlbpratsoff_filter::pr_num_mov_tipo_imp09     as nummovimtipo09
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp09     as totmovimtipo09
					,tlbpratsoff_filter::pr_num_mov_tipo_imp10     as nummovimtipo10
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp10     as totmovimtipo10
					,tlbpratsoff_filter::pr_num_mov_tipo_imp11     as nummovimtipo11
					,tlbpratsoff_filter::pr_tot_mov_tipo_imp11     as totmovimtipo11
					,tlbpratsoff_filter::pr_tipoerrore             as tipoerrore
					,tlbpratsoff_filter::pr_flagtipoquadradura     as flagtipoquadradura
					,NULL                                          as codicebanca
					,NULL                                          as ndgprincipale
					,NULL                                          as datainiziodef
                   ;
                   
--STORE soff_null_out_null INTO '/app/drlgd/datarequest/dt/tmp/debug/soff_null_out_null' USING PigStorage (';');

soff_null_out = UNION soff_null_out_null
                     ,soff_other_out
                ;

-----------------------------------------------------
/*riprovo la join per ndg principale tra cidef e sofferenze senza ciclo
qui fino alla union finale tutta la parte precedente ripetuta per ndg principale
*/
--soff_null_out_cidef_princip_join = JOIN soff_null_out BY (istituto, ndg) LEFT, tlbcidef_group BY (codicebanca, ndgprincipale);
soff_null_out_cidef_princip_join = JOIN soff_null_out BY (istituto, ndg) LEFT, tlbcidef_group BY (codicebanca_collegato, ndg_collegato);

soff_between_princip = FILTER soff_null_out_cidef_princip_join 
 BY (int)SUBSTRING(soff_null_out::datainizio,0,6) >= (int)SUBSTRING((chararray)tlbcidef_group::datainiziodef,0,6)  
AND (int)SUBSTRING(soff_null_out::datainizio,0,6) < (int)SUBSTRING((chararray)tlbcidef_group::datafinedef,0,6)
;

/*ottengo la mappatura finale*/
soff_between_out_princip = FOREACH soff_between_princip
			GENERATE
					 soff_null_out::istituto               as istituto														
					,soff_null_out::ndg                    as ndg												
					,soff_null_out::numerosofferenza       as numerosofferenza													
					,soff_null_out::datainizio             as datainizio												
					,soff_null_out::datafine               as datafine												
					,soff_null_out::statopratica      	   as statopratica 
					,soff_null_out::saldoposizione         as saldoposizione
					,soff_null_out::saldoposizionecontab   as saldoposizionecontab
					,soff_null_out::nummovimtipo01     as nummovimtipo01
					,soff_null_out::totmovimtipo01     as totmovimtipo01
					,soff_null_out::nummovimtipo02     as nummovimtipo02
					,soff_null_out::totmovimtipo02     as totmovimtipo02
					,soff_null_out::nummovimtipo03     as nummovimtipo03
					,soff_null_out::totmovimtipo03     as totmovimtipo03
					,soff_null_out::nummovimtipo04     as nummovimtipo04
					,soff_null_out::totmovimtipo04     as totmovimtipo04
					,soff_null_out::nummovimtipo05     as nummovimtipo05
					,soff_null_out::totmovimtipo05     as totmovimtipo05
					,soff_null_out::nummovimtipo06     as nummovimtipo06
					,soff_null_out::totmovimtipo06     as totmovimtipo06
					,soff_null_out::nummovimtipo07     as nummovimtipo07
					,soff_null_out::totmovimtipo07     as totmovimtipo07
					,soff_null_out::nummovimtipo08     as nummovimtipo08
					,soff_null_out::totmovimtipo08     as totmovimtipo08
					,soff_null_out::nummovimtipo09     as nummovimtipo09
					,soff_null_out::totmovimtipo09     as totmovimtipo09
					,soff_null_out::nummovimtipo10     as nummovimtipo10
					,soff_null_out::totmovimtipo10     as totmovimtipo10
					,soff_null_out::nummovimtipo11     as nummovimtipo11
					,soff_null_out::totmovimtipo11     as totmovimtipo11
					,soff_null_out::tipoerrore             as tipoerrore
					,soff_null_out::flagtipoquadradura     as flagtipoquadradura
					,tlbcidef_group::codicebanca           as  codicebanca
					,tlbcidef_group::ndgprincipale         as ndgprincipale
					,tlbcidef_group::datainiziodef         as datainiziodef
                   ;
                   
--STORE soff_between_out_princip INTO '/app/drlgd/datarequest/dt/tmp/debug/soff_between_out_princip' USING PigStorage (';');
                   
/*tutti quelli con cicli agganciati per ndg*/
soff_other_princip = FILTER soff_null_out_cidef_princip_join 
 BY tlbcidef_group::codicebanca IS NOT NULL
;

/*ottengo la mappatura */
soff_other_princip_gen = FOREACH soff_other_princip
			GENERATE
					 soff_null_out::istituto               as istituto														
					,soff_null_out::ndg                    as ndg												
					,soff_null_out::numerosofferenza       as numerosofferenza													
					,soff_null_out::datainizio             as datainizio												
					,soff_null_out::datafine               as datafine												
					,soff_null_out::statopratica      	   as statopratica 
					,soff_null_out::saldoposizione         as saldoposizione
					,soff_null_out::saldoposizionecontab   as saldoposizionecontab
					,soff_null_out::nummovimtipo01     as nummovimtipo01
					,soff_null_out::totmovimtipo01     as totmovimtipo01
					,soff_null_out::nummovimtipo02     as nummovimtipo02
					,soff_null_out::totmovimtipo02     as totmovimtipo02
					,soff_null_out::nummovimtipo03     as nummovimtipo03
					,soff_null_out::totmovimtipo03     as totmovimtipo03
					,soff_null_out::nummovimtipo04     as nummovimtipo04
					,soff_null_out::totmovimtipo04     as totmovimtipo04
					,soff_null_out::nummovimtipo05     as nummovimtipo05
					,soff_null_out::totmovimtipo05     as totmovimtipo05
					,soff_null_out::nummovimtipo06     as nummovimtipo06
					,soff_null_out::totmovimtipo06     as totmovimtipo06
					,soff_null_out::nummovimtipo07     as nummovimtipo07
					,soff_null_out::totmovimtipo07     as totmovimtipo07
					,soff_null_out::nummovimtipo08     as nummovimtipo08
					,soff_null_out::totmovimtipo08     as totmovimtipo08
					,soff_null_out::nummovimtipo09     as nummovimtipo09
					,soff_null_out::totmovimtipo09     as totmovimtipo09
					,soff_null_out::nummovimtipo10     as nummovimtipo10
					,soff_null_out::totmovimtipo10     as totmovimtipo10
					,soff_null_out::nummovimtipo11     as nummovimtipo11
					,soff_null_out::totmovimtipo11     as totmovimtipo11
					,soff_null_out::tipoerrore             as tipoerrore
					,soff_null_out::flagtipoquadradura     as flagtipoquadradura
					,NULL                                  as  codicebanca
					,NULL                                  as ndgprincipale
					,NULL                                  as datainiziodef
                   ; 
                   
--STORE soff_other_princip_gen INTO '/app/drlgd/datarequest/dt/tmp/debug/soff_other_princip_gen' USING PigStorage (';');
                   
/*rijoin per trovare quelli completamente fuori ciclo*/
soff_other_princip_gen_between_princip_join = JOIN soff_other_princip_gen BY (istituto, ndg, numerosofferenza, datainizio) LEFT, soff_between_out_princip BY (istituto, ndg, numerosofferenza, datainizio);

/*prendo solo quelli senza record dentro ciclo*/ 
soff_other_princip_filter = FILTER soff_other_princip_gen_between_princip_join 
 BY soff_between_out_princip::istituto IS NULL
;

/*ottengo la mappatura */
soff_other_princip_out = FOREACH soff_other_princip_filter
			GENERATE
					 soff_other_princip_gen::istituto              as istituto														
					,soff_other_princip_gen::ndg                   as ndg												
					,soff_other_princip_gen::numerosofferenza      as numerosofferenza													
					,soff_other_princip_gen::datainizio            as datainizio												
					,soff_other_princip_gen::datafine              as datafine												
					,soff_other_princip_gen::statopratica      	   as statopratica 
					,soff_other_princip_gen::saldoposizione        as saldoposizione
					,soff_other_princip_gen::saldoposizionecontab  as saldoposizionecontab
					,soff_other_princip_gen::nummovimtipo01        as nummovimtipo01
					,soff_other_princip_gen::totmovimtipo01        as totmovimtipo01
					,soff_other_princip_gen::nummovimtipo02        as nummovimtipo02
					,soff_other_princip_gen::totmovimtipo02        as totmovimtipo02
					,soff_other_princip_gen::nummovimtipo03        as nummovimtipo03
					,soff_other_princip_gen::totmovimtipo03        as totmovimtipo03
					,soff_other_princip_gen::nummovimtipo04        as nummovimtipo04
					,soff_other_princip_gen::totmovimtipo04        as totmovimtipo04
					,soff_other_princip_gen::nummovimtipo05        as nummovimtipo05
					,soff_other_princip_gen::totmovimtipo05        as totmovimtipo05
					,soff_other_princip_gen::nummovimtipo06        as nummovimtipo06
					,soff_other_princip_gen::totmovimtipo06        as totmovimtipo06
					,soff_other_princip_gen::nummovimtipo07        as nummovimtipo07
					,soff_other_princip_gen::totmovimtipo07        as totmovimtipo07
					,soff_other_princip_gen::nummovimtipo08        as nummovimtipo08
					,soff_other_princip_gen::totmovimtipo08        as totmovimtipo08
					,soff_other_princip_gen::nummovimtipo09        as nummovimtipo09
					,soff_other_princip_gen::totmovimtipo09        as totmovimtipo09
					,soff_other_princip_gen::nummovimtipo10        as nummovimtipo10
					,soff_other_princip_gen::totmovimtipo10        as totmovimtipo10
					,soff_other_princip_gen::nummovimtipo11        as nummovimtipo11
					,soff_other_princip_gen::totmovimtipo11        as totmovimtipo11
					,soff_other_princip_gen::tipoerrore            as tipoerrore
					,soff_other_princip_gen::flagtipoquadradura    as flagtipoquadradura
					,soff_other_princip_gen::codicebanca           as codicebanca
					,soff_other_princip_gen::ndgprincipale         as ndgprincipale
					,soff_other_princip_gen::datainiziodef         as datainiziodef
                   ; 
                   
--STORE soff_other_princip_out INTO '/app/drlgd/datarequest/dt/tmp/debug/soff_other_princip_out' USING PigStorage (';');

/*prendo quelli senza cicli agganciati*/
soff_null_princip = FILTER soff_null_out_cidef_princip_join 
 BY tlbcidef_group::codicebanca IS NULL
;

/*ottengo la mappatura finale*/
soff_null_princip_out = FOREACH soff_null_princip
			GENERATE
					 soff_null_out::istituto               as istituto														
					,soff_null_out::ndg                    as ndg												
					,soff_null_out::numerosofferenza       as numerosofferenza													
					,soff_null_out::datainizio             as datainizio												
					,soff_null_out::datafine               as datafine												
					,soff_null_out::statopratica      	   as statopratica 
					,soff_null_out::saldoposizione         as saldoposizione
					,soff_null_out::saldoposizionecontab   as saldoposizionecontab
					,soff_null_out::nummovimtipo01     as nummovimtipo01
					,soff_null_out::totmovimtipo01     as totmovimtipo01
					,soff_null_out::nummovimtipo02     as nummovimtipo02
					,soff_null_out::totmovimtipo02     as totmovimtipo02
					,soff_null_out::nummovimtipo03     as nummovimtipo03
					,soff_null_out::totmovimtipo03     as totmovimtipo03
					,soff_null_out::nummovimtipo04     as nummovimtipo04
					,soff_null_out::totmovimtipo04     as totmovimtipo04
					,soff_null_out::nummovimtipo05     as nummovimtipo05
					,soff_null_out::totmovimtipo05     as totmovimtipo05
					,soff_null_out::nummovimtipo06     as nummovimtipo06
					,soff_null_out::totmovimtipo06     as totmovimtipo06
					,soff_null_out::nummovimtipo07     as nummovimtipo07
					,soff_null_out::totmovimtipo07     as totmovimtipo07
					,soff_null_out::nummovimtipo08     as nummovimtipo08
					,soff_null_out::totmovimtipo08     as totmovimtipo08
					,soff_null_out::nummovimtipo09     as nummovimtipo09
					,soff_null_out::totmovimtipo09     as totmovimtipo09
					,soff_null_out::nummovimtipo10     as nummovimtipo10
					,soff_null_out::totmovimtipo10     as totmovimtipo10
					,soff_null_out::nummovimtipo11     as nummovimtipo11
					,soff_null_out::totmovimtipo11     as totmovimtipo11
					,soff_null_out::tipoerrore             as tipoerrore
					,soff_null_out::flagtipoquadradura     as flagtipoquadradura
					,NULL                                  as  codicebanca
					,NULL                                  as ndgprincipale
					,NULL                                  as datainiziodef
                   ;
                   
--STORE soff_null_princip_out INTO '/app/drlgd/datarequest/dt/tmp/debug/soff_null_princip_out' USING PigStorage (';');

-----------------------------------------------------
soff_out = UNION soff_between_out
                --,soff_other_out
                ,soff_between_out_princip
                ,soff_other_princip_out
                ,soff_null_princip_out
                ;
                
soff_out_dist = DISTINCT ( 
                          FOREACH soff_out                          
				          GENERATE 			                                          
						     istituto 
							,ndg 
							,numerosofferenza 
							,datainizio 
							,datafine 
							,statopratica 
							,saldoposizione 
							,saldoposizionecontab 
							,nummovimtipo01
							,totmovimtipo01
							,nummovimtipo02
							,totmovimtipo02
							,nummovimtipo03
							,totmovimtipo03
							,nummovimtipo04
							,totmovimtipo04
							,nummovimtipo05
							,totmovimtipo05
							,nummovimtipo06
							,totmovimtipo06
							,nummovimtipo07
							,totmovimtipo07
							,nummovimtipo08
							,totmovimtipo08
							,nummovimtipo09
							,totmovimtipo09
							,nummovimtipo10
							,totmovimtipo10
							,nummovimtipo11
							,totmovimtipo11
							,tipoerrore
							,flagtipoquadradura
							,codicebanca
							,ndgprincipale
							,datainiziodef)
						    ;
						    
/*
carico la tabella PRATSOFF alla data di osservazione
*/

tlbpratsoffoss = LOAD '$tlbpratsoffOsserv_path'
			USING PigStorage (';')
			AS (
				  pr_dt_riferimento                    :int
				 ,pr_istituto                          :chararray
				 ,pr_ndg                               :chararray
				 ,pr_tipo                              :chararray
				 ,pr_dt_inizio                         :int
				 ,pr_dt_riapertura                     :chararray
				 ,pr_dt_chiusura                       :chararray
				 ,pr_stato_finale                      :chararray
				 ,pr_num_soff                          :chararray
				 ,pr_saldo_posiz                       :chararray
				 ,pr_saldo_posiz_contab                :chararray
				 ,pr_num_mov_tipo_imp01                :chararray
				 ,pr_tot_mov_tipo_imp01                :chararray
				 ,pr_num_mov_tipo_imp02                :chararray
				 ,pr_tot_mov_tipo_imp02                :chararray
				 ,pr_num_mov_tipo_imp03                :chararray
				 ,pr_tot_mov_tipo_imp03                :chararray
				 ,pr_num_mov_tipo_imp04                :chararray
				 ,pr_tot_mov_tipo_imp04                :chararray
				 ,pr_num_mov_tipo_imp05                :chararray
				 ,pr_tot_mov_tipo_imp05                :chararray
				 ,pr_num_mov_tipo_imp06                :chararray
				 ,pr_tot_mov_tipo_imp06                :chararray
				 ,pr_num_mov_tipo_imp07                :chararray
				 ,pr_tot_mov_tipo_imp07                :chararray
				 ,pr_num_mov_tipo_imp08                :chararray
				 ,pr_tot_mov_tipo_imp08                :chararray
				 ,pr_num_mov_tipo_imp09                :chararray
				 ,pr_tot_mov_tipo_imp09                :chararray
				 ,pr_num_mov_tipo_imp10                :chararray
				 ,pr_tot_mov_tipo_imp10                :chararray
				 ,pr_num_mov_tipo_imp11                :chararray
				 ,pr_tot_mov_tipo_imp11                :chararray
				 ,pr_num_mov_tipo_imp12                :chararray
				 ,pr_tot_mov_tipo_imp12                :chararray
				 ,pr_num_mov_tipo_imp13                :chararray
				 ,pr_tot_mov_tipo_imp13                :chararray
				 ,pr_tipoerrore                        :chararray
				 ,pr_saldo_svalutazioni_sal90          :chararray
				 ,pr_flagtipoquadradura                :chararray
				 ,pr_istituto_orig                     :chararray
				 ,pr_ndg_orig                          :chararray
				);

soff_soffoss_join = JOIN soff_out_dist BY (istituto, numerosofferenza, datainizio) FULL OUTER, 
                         tlbpratsoffoss BY (pr_istituto, pr_num_soff, pr_dt_riapertura);
                         
/*ottengo la mappatura finale dove
1. se esiste solo soff tengo soff
2. se esistono entrambi prendo soffoss tranne chiave ciclo
3. se esiste solo soffoss tengo soffoss e chiave ciclo nulla
*/
soff_soffoss_gen = DISTINCT ( 
            FOREACH soff_soffoss_join
			GENERATE
	         ( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_istituto : soff_out_dist::istituto ) : tlbpratsoffoss::pr_istituto ) as istituto 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_ndg : soff_out_dist::ndg ) : tlbpratsoffoss::pr_ndg ) as ndg 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_soff : soff_out_dist::numerosofferenza ) : tlbpratsoffoss::pr_num_soff ) as numerosofferenza 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_dt_riapertura : soff_out_dist::datainizio ) : tlbpratsoffoss::pr_dt_riapertura ) as datainizio 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_dt_chiusura : soff_out_dist::datafine ) : tlbpratsoffoss::pr_dt_chiusura ) as datafine 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_stato_finale : soff_out_dist::statopratica ) : tlbpratsoffoss::pr_stato_finale ) as statopratica 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_saldo_posiz : soff_out_dist::saldoposizione ) : tlbpratsoffoss::pr_saldo_posiz ) as saldoposizione 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_saldo_posiz_contab : soff_out_dist::saldoposizionecontab ) : tlbpratsoffoss::pr_saldo_posiz_contab ) as saldoposizionecontab 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp01 : soff_out_dist::nummovimtipo01 ) : tlbpratsoffoss::pr_num_mov_tipo_imp01 ) as nummovimtipo01
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp01 : soff_out_dist::totmovimtipo01 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp01 ) as totmovimtipo01
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp02 : soff_out_dist::nummovimtipo02 ) : tlbpratsoffoss::pr_num_mov_tipo_imp02 ) as nummovimtipo02
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp02 : soff_out_dist::totmovimtipo02 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp02 ) as totmovimtipo02
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp03 : soff_out_dist::nummovimtipo03 ) : tlbpratsoffoss::pr_num_mov_tipo_imp03 ) as nummovimtipo03
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp03 : soff_out_dist::totmovimtipo03 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp03 ) as totmovimtipo03
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp04 : soff_out_dist::nummovimtipo04 ) : tlbpratsoffoss::pr_num_mov_tipo_imp04 ) as nummovimtipo04
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp04 : soff_out_dist::totmovimtipo04 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp04 ) as totmovimtipo04
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp05 : soff_out_dist::nummovimtipo05 ) : tlbpratsoffoss::pr_num_mov_tipo_imp05 ) as nummovimtipo05
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp05 : soff_out_dist::totmovimtipo05 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp05 ) as totmovimtipo05
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp06 : soff_out_dist::nummovimtipo06 ) : tlbpratsoffoss::pr_num_mov_tipo_imp06 ) as nummovimtipo06
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp06 : soff_out_dist::totmovimtipo06 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp06 ) as totmovimtipo06
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp07 : soff_out_dist::nummovimtipo07 ) : tlbpratsoffoss::pr_num_mov_tipo_imp07 ) as nummovimtipo07
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp07 : soff_out_dist::totmovimtipo07 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp07 ) as totmovimtipo07
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp08 : soff_out_dist::nummovimtipo08 ) : tlbpratsoffoss::pr_num_mov_tipo_imp08 ) as nummovimtipo08
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp08 : soff_out_dist::totmovimtipo08 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp08 ) as totmovimtipo08
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp09 : soff_out_dist::nummovimtipo09 ) : tlbpratsoffoss::pr_num_mov_tipo_imp09 ) as nummovimtipo09
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp09 : soff_out_dist::totmovimtipo09 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp09 ) as totmovimtipo09
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp10 : soff_out_dist::nummovimtipo10 ) : tlbpratsoffoss::pr_num_mov_tipo_imp10 ) as nummovimtipo10
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp10 : soff_out_dist::totmovimtipo10 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp10 ) as totmovimtipo10
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp11 : soff_out_dist::nummovimtipo11 ) : tlbpratsoffoss::pr_num_mov_tipo_imp11 ) as nummovimtipo11
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp11 : soff_out_dist::totmovimtipo11 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp11 ) as totmovimtipo11
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tipoerrore : soff_out_dist::tipoerrore ) : tlbpratsoffoss::pr_tipoerrore ) as tipoerrore
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_flagtipoquadradura : soff_out_dist::flagtipoquadradura ) : tlbpratsoffoss::pr_flagtipoquadradura ) as flagtipoquadradura
			,( soff_out_dist::istituto is not null? soff_out_dist::codicebanca : NULL ) as codicebanca
			,( soff_out_dist::istituto is not null? soff_out_dist::ndgprincipale : NULL ) as ndgprincipale
			,( soff_out_dist::istituto is not null? soff_out_dist::datainiziodef : NULL ) as datainiziodef
			)
            ;
            
soff_soffoss_gen2 = DISTINCT ( 
            FOREACH soff_soffoss_join
			GENERATE
	         ( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_istituto : soff_out_dist::istituto ) : tlbpratsoffoss::pr_istituto ) as istituto 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_ndg : soff_out_dist::ndg ) : tlbpratsoffoss::pr_ndg ) as ndg 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_soff : soff_out_dist::numerosofferenza ) : tlbpratsoffoss::pr_num_soff ) as numerosofferenza 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_dt_riapertura : soff_out_dist::datainizio ) : tlbpratsoffoss::pr_dt_riapertura ) as datainizio 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_dt_chiusura : soff_out_dist::datafine ) : tlbpratsoffoss::pr_dt_chiusura ) as datafine 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_stato_finale : soff_out_dist::statopratica ) : tlbpratsoffoss::pr_stato_finale ) as statopratica 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_saldo_posiz : soff_out_dist::saldoposizione ) : tlbpratsoffoss::pr_saldo_posiz ) as saldoposizione 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_saldo_posiz_contab : soff_out_dist::saldoposizionecontab ) : tlbpratsoffoss::pr_saldo_posiz_contab ) as saldoposizionecontab 
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp01 : soff_out_dist::nummovimtipo01 ) : tlbpratsoffoss::pr_num_mov_tipo_imp01 ) as nummovimtipo01
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp01 : soff_out_dist::totmovimtipo01 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp01 ) as totmovimtipo01
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp02 : soff_out_dist::nummovimtipo02 ) : tlbpratsoffoss::pr_num_mov_tipo_imp02 ) as nummovimtipo02
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp02 : soff_out_dist::totmovimtipo02 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp02 ) as totmovimtipo02
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp03 : soff_out_dist::nummovimtipo03 ) : tlbpratsoffoss::pr_num_mov_tipo_imp03 ) as nummovimtipo03
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp03 : soff_out_dist::totmovimtipo03 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp03 ) as totmovimtipo03
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp04 : soff_out_dist::nummovimtipo04 ) : tlbpratsoffoss::pr_num_mov_tipo_imp04 ) as nummovimtipo04
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp04 : soff_out_dist::totmovimtipo04 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp04 ) as totmovimtipo04
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp05 : soff_out_dist::nummovimtipo05 ) : tlbpratsoffoss::pr_num_mov_tipo_imp05 ) as nummovimtipo05
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp05 : soff_out_dist::totmovimtipo05 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp05 ) as totmovimtipo05
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp06 : soff_out_dist::nummovimtipo06 ) : tlbpratsoffoss::pr_num_mov_tipo_imp06 ) as nummovimtipo06
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp06 : soff_out_dist::totmovimtipo06 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp06 ) as totmovimtipo06
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp07 : soff_out_dist::nummovimtipo07 ) : tlbpratsoffoss::pr_num_mov_tipo_imp07 ) as nummovimtipo07
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp07 : soff_out_dist::totmovimtipo07 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp07 ) as totmovimtipo07
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp08 : soff_out_dist::nummovimtipo08 ) : tlbpratsoffoss::pr_num_mov_tipo_imp08 ) as nummovimtipo08
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp08 : soff_out_dist::totmovimtipo08 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp08 ) as totmovimtipo08
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp09 : soff_out_dist::nummovimtipo09 ) : tlbpratsoffoss::pr_num_mov_tipo_imp09 ) as nummovimtipo09
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp09 : soff_out_dist::totmovimtipo09 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp09 ) as totmovimtipo09
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp10 : soff_out_dist::nummovimtipo10 ) : tlbpratsoffoss::pr_num_mov_tipo_imp10 ) as nummovimtipo10
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp10 : soff_out_dist::totmovimtipo10 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp10 ) as totmovimtipo10
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_num_mov_tipo_imp11 : soff_out_dist::nummovimtipo11 ) : tlbpratsoffoss::pr_num_mov_tipo_imp11 ) as nummovimtipo11
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tot_mov_tipo_imp11 : soff_out_dist::totmovimtipo11 ) : tlbpratsoffoss::pr_tot_mov_tipo_imp11 ) as totmovimtipo11
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_tipoerrore : soff_out_dist::tipoerrore ) : tlbpratsoffoss::pr_tipoerrore ) as tipoerrore
			,( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_flagtipoquadradura : soff_out_dist::flagtipoquadradura ) : tlbpratsoffoss::pr_flagtipoquadradura ) as flagtipoquadradura
			,( soff_out_dist::istituto is not null? soff_out_dist::codicebanca : NULL ) as codicebanca
			,( soff_out_dist::istituto is not null? soff_out_dist::ndgprincipale : NULL ) as ndgprincipale
			,( soff_out_dist::istituto is not null? soff_out_dist::datainiziodef : NULL ) as datainiziodef
			,SUBSTRING((chararray)( (int)( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_dt_riapertura : soff_out_dist::datainizio ) : tlbpratsoffoss::pr_dt_riapertura ) <= 20150731 ? 20150731 : (int)( soff_out_dist::istituto is not null? ( tlbpratsoffoss::pr_istituto is not null? tlbpratsoffoss::pr_dt_riapertura : soff_out_dist::datainizio ) : tlbpratsoffoss::pr_dt_riapertura ) ),0,6) as datainizio_join
			)
            ;


--prima tentativo di filtrare sofferenze
---------------------------------------------------------------------------------------------

--separo records senza ciclo da quelli con ciclo
soff_soffoss_gen_null = FILTER soff_soffoss_gen2 BY codicebanca IS NULL 
;

soff_soffoss_gen_cracc_join = JOIN soff_soffoss_gen2     BY (codicebanca, ndgprincipale, istituto, ndg, datainizio_join ), 
                                   outcracc             BY (cd_isti, ndg_principale, cd_isti_ced, ndg_ced, dt_rif_cracc);

--generazione file con cicli filtrato per data cracc
soff_soffoss_gen_ciclo_out = DISTINCT (  
            FOREACH soff_soffoss_gen_cracc_join
			GENERATE
	         soff_soffoss_gen2::istituto              as istituto
			,soff_soffoss_gen2::ndg                   as ndg
			,soff_soffoss_gen2::numerosofferenza      as numerosofferenza
			,soff_soffoss_gen2::datainizio            as datainizio
			,soff_soffoss_gen2::datafine              as datafine
			,soff_soffoss_gen2::statopratica          as statopratica
			,soff_soffoss_gen2::saldoposizione        as saldoposizione
			,soff_soffoss_gen2::saldoposizionecontab  as saldoposizionecontab
			,soff_soffoss_gen2::nummovimtipo01 as nummovimtipo01
			,soff_soffoss_gen2::totmovimtipo01 as totmovimtipo01
			,soff_soffoss_gen2::nummovimtipo02 as nummovimtipo02
			,soff_soffoss_gen2::totmovimtipo02 as totmovimtipo02
			,soff_soffoss_gen2::nummovimtipo03 as nummovimtipo03
			,soff_soffoss_gen2::totmovimtipo03 as totmovimtipo03
			,soff_soffoss_gen2::nummovimtipo04 as nummovimtipo04
			,soff_soffoss_gen2::totmovimtipo04 as totmovimtipo04
			,soff_soffoss_gen2::nummovimtipo05 as nummovimtipo05
			,soff_soffoss_gen2::totmovimtipo05 as totmovimtipo05
			,soff_soffoss_gen2::nummovimtipo06 as nummovimtipo06
			,soff_soffoss_gen2::totmovimtipo06 as totmovimtipo06
			,soff_soffoss_gen2::nummovimtipo07 as nummovimtipo07
			,soff_soffoss_gen2::totmovimtipo07 as totmovimtipo07
			,soff_soffoss_gen2::nummovimtipo08 as nummovimtipo08
			,soff_soffoss_gen2::totmovimtipo08 as totmovimtipo08
			,soff_soffoss_gen2::nummovimtipo09 as nummovimtipo09
			,soff_soffoss_gen2::totmovimtipo09 as totmovimtipo09
			,soff_soffoss_gen2::nummovimtipo10 as nummovimtipo10
			,soff_soffoss_gen2::totmovimtipo10 as totmovimtipo10
			,soff_soffoss_gen2::nummovimtipo11 as nummovimtipo11
			,soff_soffoss_gen2::totmovimtipo11 as totmovimtipo11
			,soff_soffoss_gen2::tipoerrore     as tipoerrore
			,soff_soffoss_gen2::flagtipoquadradura as flagtipoquadradura
			,soff_soffoss_gen2::codicebanca        as codicebanca
			,soff_soffoss_gen2::ndgprincipale      as ndgprincipale
			,soff_soffoss_gen2::datainiziodef      as datainiziodef
			)
            ;

soff_out_filter = UNION soff_soffoss_gen_ciclo_out
                        ,soff_soffoss_gen_null
                        ;

--secondo tentativo di filtrare sofferenze
---------------------------------------------------------------------------------------------
--separo records senza ciclo da quelli con ciclo o quelli minori o uguali alla data impianto
soff_soffoss_gen_null2 = FILTER soff_soffoss_gen2 BY codicebanca IS NULL 
                                                   OR (int)datainizio <= 20150731
;

--separo records maggiori alla data impianto
soff_soffoss_gen_maggiore = FILTER soff_soffoss_gen2 BY (int)datainizio > 20150731
;

soff_soffoss_gen_cracc_join2 = JOIN soff_soffoss_gen_maggiore BY (codicebanca, ndgprincipale, istituto, ndg, datainizio_join ), 
                                    outcracc                  BY (cd_isti, ndg_principale, cd_isti_ced, ndg_ced, dt_rif_cracc);

--generazione file con cicli filtrato per data cracc
soff_soffoss_gen_ciclo_out2 = DISTINCT (  
            FOREACH soff_soffoss_gen_cracc_join2
			GENERATE
	         soff_soffoss_gen_maggiore::istituto              as istituto
			,soff_soffoss_gen_maggiore::ndg                   as ndg
			,soff_soffoss_gen_maggiore::numerosofferenza      as numerosofferenza
			,soff_soffoss_gen_maggiore::datainizio            as datainizio
			,soff_soffoss_gen_maggiore::datafine              as datafine
			,soff_soffoss_gen_maggiore::statopratica          as statopratica
			,soff_soffoss_gen_maggiore::saldoposizione        as saldoposizione
			,soff_soffoss_gen_maggiore::saldoposizionecontab  as saldoposizionecontab
			,soff_soffoss_gen_maggiore::nummovimtipo01 as nummovimtipo01
			,soff_soffoss_gen_maggiore::totmovimtipo01 as totmovimtipo01
			,soff_soffoss_gen_maggiore::nummovimtipo02 as nummovimtipo02
			,soff_soffoss_gen_maggiore::totmovimtipo02 as totmovimtipo02
			,soff_soffoss_gen_maggiore::nummovimtipo03 as nummovimtipo03
			,soff_soffoss_gen_maggiore::totmovimtipo03 as totmovimtipo03
			,soff_soffoss_gen_maggiore::nummovimtipo04 as nummovimtipo04
			,soff_soffoss_gen_maggiore::totmovimtipo04 as totmovimtipo04
			,soff_soffoss_gen_maggiore::nummovimtipo05 as nummovimtipo05
			,soff_soffoss_gen_maggiore::totmovimtipo05 as totmovimtipo05
			,soff_soffoss_gen_maggiore::nummovimtipo06 as nummovimtipo06
			,soff_soffoss_gen_maggiore::totmovimtipo06 as totmovimtipo06
			,soff_soffoss_gen_maggiore::nummovimtipo07 as nummovimtipo07
			,soff_soffoss_gen_maggiore::totmovimtipo07 as totmovimtipo07
			,soff_soffoss_gen_maggiore::nummovimtipo08 as nummovimtipo08
			,soff_soffoss_gen_maggiore::totmovimtipo08 as totmovimtipo08
			,soff_soffoss_gen_maggiore::nummovimtipo09 as nummovimtipo09
			,soff_soffoss_gen_maggiore::totmovimtipo09 as totmovimtipo09
			,soff_soffoss_gen_maggiore::nummovimtipo10 as nummovimtipo10
			,soff_soffoss_gen_maggiore::totmovimtipo10 as totmovimtipo10
			,soff_soffoss_gen_maggiore::nummovimtipo11 as nummovimtipo11
			,soff_soffoss_gen_maggiore::totmovimtipo11 as totmovimtipo11
			,soff_soffoss_gen_maggiore::tipoerrore     as tipoerrore
			,soff_soffoss_gen_maggiore::flagtipoquadradura as flagtipoquadradura
			,soff_soffoss_gen_maggiore::codicebanca        as codicebanca
			,soff_soffoss_gen_maggiore::ndgprincipale      as ndgprincipale
			,soff_soffoss_gen_maggiore::datainiziodef      as datainiziodef
			)
            ;

soff_out_filter2 = UNION soff_soffoss_gen_null2
                        ,soff_soffoss_gen_ciclo_out2
                        ;

---------------------------------------------------------------------------------------------

--generazione file di controllo dei doppi
soff_soffoss_ctrl_group = GROUP soff_soffoss_gen BY (istituto, numerosofferenza, datainizio);

soff_soffoss_ctrl = FOREACH soff_soffoss_ctrl_group 
                            GENERATE 
                             group.istituto          as istituto
                            ,group.numerosofferenza  as numerosofferenza
                            ,group.datainizio        as datainizio
                            ,COUNT(soff_soffoss_gen) as num_recs
;

soff_soffoss_ctrl_filter = FILTER soff_soffoss_ctrl BY num_recs > 1;

STORE soff_soffoss_gen INTO '$soff_outdir' USING PigStorage (';');

STORE soff_soffoss_ctrl_filter INTO '$soff_outctrl' USING PigStorage (';');

STORE soff_out_filter INTO '$soff_outfilter' USING PigStorage (';');

STORE soff_out_filter2 INTO '$soff_outfilter2' USING PigStorage (';');