/*
$LastChangedDate: 2017-01-03 11:30:55 +0100 (mar, 03 gen 2017) $
$Rev: 601 $		
$Author: arzelenko $	

Sono coinvolti i seguenti parametri:
		-tlbcdef_path
		-data_da
		-data_a
		-ciclilav_step1_outdir
		
*/		
	
/*
Questo script carica la tlbcidef e orizontalizza le righe in colonne 
calcolandosi le date dei vari stati.
*/

/*
Carico il file. 
*/
tlbcidef = LOAD '$tlbcdef_path' 
      USING PigStorage(';') 
      AS (
			 /*dt_riferimento: int eliminato */
			cd_isti : chararray
			,ndg_principale: chararray
			,cod_cr: chararray
			,dt_inizio_ciclo: int
			,dt_ingresso_status: int
			,status_ingresso: chararray
			,dt_uscita_status: chararray
			,status_uscita: chararray
			,dt_fine_ciclo: chararray
			,indi_pastdue: chararray
			,indi_impr_priv: chararray
		);

/* Filtro le date. Prendo come data inizio  e come data fine 20151030 */		
tlbcidef_filtered = FILTER tlbcidef BY dt_inizio_ciclo >= $data_da AND dt_inizio_ciclo <= $data_a;

/*
Creo un tracciato con la chiave e i campi data con gli stati che devo considerare. Per lo stato PASTDUE considero anche il numero di mesi >12 (da rendere parametrico)
*/
tlbcidef_unpivot = 
				FOREACH tlbcidef_filtered 
                GENERATE cd_isti,
                         ndg_principale, 
                         dt_inizio_ciclo,
                         dt_fine_ciclo,
                         (TRIM(status_ingresso)=='PASTDUE'?dt_ingresso_status:null) as datainiziopd,
                         (TRIM(status_ingresso)=='INCA' or TRIM(status_ingresso)=='INADPRO'?dt_ingresso_status:null) as datainizioinc,
                         (TRIM(status_ingresso)=='RISTR'?dt_ingresso_status:null) as datainizioristrutt,
                         (TRIM(status_ingresso)=='SOFF'?dt_ingresso_status:null) as datainiziosoff
                         ;
                         
                         
                         
/*
Faccio la group by e poi la min delle 4 date
*/
tlbcidef_group = GROUP tlbcidef_unpivot BY (cd_isti,ndg_principale,dt_inizio_ciclo );
tlbcidef_max = FOREACH tlbcidef_group 
                GENERATE group.cd_isti as cd_isti
                        ,group.ndg_principale as ndg_principale
                        ,group.dt_inizio_ciclo as dt_inizio_ciclo
                        ,MAX(tlbcidef_unpivot.dt_fine_ciclo) as  dt_fine_ciclo
                        ,MIN(tlbcidef_unpivot.datainiziopd) as datainiziopd
                        ,MIN(tlbcidef_unpivot.datainizioristrutt) as datainizioristrutt
                        ,MIN(tlbcidef_unpivot.datainizioinc) as datainizioinc
                        ,MIN(tlbcidef_unpivot.datainiziosoff)  as datainiziosoff;

/*
Carico la TLBMIGNW		
Aggiungo il proressivo 0 
*/

tlbcracc_load = LOAD '$tlbcracc_path' 
      USING PigStorage(';') 
      AS (
			 data_rif     : int
			,cd_isti      : chararray
			,ndg          : chararray
			,cod_raccordo : chararray
			,data_val     : int
		);

/* Filtro le date. se data_a <= 20150731(mese impianto) allora <20150731 altrimenti <= data_a */		
--tlbcracc = FILTER tlbcracc_load BY ( (int)$data_a <= 20150731 ? data_rif == 20150731 : data_rif <= (int)$data_a ) ;
tlbcracc = FILTER tlbcracc_load BY data_rif <= ( (int)$data_a <= 20150731 ? 20150731 : (int)$data_a );

--STORE tlbcracc INTO '/app/drlgd/datarequest/dt/tmp/debug/tlbcracc' USING PigStorage(';');

/*
Join per cd_ist e ndg_principale sui campi cd_isti_ric e ndg_ric
*/                      
ciclilav_tlbmignw_join_1 = JOIN tlbcidef_max BY (cd_isti, ndg_principale) LEFT, tlbcracc BY (cd_isti, ndg);

cicli_racc_1 =  FOREACH ciclilav_tlbmignw_join_1 
                GENERATE tlbcidef_max::cd_isti             as cd_isti
                        ,tlbcidef_max::ndg_principale      as ndg_principale
                        ,tlbcidef_max::dt_inizio_ciclo     as dt_inizio_ciclo
                        ,tlbcidef_max::dt_fine_ciclo       as dt_fine_ciclo
                        ,tlbcidef_max::datainiziopd        as datainiziopd
                        ,tlbcidef_max::datainizioristrutt  as datainizioristrutt
                        ,tlbcidef_max::datainizioinc       as datainizioinc
                        ,tlbcidef_max::datainiziosoff      as datainiziosoff
                        ,tlbcracc::cod_raccordo            as cod_raccordo
                        ,tlbcracc::data_rif                as data_rif
                        ;

ciclilav_tlbmignw_join_2 = JOIN cicli_racc_1 BY (cod_raccordo, data_rif) LEFT, tlbcracc BY (cod_raccordo, data_rif);

/*
Scrivo il tracciato della ciclilav_out.
I campi cd_isti e ndg_principale sono popolati secondo una logica di precedenza.
Se i campi cd_isti_ric e ndg_ced sono popolati allora metto quelli altrimenti metto quelli della ciclilav. 
*/
ciclilav_step1_gen = FOREACH ciclilav_tlbmignw_join_2 
                     GENERATE  
                         cicli_racc_1::cd_isti as cd_isti
						,cicli_racc_1::ndg_principale as ndg_principale  
						,cicli_racc_1::dt_inizio_ciclo     
						,cicli_racc_1::dt_fine_ciclo       
						,cicli_racc_1::datainiziopd        
						,cicli_racc_1::datainizioristrutt  
						,cicli_racc_1::datainizioinc       
						,cicli_racc_1::datainiziosoff      
						/* tengo traccia dei progressivi. 0 */
						,0 as progr
						/* se trovo il record nella mignw prendo cd_isti_ced e ndg_ced da li altrimenti prendo quelli della ciclilav  */
						,(tlbcracc::cd_isti is not null ? tlbcracc::cd_isti : cicli_racc_1::cd_isti) as cd_isti_ced
						,(tlbcracc::ndg     is not null ? tlbcracc::ndg     : cicli_racc_1::ndg_principale) as ndg_ced      
                   ;

ciclilav_step1 = DISTINCT ( FOREACH ciclilav_step1_gen
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
							 ,cd_isti_ced
							 ,ndg_ced)
							 ;

-----------------------------------------------
-- generazione del file uguale a precedente piu' data_rif della cracc
-----------------------------------------------

ciclilav_step1_gen_filecracc = FOREACH ciclilav_tlbmignw_join_2 
                     GENERATE  
                         cicli_racc_1::cd_isti as cd_isti
						,cicli_racc_1::ndg_principale as ndg_principale  
						,cicli_racc_1::dt_inizio_ciclo     
						,cicli_racc_1::dt_fine_ciclo       
						/* se trovo il record nella mignw prendo cd_isti_ced e ndg_ced da li altrimenti prendo quelli della ciclilav  */
						,(tlbcracc::cd_isti  is not null ? tlbcracc::cd_isti  : cicli_racc_1::cd_isti) as cd_isti_ced
						,(tlbcracc::ndg      is not null ? tlbcracc::ndg      : cicli_racc_1::ndg_principale) as ndg_ced
						,(tlbcracc::data_rif is not null ? tlbcracc::data_rif : cicli_racc_1::dt_inizio_ciclo) as dt_rif_cracc
                   ;

ciclilav_step1_filecracc = DISTINCT ( FOREACH ciclilav_step1_gen_filecracc
                              GENERATE 
		                      cd_isti
				 	 		 ,ndg_principale     
							 ,dt_inizio_ciclo     
							 ,dt_fine_ciclo     
							 ,cd_isti_ced
							 ,ndg_ced
							 ,dt_rif_cracc)
							 ;

-----------------------------------------------

STORE ciclilav_step1 INTO '$ciclilav_step1_outdir' USING PigStorage(';');

STORE ciclilav_step1_filecracc INTO '$ciclilav_step1_outcracc' USING PigStorage(';');
