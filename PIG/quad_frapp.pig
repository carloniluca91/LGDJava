/*
$LastChangedDate: 2016-06-01 10:53:23 +0200 (mer, 01 giu 2016) $
$Rev: 389 $		
$Author: arzelenko $	

Sono coinvolti i seguenti parametri:
		-cicli_ndg_path
		-tlbpratsoff_path
		-data_osservazione
		-soff_outdir
				
*/

/*
carico il file fanag prodotto da hadoop
*/
hadoop_frapp = LOAD '/app/drlgd/datarequest/dt/out/finale/$ufficio/frapp'
      USING PigStorage(';') 
  	  AS (
         codicebanca               :chararray
		,ndg 			           :chararray
		,sportello		           :chararray
		,conto			           :chararray
		,datariferimento	       :chararray
		,contoesteso		       :chararray
		,formatecnica		       :chararray
		,dataaccensione		       :chararray
		,dataestinzione 	       :chararray
		,datascadenza		       :chararray
		,r046tpammortam		       :chararray
		,r071tprapporto		       :chararray
		,r209periodliquid	       :chararray
		,cdprodottorischio	       :chararray
		,durataoriginaria	       :chararray
		,r004divisa 		       :chararray
		,duarataresidua		       :chararray
		,r239tpcontrrapp	       :chararray
		,d2240marginefidorev	   :chararray
		,d239marginefidoirrev	   :chararray
		,d1788utilizzaccafirm	   :chararray
		,d1780impaccordato	       :chararray
		,r025flagristrutt	       :chararray
		,d9322impaccorope	       :chararray
		,d9323impaaccornonope	   :chararray
		,d6464ggsconfino	       :chararray
		,d0018fidonopesbfpi	       :chararray
		,d0019fidopesbfpill        :chararray
		,d0623fidoopefirma	       :chararray
		,d0009fidodpecassa	       :chararray
		,d0008fidononopercas	   :chararray
		,d0622fidononoperfir	   :chararray
		,d0058fidonopesbfced	   :chararray
		,d0059fidoopesbfced	       :chararray
		,d0007autosconfcassa	   :chararray
		,d0017autosconfsbf	       :chararray
		,d0621autosconffirma	   :chararray
		,d0967gamerci		       :chararray
		,d0968garipoteca	       :chararray
		,d0970garpersonale	       :chararray
		,d0971garaltre		       :chararray
		,d0985garobblpremiss	   :chararray
		,d0986garobblbcentr	       :chararray
		,d0987garobblaltre	       :chararray
		,d0988gardepdenaro	       :chararray
		,d0989garcdpremiss	       :chararray
		,d0990garaltrivalori	   :chararray
		,d0260valoredossier	       :chararray
		,d0021fidoderiv		       :chararray
		,d0061saldocont		       :chararray
		,d0062aldocontdare	       :chararray
		,d0063saldocontavere	   :chararray
		,d0064partilliqsbf	       :chararray
		,d0088indutilfidomed	   :chararray
		,d0624impfidoaccfirma      :chararray
		,d0652imputilizzofirma	   :chararray
		,d0982impgarperscred	   :chararray
		,d1509impsallqmeddar	   :chararray
		,d1785imputilizzocassa	   :chararray
		,d0462impdervalintps	   :chararray
		,d0475impdervalintng	   :chararray
		,qcaprateascad		       :chararray
		,qcaprateimpag		       :chararray
		,qintrateimpag		       :chararray
		,qcapratemora		       :chararray
		,qintratemora		       :chararray
		,accontiratescad	       :chararray
		,imporigprestito	       :chararray
		,d6998			           :chararray
		,d6970			           :chararray
		,d0075addebitiinsosp	   :chararray
		,codicebanca_princ         :chararray
		,ndgprincipale		       :chararray
		,datainiziodef		       :chararray
         );
   
     
/*
carico la tabella MOVCONTA
*/

oldfrapp_load = LOAD '/app/drlgd/store/oldfrapp'
			USING PigStorage (';')
			AS (
 CODICEBANCA                   :chararray
,NDG                           :chararray
,SPORTELLO                     :chararray
,CONTO                         :chararray
,PROGR_SEGMENTO                :chararray
,DT_RIFERIMENTO                :chararray
,CONTO_ESTESO                  :chararray
,FORMA_TECNICA                 :chararray
,DT_ACCENSIONE                 :chararray
,DT_ESTINZIONE                 :chararray
,DT_SCADENZA                   :chararray
,R046_TP_AMMORTAMENTO          :chararray
,R071_TP_RAPPORTO              :chararray
,R209_PERIOD_LIQUIDAZION       :chararray
,CD_PRODOTTO_RISCHI            :chararray
,DURATA_ORIGINARIA             :chararray
,R004_DIVISA                   :chararray
,DURATA_RESIDUA                :chararray
,R239_TP_CONTR_RAPP            :chararray
,D2240_MARGINE_FIDO_REV        :chararray
,D2239_MARGINE_FIDO_IRREV      :chararray
,D1781_UTILIZZ_CASSA_FIRM      :chararray
,D1780_IMP_ACCORDATO           :chararray
,R025_FLAG_RISTRUTT            :chararray
,D9322_IMP_ACCOR_OPE           :chararray
,D9323_IMP_ACCOR_NON_OPE       :chararray
,D6464_GG_SCONFINO             :chararray
,D0018_FIDO_N_OPE_SBF_P_I      :chararray
,D0019_FIDO_OPE_SBF_P_ILL      :chararray
,D0623_FIDO_OPE_FIRMA          :chararray
,D0009_FIDO_OPE_CASSA          :chararray
,D0008_FIDO_NON_OPER_CAS       :chararray
,D0622_FIDO_NON_OPER_FIR       :chararray
,D0058_FIDO_N_OPE_SBF_CED      :chararray
,D0059_FIDO_OPE_SBF_CED        :chararray
,D0007_AUT_SCONF_CASSA         :chararray
,D0017_AUT_SCONF_SBF           :chararray
,D0621_AUT_SCONF_FIRMA         :chararray
,D0967_GAR_MERCI               :chararray
,D0968_GAR_IPOTECA             :chararray
,D0970_GAR_PERSONALE           :chararray
,D0971_GAR_ALTRE               :chararray
,D0985_GAR_OBBL_PR_EMISS       :chararray
,D0986_GAR_OBBL_B_CENTR        :chararray
,D0987_GAR_OBBL_ALTRE          :chararray
,D0988_GAR_DEP_DENARO          :chararray
,D0989_GAR_CD_PR_EMISS         :chararray
,D0990_GAR_ALTRI_VALORI        :chararray
,D0260_VALORE_DOSSIER          :chararray
,D0021_FIDO_DERIV              :chararray
,D0061_SALDO_CONT              :chararray
,D0062_SALDO_CONT_DARE         :chararray
,D0063_SALDO_CONT_AVERE        :chararray
,D0064_PART_ILLIQ_SBF          :chararray
,D0088_IND_UTIL_FIDO_MED       :chararray
,D0624_IMP_FIDO_ACC_FIRMA      :chararray
,D0625_IMP_UTILIZZO_FIRMA      :chararray
,D0982_IMP_GAR_PERS_CRED       :chararray
,D1509_IMP_SAL_LQ_MED_DAR      :chararray
,D1785_IMP_UTILIZZO_CASSA      :chararray
,D0462_IMP_DER_VAL_INT_PS      :chararray
,D0475_IMP_DER_VAL_INT_NG      :chararray
,QCAPRATEASCAD                 :chararray
,QCAPRATEIMPAG                 :chararray
,QINTERRATEIMPAG               :chararray
,QCAPRATEMORA                  :chararray
,QINTERRATEMORA                :chararray
,ACCONTIRATESCAD               :chararray
,IMPORIGPRESTITO               :chararray
,CDFISC                        :chararray
,D6998_GAR_TITOLI              :chararray
,D6970_GAR_PERS                :chararray
,ADDEBITI_IN_SOSP              :chararray
				);

fcoll = LOAD '/app/drlgd/datarequest/dt/out/quad/$ufficio/fcoll'
			USING PigStorage (';')
			AS (
				 CODICEBANCA        :chararray
				,NDGPRINCIPALE	    :chararray
				,DATAINIZIODEF	    :chararray
				,DATAFINEDEF		:chararray
	            ,DATA_DEFAULT       :chararray
	            ,ISTITUTO_COLLEGATO :chararray
	            ,NDG_COLLEGATO      :chararray
	            ,DATA_COLLEGAMENTO  :chararray
	            ,CUMULO             :chararray
			   );

oldfrapp_load_fcoll_join = JOIN oldfrapp_load BY (CODICEBANCA, NDG), fcoll BY (ISTITUTO_COLLEGATO, NDG_COLLEGATO);

oldfrapp_load_fcoll_join_filt = FILTER oldfrapp_load_fcoll_join 
 BY ToDate(oldfrapp_load::DT_RIFERIMENTO,'yyyyMMdd') >= ToDate( fcoll::DATAINIZIODEF,'yyyyMMdd') 
AND ToDate(oldfrapp_load::DT_RIFERIMENTO,'yyyyMMdd') <= ToDate( fcoll::DATAFINEDEF,'yyyyMMdd'  )
;

oldfrapp = FOREACH oldfrapp_load_fcoll_join_filt
			     GENERATE
 oldfrapp_load::CODICEBANCA                   as CODICEBANCA             
,oldfrapp_load::NDG                           as NDG                     
,oldfrapp_load::SPORTELLO                     as SPORTELLO               
,oldfrapp_load::CONTO                         as CONTO                   
,oldfrapp_load::PROGR_SEGMENTO                as PROGR_SEGMENTO          
,oldfrapp_load::DT_RIFERIMENTO                as DT_RIFERIMENTO          
,oldfrapp_load::CONTO_ESTESO                  as CONTO_ESTESO            
,oldfrapp_load::FORMA_TECNICA                 as FORMA_TECNICA           
,oldfrapp_load::DT_ACCENSIONE                 as DT_ACCENSIONE           
,oldfrapp_load::DT_ESTINZIONE                 as DT_ESTINZIONE           
,oldfrapp_load::DT_SCADENZA                   as DT_SCADENZA             
,oldfrapp_load::R046_TP_AMMORTAMENTO          as R046_TP_AMMORTAMENTO    
,oldfrapp_load::R071_TP_RAPPORTO              as R071_TP_RAPPORTO        
,oldfrapp_load::R209_PERIOD_LIQUIDAZION       as R209_PERIOD_LIQUIDAZION 
,oldfrapp_load::CD_PRODOTTO_RISCHI            as CD_PRODOTTO_RISCHI      
,oldfrapp_load::DURATA_ORIGINARIA             as DURATA_ORIGINARIA       
,oldfrapp_load::R004_DIVISA                   as R004_DIVISA             
,oldfrapp_load::DURATA_RESIDUA                as DURATA_RESIDUA          
,oldfrapp_load::R239_TP_CONTR_RAPP            as R239_TP_CONTR_RAPP      
,oldfrapp_load::D2240_MARGINE_FIDO_REV        as D2240_MARGINE_FIDO_REV  
,oldfrapp_load::D2239_MARGINE_FIDO_IRREV      as D2239_MARGINE_FIDO_IRREV
,oldfrapp_load::D1781_UTILIZZ_CASSA_FIRM      as D1781_UTILIZZ_CASSA_FIRM
,oldfrapp_load::D1780_IMP_ACCORDATO           as D1780_IMP_ACCORDATO     
,oldfrapp_load::R025_FLAG_RISTRUTT            as R025_FLAG_RISTRUTT      
,oldfrapp_load::D9322_IMP_ACCOR_OPE           as D9322_IMP_ACCOR_OPE     
,oldfrapp_load::D9323_IMP_ACCOR_NON_OPE       as D9323_IMP_ACCOR_NON_OPE 
,oldfrapp_load::D6464_GG_SCONFINO             as D6464_GG_SCONFINO       
,oldfrapp_load::D0018_FIDO_N_OPE_SBF_P_I      as D0018_FIDO_N_OPE_SBF_P_I
,oldfrapp_load::D0019_FIDO_OPE_SBF_P_ILL      as D0019_FIDO_OPE_SBF_P_ILL
,oldfrapp_load::D0623_FIDO_OPE_FIRMA          as D0623_FIDO_OPE_FIRMA    
,oldfrapp_load::D0009_FIDO_OPE_CASSA          as D0009_FIDO_OPE_CASSA    
,oldfrapp_load::D0008_FIDO_NON_OPER_CAS       as D0008_FIDO_NON_OPER_CAS 
,oldfrapp_load::D0622_FIDO_NON_OPER_FIR       as D0622_FIDO_NON_OPER_FIR 
,oldfrapp_load::D0058_FIDO_N_OPE_SBF_CED      as D0058_FIDO_N_OPE_SBF_CED
,oldfrapp_load::D0059_FIDO_OPE_SBF_CED        as D0059_FIDO_OPE_SBF_CED  
,oldfrapp_load::D0007_AUT_SCONF_CASSA         as D0007_AUT_SCONF_CASSA   
,oldfrapp_load::D0017_AUT_SCONF_SBF           as D0017_AUT_SCONF_SBF     
,oldfrapp_load::D0621_AUT_SCONF_FIRMA         as D0621_AUT_SCONF_FIRMA   
,oldfrapp_load::D0967_GAR_MERCI               as D0967_GAR_MERCI         
,oldfrapp_load::D0968_GAR_IPOTECA             as D0968_GAR_IPOTECA       
,oldfrapp_load::D0970_GAR_PERSONALE           as D0970_GAR_PERSONALE     
,oldfrapp_load::D0971_GAR_ALTRE               as D0971_GAR_ALTRE         
,oldfrapp_load::D0985_GAR_OBBL_PR_EMISS       as D0985_GAR_OBBL_PR_EMISS 
,oldfrapp_load::D0986_GAR_OBBL_B_CENTR        as D0986_GAR_OBBL_B_CENTR  
,oldfrapp_load::D0987_GAR_OBBL_ALTRE          as D0987_GAR_OBBL_ALTRE    
,oldfrapp_load::D0988_GAR_DEP_DENARO          as D0988_GAR_DEP_DENARO    
,oldfrapp_load::D0989_GAR_CD_PR_EMISS         as D0989_GAR_CD_PR_EMISS   
,oldfrapp_load::D0990_GAR_ALTRI_VALORI        as D0990_GAR_ALTRI_VALORI  
,oldfrapp_load::D0260_VALORE_DOSSIER          as D0260_VALORE_DOSSIER    
,oldfrapp_load::D0021_FIDO_DERIV              as D0021_FIDO_DERIV        
,oldfrapp_load::D0061_SALDO_CONT              as D0061_SALDO_CONT        
,oldfrapp_load::D0062_SALDO_CONT_DARE         as D0062_SALDO_CONT_DARE   
,oldfrapp_load::D0063_SALDO_CONT_AVERE        as D0063_SALDO_CONT_AVERE  
,oldfrapp_load::D0064_PART_ILLIQ_SBF          as D0064_PART_ILLIQ_SBF    
,oldfrapp_load::D0088_IND_UTIL_FIDO_MED       as D0088_IND_UTIL_FIDO_MED 
,oldfrapp_load::D0624_IMP_FIDO_ACC_FIRMA      as D0624_IMP_FIDO_ACC_FIRMA
,oldfrapp_load::D0625_IMP_UTILIZZO_FIRMA      as D0625_IMP_UTILIZZO_FIRMA
,oldfrapp_load::D0982_IMP_GAR_PERS_CRED       as D0982_IMP_GAR_PERS_CRED 
,oldfrapp_load::D1509_IMP_SAL_LQ_MED_DAR      as D1509_IMP_SAL_LQ_MED_DAR
,oldfrapp_load::D1785_IMP_UTILIZZO_CASSA      as D1785_IMP_UTILIZZO_CASSA
,oldfrapp_load::D0462_IMP_DER_VAL_INT_PS      as D0462_IMP_DER_VAL_INT_PS
,oldfrapp_load::D0475_IMP_DER_VAL_INT_NG      as D0475_IMP_DER_VAL_INT_NG
,oldfrapp_load::QCAPRATEASCAD                 as QCAPRATEASCAD           
,oldfrapp_load::QCAPRATEIMPAG                 as QCAPRATEIMPAG           
,oldfrapp_load::QINTERRATEIMPAG               as QINTERRATEIMPAG         
,oldfrapp_load::QCAPRATEMORA                  as QCAPRATEMORA            
,oldfrapp_load::QINTERRATEMORA                as QINTERRATEMORA          
,oldfrapp_load::ACCONTIRATESCAD               as ACCONTIRATESCAD         
,oldfrapp_load::IMPORIGPRESTITO               as IMPORIGPRESTITO         
,oldfrapp_load::CDFISC                        as CDFISC                  
,oldfrapp_load::D6998_GAR_TITOLI              as D6998_GAR_TITOLI        
,oldfrapp_load::D6970_GAR_PERS                as D6970_GAR_PERS          
,oldfrapp_load::ADDEBITI_IN_SOSP              as ADDEBITI_IN_SOSP        
,fcoll::CODICEBANCA              as CODICEBANCA_PRINC
,fcoll::NDGPRINCIPALE            as NDGPRINCIPALE
,fcoll::DATAINIZIODEF            as DATAINIZIODEF
					;


/*unisco le due tabelle hadoop e old */

hadoop_frapp_oldfrapp_join = JOIN hadoop_frapp BY (codicebanca_princ, ndgprincipale, datainiziodef, codicebanca, ndg, sportello, conto, datariferimento) FULL OUTER, 
                                  oldfrapp BY (CODICEBANCA_PRINC, NDGPRINCIPALE, DATAINIZIODEF, CODICEBANCA, NDG, SPORTELLO, CONTO, DT_RIFERIMENTO);

solo_hadoop_frapp_filter = FILTER hadoop_frapp_oldfrapp_join BY oldfrapp::CODICEBANCA IS NULL;	

solo_oldfrapp_filter = FILTER hadoop_frapp_oldfrapp_join BY hadoop_frapp::codicebanca IS NULL;

/*
abbinati_filter = FILTER hadoop_frapp_oldfrapp_join BY hadoop_frapp::codicebanca IS NOT NULL AND oldfrapp::CODICEBANCA IS NOT NULL
AND hadoop_frapp::codicebanca != oldfrapp::CODICEBANCA
AND hadoop_frapp::ndg         != oldfrapp::NDG
AND hadoop_frapp::sportello   != oldfrapp::SPORTELLO
AND hadoop_frapp::conto       != oldfrapp::CONTO
;
*/

/*ottengo la mappatura finale*/

hadoop_frapp_out = FOREACH solo_hadoop_frapp_filter
			     GENERATE
'$ufficio'           as ufficio
,hadoop_frapp::codicebanca               
,hadoop_frapp::ndg 			   
,hadoop_frapp::sportello		   
,hadoop_frapp::conto			   
,hadoop_frapp::datariferimento	   
,hadoop_frapp::contoesteso		   
,hadoop_frapp::formatecnica		   
,hadoop_frapp::dataaccensione		   
,hadoop_frapp::dataestinzione 	   
,hadoop_frapp::datascadenza		   
,hadoop_frapp::r046tpammortam		   
,hadoop_frapp::r071tprapporto		   
,hadoop_frapp::r209periodliquid	   
,hadoop_frapp::cdprodottorischio	   
,hadoop_frapp::durataoriginaria	   
,hadoop_frapp::r004divisa 		   
,hadoop_frapp::duarataresidua		   
,hadoop_frapp::r239tpcontrrapp	   
,hadoop_frapp::d2240marginefidorev	   
,hadoop_frapp::d239marginefidoirrev	   
,hadoop_frapp::d1788utilizzaccafirm	   
,hadoop_frapp::d1780impaccordato	   
,hadoop_frapp::r025flagristrutt	   
,hadoop_frapp::d9322impaccorope	   
,hadoop_frapp::d9323impaaccornonope	   
,hadoop_frapp::d6464ggsconfino	   
,hadoop_frapp::d0018fidonopesbfpi	   
,hadoop_frapp::d0019fidopesbfpill        
,hadoop_frapp::d0623fidoopefirma	   
,hadoop_frapp::d0009fidodpecassa	   
,hadoop_frapp::d0008fidononopercas	   
,hadoop_frapp::d0622fidononoperfir	   
,hadoop_frapp::d0058fidonopesbfced	   
,hadoop_frapp::d0059fidoopesbfced	   
,hadoop_frapp::d0007autosconfcassa	   
,hadoop_frapp::d0017autosconfsbf	   
,hadoop_frapp::d0621autosconffirma	   
,hadoop_frapp::d0967gamerci		   
,hadoop_frapp::d0968garipoteca	   
,hadoop_frapp::d0970garpersonale	   
,hadoop_frapp::d0971garaltre		   
,hadoop_frapp::d0985garobblpremiss	   
,hadoop_frapp::d0986garobblbcentr	   
,hadoop_frapp::d0987garobblaltre	   
,hadoop_frapp::d0988gardepdenaro	   
,hadoop_frapp::d0989garcdpremiss	   
,hadoop_frapp::d0990garaltrivalori	   
,hadoop_frapp::d0260valoredossier	   
,hadoop_frapp::d0021fidoderiv		   
,hadoop_frapp::d0061saldocont		   
,hadoop_frapp::d0062aldocontdare	   
,hadoop_frapp::d0063saldocontavere	   
,hadoop_frapp::d0064partilliqsbf	   
,hadoop_frapp::d0088indutilfidomed	   
,hadoop_frapp::d0624impfidoaccfirma      
,hadoop_frapp::d0652imputilizzofirma	   
,hadoop_frapp::d0982impgarperscred	   
,hadoop_frapp::d1509impsallqmeddar	   
,hadoop_frapp::d1785imputilizzocassa	   
,hadoop_frapp::d0462impdervalintps	   
,hadoop_frapp::d0475impdervalintng	   
,hadoop_frapp::qcaprateascad		   
,hadoop_frapp::qcaprateimpag		   
,hadoop_frapp::qintrateimpag		   
,hadoop_frapp::qcapratemora		   
,hadoop_frapp::qintratemora		   
,hadoop_frapp::accontiratescad	   
,hadoop_frapp::imporigprestito	   
,hadoop_frapp::d6998			   
,hadoop_frapp::d6970			   
,hadoop_frapp::d0075addebitiinsosp	   
,hadoop_frapp::codicebanca_princ
,hadoop_frapp::ndgprincipale		   
,hadoop_frapp::datainiziodef		   
,oldfrapp::CODICEBANCA               
,oldfrapp::NDG                       
,oldfrapp::SPORTELLO                 
,oldfrapp::CONTO                     
,oldfrapp::PROGR_SEGMENTO            
,oldfrapp::DT_RIFERIMENTO            
,oldfrapp::CONTO_ESTESO              
,oldfrapp::FORMA_TECNICA             
,oldfrapp::DT_ACCENSIONE             
,oldfrapp::DT_ESTINZIONE             
,oldfrapp::DT_SCADENZA               
,oldfrapp::R046_TP_AMMORTAMENTO      
,oldfrapp::R071_TP_RAPPORTO          
,oldfrapp::R209_PERIOD_LIQUIDAZION   
,oldfrapp::CD_PRODOTTO_RISCHI        
,oldfrapp::DURATA_ORIGINARIA         
,oldfrapp::R004_DIVISA               
,oldfrapp::DURATA_RESIDUA            
,oldfrapp::R239_TP_CONTR_RAPP        
,oldfrapp::D2240_MARGINE_FIDO_REV    
,oldfrapp::D2239_MARGINE_FIDO_IRREV  
,oldfrapp::D1781_UTILIZZ_CASSA_FIRM  
,oldfrapp::D1780_IMP_ACCORDATO       
,oldfrapp::R025_FLAG_RISTRUTT        
,oldfrapp::D9322_IMP_ACCOR_OPE       
,oldfrapp::D9323_IMP_ACCOR_NON_OPE   
,oldfrapp::D6464_GG_SCONFINO         
,oldfrapp::D0018_FIDO_N_OPE_SBF_P_I  
,oldfrapp::D0019_FIDO_OPE_SBF_P_ILL  
,oldfrapp::D0623_FIDO_OPE_FIRMA      
,oldfrapp::D0009_FIDO_OPE_CASSA      
,oldfrapp::D0008_FIDO_NON_OPER_CAS   
,oldfrapp::D0622_FIDO_NON_OPER_FIR   
,oldfrapp::D0058_FIDO_N_OPE_SBF_CED  
,oldfrapp::D0059_FIDO_OPE_SBF_CED    
,oldfrapp::D0007_AUT_SCONF_CASSA     
,oldfrapp::D0017_AUT_SCONF_SBF       
,oldfrapp::D0621_AUT_SCONF_FIRMA     
,oldfrapp::D0967_GAR_MERCI           
,oldfrapp::D0968_GAR_IPOTECA         
,oldfrapp::D0970_GAR_PERSONALE       
,oldfrapp::D0971_GAR_ALTRE           
,oldfrapp::D0985_GAR_OBBL_PR_EMISS   
,oldfrapp::D0986_GAR_OBBL_B_CENTR    
,oldfrapp::D0987_GAR_OBBL_ALTRE      
,oldfrapp::D0988_GAR_DEP_DENARO      
,oldfrapp::D0989_GAR_CD_PR_EMISS     
,oldfrapp::D0990_GAR_ALTRI_VALORI    
,oldfrapp::D0260_VALORE_DOSSIER      
,oldfrapp::D0021_FIDO_DERIV          
,oldfrapp::D0061_SALDO_CONT          
,oldfrapp::D0062_SALDO_CONT_DARE     
,oldfrapp::D0063_SALDO_CONT_AVERE    
,oldfrapp::D0064_PART_ILLIQ_SBF      
,oldfrapp::D0088_IND_UTIL_FIDO_MED   
,oldfrapp::D0624_IMP_FIDO_ACC_FIRMA  
,oldfrapp::D0625_IMP_UTILIZZO_FIRMA  
,oldfrapp::D0982_IMP_GAR_PERS_CRED   
,oldfrapp::D1509_IMP_SAL_LQ_MED_DAR  
,oldfrapp::D1785_IMP_UTILIZZO_CASSA  
,oldfrapp::D0462_IMP_DER_VAL_INT_PS  
,oldfrapp::D0475_IMP_DER_VAL_INT_NG  
,oldfrapp::QCAPRATEASCAD             
,oldfrapp::QCAPRATEIMPAG             
,oldfrapp::QINTERRATEIMPAG           
,oldfrapp::QCAPRATEMORA              
,oldfrapp::QINTERRATEMORA            
,oldfrapp::ACCONTIRATESCAD           
,oldfrapp::IMPORIGPRESTITO           
,oldfrapp::CDFISC                    
,oldfrapp::D6998_GAR_TITOLI          
,oldfrapp::D6970_GAR_PERS            
,oldfrapp::ADDEBITI_IN_SOSP          
,oldfrapp::CODICEBANCA_PRINC
,oldfrapp::NDGPRINCIPALE
,oldfrapp::DATAINIZIODEF 
            ;
					
oldfrapp_out = FOREACH solo_oldfrapp_filter
			     GENERATE
 '$ufficio'           as ufficio
,hadoop_frapp::codicebanca               
,hadoop_frapp::ndg 			   
,hadoop_frapp::sportello		   
,hadoop_frapp::conto			   
,hadoop_frapp::datariferimento	   
,hadoop_frapp::contoesteso		   
,hadoop_frapp::formatecnica		   
,hadoop_frapp::dataaccensione		   
,hadoop_frapp::dataestinzione 	   
,hadoop_frapp::datascadenza		   
,hadoop_frapp::r046tpammortam		   
,hadoop_frapp::r071tprapporto		   
,hadoop_frapp::r209periodliquid	   
,hadoop_frapp::cdprodottorischio	   
,hadoop_frapp::durataoriginaria	   
,hadoop_frapp::r004divisa 		   
,hadoop_frapp::duarataresidua		   
,hadoop_frapp::r239tpcontrrapp	   
,hadoop_frapp::d2240marginefidorev	   
,hadoop_frapp::d239marginefidoirrev	   
,hadoop_frapp::d1788utilizzaccafirm	   
,hadoop_frapp::d1780impaccordato	   
,hadoop_frapp::r025flagristrutt	   
,hadoop_frapp::d9322impaccorope	   
,hadoop_frapp::d9323impaaccornonope	   
,hadoop_frapp::d6464ggsconfino	   
,hadoop_frapp::d0018fidonopesbfpi	   
,hadoop_frapp::d0019fidopesbfpill        
,hadoop_frapp::d0623fidoopefirma	   
,hadoop_frapp::d0009fidodpecassa	   
,hadoop_frapp::d0008fidononopercas	   
,hadoop_frapp::d0622fidononoperfir	   
,hadoop_frapp::d0058fidonopesbfced	   
,hadoop_frapp::d0059fidoopesbfced	   
,hadoop_frapp::d0007autosconfcassa	   
,hadoop_frapp::d0017autosconfsbf	   
,hadoop_frapp::d0621autosconffirma	   
,hadoop_frapp::d0967gamerci		   
,hadoop_frapp::d0968garipoteca	   
,hadoop_frapp::d0970garpersonale	   
,hadoop_frapp::d0971garaltre		   
,hadoop_frapp::d0985garobblpremiss	   
,hadoop_frapp::d0986garobblbcentr	   
,hadoop_frapp::d0987garobblaltre	   
,hadoop_frapp::d0988gardepdenaro	   
,hadoop_frapp::d0989garcdpremiss	   
,hadoop_frapp::d0990garaltrivalori	   
,hadoop_frapp::d0260valoredossier	   
,hadoop_frapp::d0021fidoderiv		   
,hadoop_frapp::d0061saldocont		   
,hadoop_frapp::d0062aldocontdare	   
,hadoop_frapp::d0063saldocontavere	   
,hadoop_frapp::d0064partilliqsbf	   
,hadoop_frapp::d0088indutilfidomed	   
,hadoop_frapp::d0624impfidoaccfirma      
,hadoop_frapp::d0652imputilizzofirma	   
,hadoop_frapp::d0982impgarperscred	   
,hadoop_frapp::d1509impsallqmeddar	   
,hadoop_frapp::d1785imputilizzocassa	   
,hadoop_frapp::d0462impdervalintps	   
,hadoop_frapp::d0475impdervalintng	   
,hadoop_frapp::qcaprateascad		   
,hadoop_frapp::qcaprateimpag		   
,hadoop_frapp::qintrateimpag		   
,hadoop_frapp::qcapratemora		   
,hadoop_frapp::qintratemora		   
,hadoop_frapp::accontiratescad	   
,hadoop_frapp::imporigprestito	   
,hadoop_frapp::d6998			   
,hadoop_frapp::d6970			   
,hadoop_frapp::d0075addebitiinsosp	   
,hadoop_frapp::codicebanca_princ
,hadoop_frapp::ndgprincipale		   
,hadoop_frapp::datainiziodef
,oldfrapp::CODICEBANCA               
,oldfrapp::NDG                       
,oldfrapp::SPORTELLO                 
,oldfrapp::CONTO                     
,oldfrapp::PROGR_SEGMENTO            
,oldfrapp::DT_RIFERIMENTO            
,oldfrapp::CONTO_ESTESO              
,oldfrapp::FORMA_TECNICA             
,oldfrapp::DT_ACCENSIONE             
,oldfrapp::DT_ESTINZIONE             
,oldfrapp::DT_SCADENZA               
,oldfrapp::R046_TP_AMMORTAMENTO      
,oldfrapp::R071_TP_RAPPORTO          
,oldfrapp::R209_PERIOD_LIQUIDAZION   
,oldfrapp::CD_PRODOTTO_RISCHI        
,oldfrapp::DURATA_ORIGINARIA         
,oldfrapp::R004_DIVISA               
,oldfrapp::DURATA_RESIDUA            
,oldfrapp::R239_TP_CONTR_RAPP        
,oldfrapp::D2240_MARGINE_FIDO_REV    
,oldfrapp::D2239_MARGINE_FIDO_IRREV  
,oldfrapp::D1781_UTILIZZ_CASSA_FIRM  
,oldfrapp::D1780_IMP_ACCORDATO       
,oldfrapp::R025_FLAG_RISTRUTT        
,oldfrapp::D9322_IMP_ACCOR_OPE       
,oldfrapp::D9323_IMP_ACCOR_NON_OPE   
,oldfrapp::D6464_GG_SCONFINO         
,oldfrapp::D0018_FIDO_N_OPE_SBF_P_I  
,oldfrapp::D0019_FIDO_OPE_SBF_P_ILL  
,oldfrapp::D0623_FIDO_OPE_FIRMA      
,oldfrapp::D0009_FIDO_OPE_CASSA      
,oldfrapp::D0008_FIDO_NON_OPER_CAS   
,oldfrapp::D0622_FIDO_NON_OPER_FIR   
,oldfrapp::D0058_FIDO_N_OPE_SBF_CED  
,oldfrapp::D0059_FIDO_OPE_SBF_CED    
,oldfrapp::D0007_AUT_SCONF_CASSA     
,oldfrapp::D0017_AUT_SCONF_SBF       
,oldfrapp::D0621_AUT_SCONF_FIRMA     
,oldfrapp::D0967_GAR_MERCI           
,oldfrapp::D0968_GAR_IPOTECA         
,oldfrapp::D0970_GAR_PERSONALE       
,oldfrapp::D0971_GAR_ALTRE           
,oldfrapp::D0985_GAR_OBBL_PR_EMISS   
,oldfrapp::D0986_GAR_OBBL_B_CENTR    
,oldfrapp::D0987_GAR_OBBL_ALTRE      
,oldfrapp::D0988_GAR_DEP_DENARO      
,oldfrapp::D0989_GAR_CD_PR_EMISS     
,oldfrapp::D0990_GAR_ALTRI_VALORI    
,oldfrapp::D0260_VALORE_DOSSIER      
,oldfrapp::D0021_FIDO_DERIV          
,oldfrapp::D0061_SALDO_CONT          
,oldfrapp::D0062_SALDO_CONT_DARE     
,oldfrapp::D0063_SALDO_CONT_AVERE    
,oldfrapp::D0064_PART_ILLIQ_SBF      
,oldfrapp::D0088_IND_UTIL_FIDO_MED   
,oldfrapp::D0624_IMP_FIDO_ACC_FIRMA  
,oldfrapp::D0625_IMP_UTILIZZO_FIRMA  
,oldfrapp::D0982_IMP_GAR_PERS_CRED   
,oldfrapp::D1509_IMP_SAL_LQ_MED_DAR  
,oldfrapp::D1785_IMP_UTILIZZO_CASSA  
,oldfrapp::D0462_IMP_DER_VAL_INT_PS  
,oldfrapp::D0475_IMP_DER_VAL_INT_NG  
,oldfrapp::QCAPRATEASCAD             
,oldfrapp::QCAPRATEIMPAG             
,oldfrapp::QINTERRATEIMPAG           
,oldfrapp::QCAPRATEMORA              
,oldfrapp::QINTERRATEMORA            
,oldfrapp::ACCONTIRATESCAD           
,oldfrapp::IMPORIGPRESTITO           
,oldfrapp::CDFISC                    
,oldfrapp::D6998_GAR_TITOLI          
,oldfrapp::D6970_GAR_PERS            
,oldfrapp::ADDEBITI_IN_SOSP          
,oldfrapp::CODICEBANCA_PRINC
,oldfrapp::NDGPRINCIPALE
,oldfrapp::DATAINIZIODEF
					;

/*
abbinati_out = FOREACH abbinati_filter
			     GENERATE
 '$ufficio'           as ufficio
,hadoop_frapp::codicebanca               
,hadoop_frapp::ndg 			   
,hadoop_frapp::sportello		   
,hadoop_frapp::conto			   
,hadoop_frapp::datariferimento	   
,hadoop_frapp::contoesteso		   
,hadoop_frapp::formatecnica		   
,hadoop_frapp::dataaccensione		   
,hadoop_frapp::dataestinzione 	   
,hadoop_frapp::datascadenza		   
,hadoop_frapp::r046tpammortam		   
,hadoop_frapp::r071tprapporto		   
,hadoop_frapp::r209periodliquid	   
,hadoop_frapp::cdprodottorischio	   
,hadoop_frapp::durataoriginaria	   
,hadoop_frapp::r004divisa 		   
,hadoop_frapp::duarataresidua		   
,hadoop_frapp::r239tpcontrrapp	   
,hadoop_frapp::d2240marginefidorev	   
,hadoop_frapp::d239marginefidoirrev	   
,hadoop_frapp::d1788utilizzaccafirm	   
,hadoop_frapp::d1780impaccordato	   
,hadoop_frapp::r025flagristrutt	   
,hadoop_frapp::d9322impaccorope	   
,hadoop_frapp::d9323impaaccornonope	   
,hadoop_frapp::d6464ggsconfino	   
,hadoop_frapp::d0018fidonopesbfpi	   
,hadoop_frapp::d0019fidopesbfpill        
,hadoop_frapp::d0623fidoopefirma	   
,hadoop_frapp::d0009fidodpecassa	   
,hadoop_frapp::d0008fidononopercas	   
,hadoop_frapp::d0622fidononoperfir	   
,hadoop_frapp::d0058fidonopesbfced	   
,hadoop_frapp::d0059fidoopesbfced	   
,hadoop_frapp::d0007autosconfcassa	   
,hadoop_frapp::d0017autosconfsbf	   
,hadoop_frapp::d0621autosconffirma	   
,hadoop_frapp::d0967gamerci		   
,hadoop_frapp::d0968garipoteca	   
,hadoop_frapp::d0970garpersonale	   
,hadoop_frapp::d0971garaltre		   
,hadoop_frapp::d0985garobblpremiss	   
,hadoop_frapp::d0986garobblbcentr	   
,hadoop_frapp::d0987garobblaltre	   
,hadoop_frapp::d0988gardepdenaro	   
,hadoop_frapp::d0989garcdpremiss	   
,hadoop_frapp::d0990garaltrivalori	   
,hadoop_frapp::d0260valoredossier	   
,hadoop_frapp::d0021fidoderiv		   
,hadoop_frapp::d0061saldocont		   
,hadoop_frapp::d0062aldocontdare	   
,hadoop_frapp::d0063saldocontavere	   
,hadoop_frapp::d0064partilliqsbf	   
,hadoop_frapp::d0088indutilfidomed	   
,hadoop_frapp::d0624impfidoaccfirma      
,hadoop_frapp::d0652imputilizzofirma	   
,hadoop_frapp::d0982impgarperscred	   
,hadoop_frapp::d1509impsallqmeddar	   
,hadoop_frapp::d1785imputilizzocassa	   
,hadoop_frapp::d0462impdervalintps	   
,hadoop_frapp::d0475impdervalintng	   
,hadoop_frapp::qcaprateascad		   
,hadoop_frapp::qcaprateimpag		   
,hadoop_frapp::qintrateimpag		   
,hadoop_frapp::qcapratemora		   
,hadoop_frapp::qintratemora		   
,hadoop_frapp::accontiratescad	   
,hadoop_frapp::imporigprestito	   
,hadoop_frapp::d6998			   
,hadoop_frapp::d6970			   
,hadoop_frapp::d0075addebitiinsosp	   
,hadoop_frapp::codicebanca_princ
,hadoop_frapp::ndgprincipale		   
,hadoop_frapp::datainiziodef		   
,oldfrapp::CODICEBANCA               
,oldfrapp::NDG                       
,oldfrapp::SPORTELLO                 
,oldfrapp::CONTO                     
,oldfrapp::PROGR_SEGMENTO            
,oldfrapp::DT_RIFERIMENTO            
,oldfrapp::CONTO_ESTESO              
,oldfrapp::FORMA_TECNICA             
,oldfrapp::DT_ACCENSIONE             
,oldfrapp::DT_ESTINZIONE             
,oldfrapp::DT_SCADENZA               
,oldfrapp::R046_TP_AMMORTAMENTO      
,oldfrapp::R071_TP_RAPPORTO          
,oldfrapp::R209_PERIOD_LIQUIDAZION   
,oldfrapp::CD_PRODOTTO_RISCHI        
,oldfrapp::DURATA_ORIGINARIA         
,oldfrapp::R004_DIVISA               
,oldfrapp::DURATA_RESIDUA            
,oldfrapp::R239_TP_CONTR_RAPP        
,oldfrapp::D2240_MARGINE_FIDO_REV    
,oldfrapp::D2239_MARGINE_FIDO_IRREV  
,oldfrapp::D1781_UTILIZZ_CASSA_FIRM  
,oldfrapp::D1780_IMP_ACCORDATO       
,oldfrapp::R025_FLAG_RISTRUTT        
,oldfrapp::D9322_IMP_ACCOR_OPE       
,oldfrapp::D9323_IMP_ACCOR_NON_OPE   
,oldfrapp::D6464_GG_SCONFINO         
,oldfrapp::D0018_FIDO_N_OPE_SBF_P_I  
,oldfrapp::D0019_FIDO_OPE_SBF_P_ILL  
,oldfrapp::D0623_FIDO_OPE_FIRMA      
,oldfrapp::D0009_FIDO_OPE_CASSA      
,oldfrapp::D0008_FIDO_NON_OPER_CAS   
,oldfrapp::D0622_FIDO_NON_OPER_FIR   
,oldfrapp::D0058_FIDO_N_OPE_SBF_CED  
,oldfrapp::D0059_FIDO_OPE_SBF_CED    
,oldfrapp::D0007_AUT_SCONF_CASSA     
,oldfrapp::D0017_AUT_SCONF_SBF       
,oldfrapp::D0621_AUT_SCONF_FIRMA     
,oldfrapp::D0967_GAR_MERCI           
,oldfrapp::D0968_GAR_IPOTECA         
,oldfrapp::D0970_GAR_PERSONALE       
,oldfrapp::D0971_GAR_ALTRE           
,oldfrapp::D0985_GAR_OBBL_PR_EMISS   
,oldfrapp::D0986_GAR_OBBL_B_CENTR    
,oldfrapp::D0987_GAR_OBBL_ALTRE      
,oldfrapp::D0988_GAR_DEP_DENARO      
,oldfrapp::D0989_GAR_CD_PR_EMISS     
,oldfrapp::D0990_GAR_ALTRI_VALORI    
,oldfrapp::D0260_VALORE_DOSSIER      
,oldfrapp::D0021_FIDO_DERIV          
,oldfrapp::D0061_SALDO_CONT          
,oldfrapp::D0062_SALDO_CONT_DARE     
,oldfrapp::D0063_SALDO_CONT_AVERE    
,oldfrapp::D0064_PART_ILLIQ_SBF      
,oldfrapp::D0088_IND_UTIL_FIDO_MED   
,oldfrapp::D0624_IMP_FIDO_ACC_FIRMA  
,oldfrapp::D0625_IMP_UTILIZZO_FIRMA  
,oldfrapp::D0982_IMP_GAR_PERS_CRED   
,oldfrapp::D1509_IMP_SAL_LQ_MED_DAR  
,oldfrapp::D1785_IMP_UTILIZZO_CASSA  
,oldfrapp::D0462_IMP_DER_VAL_INT_PS  
,oldfrapp::D0475_IMP_DER_VAL_INT_NG  
,oldfrapp::QCAPRATEASCAD             
,oldfrapp::QCAPRATEIMPAG             
,oldfrapp::QINTERRATEIMPAG           
,oldfrapp::QCAPRATEMORA              
,oldfrapp::QINTERRATEMORA            
,oldfrapp::ACCONTIRATESCAD           
,oldfrapp::IMPORIGPRESTITO           
,oldfrapp::CDFISC                    
,oldfrapp::D6998_GAR_TITOLI          
,oldfrapp::D6970_GAR_PERS            
,oldfrapp::ADDEBITI_IN_SOSP          
,oldfrapp::CODICEBANCA_PRINC
,oldfrapp::NDGPRINCIPALE
,oldfrapp::DATAINIZIODEF
					;
*/

STORE hadoop_frapp_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/frapp/frapp_solo_hadoop' USING PigStorage (';');

STORE oldfrapp_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/frapp/frapp_solo_old' USING PigStorage (';');

--STORE abbinati_out INTO '/app/drlgd/datarequest/dt/out/$ufficio/quad/frapp/frapp_abbinati' USING PigStorage (';');
