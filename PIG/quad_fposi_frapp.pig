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
   
hadoop_frapp_gen = FOREACH hadoop_frapp
			     GENERATE
 codicebanca_princ      as codicebanca_princ
,ndgprincipale          as ndgprincipale
,datainiziodef          as datainiziodef
,(double)REPLACE(d1785imputilizzocassa, ',','.')  as utilizzato
,(double)REPLACE(d1780impaccordato, ',','.')      as accordato
,datariferimento        as datariferimento
;

hadoop_frapp_gen_filter = FILTER hadoop_frapp_gen BY datainiziodef==datariferimento;

hadoop_frapp_gen_filter_group = GROUP hadoop_frapp_gen_filter BY (codicebanca_princ, ndgprincipale, datainiziodef);

hadoop_frapp_sum = FOREACH hadoop_frapp_gen_filter_group
			     GENERATE
 group.codicebanca_princ      as codicebanca_princ
,group.ndgprincipale          as ndgprincipale
,group.datainiziodef          as datainiziodef
,SUM(hadoop_frapp_gen_filter.utilizzato)              as utilizzato
,SUM(hadoop_frapp_gen_filter.accordato)               as accordato
;

/*
carico il file fposi prodotto da hadoop
*/
hadoop_fposi = LOAD '/app/drlgd/datarequest/dt/out/finale/$ufficio/fposi'
      USING PigStorage(';') 
  	  AS (
                 codicebanca            :chararray
				,ndgprincipale          :chararray
                ,datainiziodef          :chararray
                ,datafinedef            :chararray
				,ndg_gruppo             :chararray
				,datainiziopd           :chararray
				,datainizioinc          :chararray
				,datainizioristrutt     :chararray
				,datainiziosoff         :chararray
				,totaccordatodatdef     :double
				,totutilizzdatdef       :double
         );
         
hadoop_fposi_gen = FOREACH hadoop_fposi
			     GENERATE
 codicebanca            as codicebanca
,ndgprincipale          as ndgprincipale
,datainiziodef          as datainiziodef
,totutilizzdatdef       as totutilizzdatdef
,totaccordatodatdef     as totaccordatodatdef
;

/*unisco le due tabelle hadoop e old */
hadoop_frapp_oldfrapp_join = JOIN hadoop_frapp_sum BY (codicebanca_princ, ndgprincipale, datainiziodef), 
                                  hadoop_fposi BY (codicebanca, ndgprincipale, datainiziodef);

file_out_filter = FILTER hadoop_frapp_oldfrapp_join 
BY (utilizzato != totutilizzdatdef ) 
OR (accordato  != totaccordatodatdef )
;	

file_out = FOREACH file_out_filter
			     GENERATE
'$ufficio' as ufficio
,hadoop_fposi::codicebanca             
,hadoop_fposi::ndgprincipale
,hadoop_fposi::datainiziodef
,hadoop_fposi::totaccordatodatdef
,hadoop_fposi::totutilizzdatdef
,hadoop_frapp_sum::accordato
,hadoop_frapp_sum::utilizzato
;

STORE file_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/fposifrapp' USING PigStorage (';');
