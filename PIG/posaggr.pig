/*
$LastChangedDate: 2016-01-08 16:13:43 +0100 (ven, 08 gen 2016) $
$Rev: 237 $		
$Author: davitali $	

Sono coinvolti i seguenti parametri:
		-tlbcomp_path
		-tlbaggr_path
		-tlbposi_path
		-posaggr_outdir
		
*/

/*
Carico il file tblcomp
*/
register DoubleConverter.jar;

tblcomp =  LOAD '$tlbcomp_path' 
           		USING PigStorage(';') 
           		AS (
           		    dt_riferimento: int
					,c_key: chararray
					,tipo_segmne: chararray
					,cd_istituto: chararray
					,ndg: chararray
				   );

/*
Carico il file tlbaggr
*/
tlbaggr = LOAD  '$tlbaggr_path'  
          USING PigStorage(';')
          AS (
                 dt_riferimento: int
				,c_key_aggr: chararray
				,ndg_gruppo: chararray
				,cod_fiscale: chararray
				,tipo_segmne_aggr: chararray
				,segmento: chararray
				,tipo_motore: chararray
				,cd_istituto: chararray
				,ndg: chararray
				,rae: chararray
				,sae: chararray
				,tp_ndg: chararray
				,prov_segm: chararray
				,fonte_segmento: chararray
				,utilizzo_cr: chararray
				,accordato_cr: chararray
				,databil: chararray
				,strutbil: chararray
				,fatturbil: chararray
				,attivobil: chararray
				,codimp_cebi: chararray
				,tot_acco_agr: double
				,tot_util_agr: chararray
				,n058_int_vig: chararray
             );

/*
A rottura della chiave della tabella TLBAGGR, si ottiene un elenco di chiavi univoche della tabella TLBCOMP:
DT_RIFERIMENTO
CD_ISTITUTO
NDG

*/             
             
tblcomp_tlbaggr_join = JOIN tlbaggr BY (dt_riferimento,c_key_aggr,tipo_segmne_aggr,cd_istituto), tblcomp BY ( dt_riferimento, c_key, tipo_segmne, cd_istituto);
tblcomp_tlbaggr = FOREACH tblcomp_tlbaggr_join
                  GENERATE 
                      tlbaggr::dt_riferimento as  dt_riferimento
                     ,tblcomp::cd_istituto as  cd_istituto
                     ,tblcomp::ndg as ndg
                     ,tlbaggr::c_key_aggr as c_key_aggr 
                     ,tlbaggr::tipo_segmne_aggr as tipo_segmne_aggr
                     ,tlbaggr::segmento as segmento
                     ,tlbaggr::tp_ndg as tp_ndg
                   ;  

/*
Con questo elenco, che costituisce chiave primaria anche per la tabella TLBPOSI, 
si ottengono i record della tabella TLBPOSI e si aggregano quindi tutti gli importi trovati
*/

/*
Carico il file tlbposi
*/	
tlbposi_load = LOAD '$tlbposi_path' 
		          USING  PigStorage(';') 
		          AS (
						 dt_riferimento: int
						,cd_istituto: chararray
						,ndg: chararray
						,c_key: chararray
						,cod_fiscale: chararray
						,ndg_gruppo: chararray
						,bo_acco: chararray
						,bo_util: chararray
						,tot_add_sosp: chararray
						,tot_val_intr: chararray
						,ca_acco: chararray
						,ca_util: chararray
						,fl_incaglio: chararray
						,fl_soff: chararray
						,fl_inc_ogg: chararray
						,fl_ristr: chararray
						,fl_pd_90: chararray
						,fl_pd_180: chararray
						,util_cassa:  chararray 
						,fido_op_cassa: chararray
						,utilizzo_titoli: chararray
						,esposizione_titoli: chararray 
					 );	

tlbposi = FOREACH tlbposi_load
          GENERATE 
				 dt_riferimento
				,cd_istituto
				,ndg
				,c_key
				,cod_fiscale
				,ndg_gruppo
				,(double)REPLACE(bo_acco,',','.') as bo_acco
				,(double)REPLACE(bo_util,',','.') as bo_util
				,(double)REPLACE(tot_add_sosp,',','.') as tot_add_sosp
				,(double)REPLACE(tot_val_intr,',','.') as tot_val_intr
				,(double)REPLACE(ca_acco,',','.') as ca_acco
				,(double)REPLACE(ca_util,',','.') as ca_util
				,fl_incaglio
				,fl_soff
				,fl_inc_ogg
				,fl_ristr
				,fl_pd_90
				,fl_pd_180
				,(double)REPLACE(util_cassa,',','.') as util_cassa 
				,(double)REPLACE(fido_op_cassa,',','.') as fido_op_cassa
				,(double)REPLACE(utilizzo_titoli,',','.') as utilizzo_titoli
				,(double)REPLACE(esposizione_titoli,',','.') as esposizione_titoli
			 ;	


/* 
Join tra fposi e aggregato
*/			 

tblcomp_tlbaggr_tlbposi_join = JOIN tblcomp_tlbaggr BY (dt_riferimento,cd_istituto,ndg), tlbposi BY (dt_riferimento,cd_istituto,ndg);
tblcomp_tlbaggr_tlbposi = FOREACH tblcomp_tlbaggr_tlbposi_join
                          GENERATE 
                              tblcomp_tlbaggr::dt_riferimento as dt_riferimento
                             ,tblcomp_tlbaggr::cd_istituto as cd_istituto
                             ,tblcomp_tlbaggr::ndg as ndg
                             ,tblcomp_tlbaggr::c_key_aggr as c_key_aggr 
		                     ,tblcomp_tlbaggr::tipo_segmne_aggr as tipo_segmne_aggr
		                     ,tblcomp_tlbaggr::segmento as segmento
		                     ,TRIM(tblcomp_tlbaggr::tp_ndg) as tp_ndg
                             ,tlbposi::bo_acco as  bo_acco
                             ,tlbposi::bo_util as bo_util
                             ,tlbposi::tot_add_sosp as tot_add_sosp
			                 ,tlbposi::tot_val_intr as tot_val_intr
			                 ,tlbposi::ca_acco as ca_acco
			                 ,tlbposi::ca_util as ca_util
			                 ,tlbposi::util_cassa as util_cassa
			                 ,tlbposi::fido_op_cassa as fido_op_cassa
			                 ,tlbposi::utilizzo_titoli as utilizzo_titoli
							 ,tlbposi::esposizione_titoli as esposizione_titoli
                          ;
                          
tblcomp_tlbaggr_tlbposi_grp = GROUP tblcomp_tlbaggr_tlbposi BY (dt_riferimento, cd_istituto, c_key_aggr, tipo_segmne_aggr);

posaggr = FOREACH tblcomp_tlbaggr_tlbposi_grp
          GENERATE 
               group.dt_riferimento
		      ,group.cd_istituto
		      ,group.c_key_aggr
		      ,group.tipo_segmne_aggr
		      ,FLATTEN(tblcomp_tlbaggr_tlbposi.segmento) as segmento
		      ,FLATTEN(tblcomp_tlbaggr_tlbposi.tp_ndg) as tp_ndg
			  ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.bo_acco)) as accordato_bo
              ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.bo_util)) as utilizzato_bo
              ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.tot_add_sosp)) as tot_add_sosp
              ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.tot_val_intr)) as tot_val_intr_ps
              ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.ca_acco)) as accordato_ca
              ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.ca_util)) as utilizzato_ca
              ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.util_cassa)) as util_cassa
			  ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.fido_op_cassa)) as fido_op_cassa
			  ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.utilizzo_titoli)) as utilizzo_titoli
			  ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.esposizione_titoli)) as esposizione_titoli
			 ; 

STORE posaggr INTO '$posaggr_outdir' USING PigStorage(';');

