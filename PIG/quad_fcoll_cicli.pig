/*
$LastChangedDate: 2016-06-01 10:53:23 +0200 (mer, 01 giu 2016) $
$Rev: 389 $		
$Author: arzelenko $	

Sono coinvolti i seguenti parametri:
		-$ufficio

*/

/*
carico fcoll_oldfposi
*/
fcoll = LOAD '/app/drlgd/datarequest/dt/out/quad/$ufficio/fcoll'
      USING PigStorage(';') 
  	  AS (
  	         codicebanca          :chararray
			,ndgprincipale        :chararray
			,datainiziodef        :chararray
			,datafinedef          :chararray
            ,data_default         :chararray
            ,istituto_collegato   :chararray
            ,ndg_collegato        :chararray
            ,data_collegamento    :chararray
            ,cumulo               :chararray
         );
/*
fcoll1 = FILTER fcoll_load 
BY cumulo != 'cumulo' ;
*/

/*
carico la tabella cicli_ndg
*/
cicli_ndg_load = LOAD '/app/drlgd/datarequest/dt/tmp/$ufficio/cicli_ndg'
			USING PigStorage (';')
			AS (
				 cd_isti             :chararray
				,ndg_principale      :chararray
				,dt_inizio_ciclo     :chararray
				,dt_fine_ciclo       :chararray
				,datainiziopd        :chararray
				,datainizioristrutt  :chararray
				,datainizioinc       :chararray
				,datainiziosoff      :chararray
				,c_key               :chararray
				,tipo_segmne         :chararray
				,sae_segm            :chararray
				,rae_segm            :chararray
				,segmento            :chararray
				,tp_ndg              :chararray
				,provincia_segm      :chararray
				,databilseg          :chararray
				,strbilseg           :chararray
				,attivobilseg        :chararray
				,fatturbilseg        :chararray
				,ndg_coll            :chararray
				,cd_isti_coll        :chararray
				);

/*unisco le due tabelle hadoop e fcoll */
fcoll_cicli_join = JOIN cicli_ndg_load BY (cd_isti_coll, ndg_coll) FULL OUTER
                       ,fcoll          BY (istituto_collegato, ndg_collegato);
            
solo_fcoll_out = FOREACH fcoll_cicli_join
			     GENERATE
			 '$ufficio'                        as ufficio
			,cicli_ndg_load::cd_isti           as cd_isti
			,cicli_ndg_load::ndg_principale    as ndg_principale
			,cicli_ndg_load::dt_inizio_ciclo   as dt_inizio_ciclo
			,cicli_ndg_load::cd_isti_coll      as cd_isti_coll
			,cicli_ndg_load::ndg_coll          as ndg_coll
			,fcoll::codicebanca                as fcoll_codicebanca
			,fcoll::ndgprincipale              as fcoll_ndgprincipale
            ,fcoll::istituto_collegato         as fcoll_cd_istituto_coll
            ,fcoll::ndg_collegato              as fcoll_ndg_coll
            ,fcoll::datainiziodef              as fcoll_data_inizio_def
            ;
                
file_out = DISTINCT ( 
                      FOREACH solo_fcoll_out                          
			          GENERATE
                      ufficio
					 ,cd_isti  
					 ,ndg_principale	          
					 ,dt_inizio_ciclo	      
					 ,cd_isti_coll	      
					 ,ndg_coll
					 ,fcoll_codicebanca
					 ,fcoll_ndgprincipale
					 ,fcoll_cd_istituto_coll
					 ,fcoll_ndg_coll
					 ,fcoll_data_inizio_def)
                     ;   

STORE file_out INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/fcoll_cicli' USING PigStorage (';');
