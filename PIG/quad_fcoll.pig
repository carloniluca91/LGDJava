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
fcoll_load = LOAD '/app/drlgd/store/fcoll'
      USING PigStorage(';') 
  	  AS (
			 cumulo             :chararray
			,cd_istituto_COLL   :chararray
			,ndg_COLL           :chararray
			,data_inizio_DEF    :chararray
			,data_collegamento  :chararray
			,pri                :chararray
         );
/*
fcoll1 = FILTER fcoll_load 
BY cumulo != 'cumulo' ;
*/
fcoll = FOREACH fcoll_load
			GENERATE
			 cumulo             as cumulo
			,cd_istituto_COLL   as cd_istituto_COLL
			,ndg_COLL           as ndg_COLL
			,ToString(ToDate( data_inizio_DEF,'ddMMMyyyy'),'yyyyMMdd')    as data_inizio_DEF
			,ToString(ToDate( data_collegamento,'ddMMMyyyy'),'yyyyMMdd')  as data_collegamento
			,pri                as pri
            ;

/*
carico la tabella MOVCONTA
*/
oldfposi_load = LOAD '/app/drlgd/store/oldfposi'
			USING PigStorage (';')
			AS (
				 datainizioDEF   :chararray
				,dataFINEDEF     :chararray
				,dataINIZIOPD    :chararray
				,datainizioinc   :chararray
				,dataSOFFERENZA  :chararray
				,codicebanca     :chararray
				,ndgprincipale   :chararray
				,flagincristrut  :chararray
				,cumulo          :chararray
				);

/* scarto records con tutte date default non valorizzate */
oldfposi1 = FILTER oldfposi_load 
BY dataINIZIOPD is not null
OR datainizioinc is not null
OR dataSOFFERENZA is not null
;

oldfposi = FOREACH oldfposi1
			GENERATE
			     ToString(ToDate( datainizioDEF,'yy-MM-dd'),'yyyyMMdd')   as datainizioDEF
			    ,ToString(ToDate( dataFINEDEF,'yy-MM-dd'),'yyyyMMdd')   as dataFINEDEF
				,dataINIZIOPD    as dataINIZIOPD
				,datainizioinc   as datainizioinc
				,dataSOFFERENZA  as dataSOFFERENZA
				,codicebanca     as codicebanca
				,ndgprincipale   as ndgprincipale
				,flagincristrut  as flagincristrut
				,cumulo          as cumulo
            ;

/*unisco le due tabelle hadoop e old */

fcoll_oldfposi_join = JOIN oldfposi BY (cumulo) LEFT, fcoll BY (cumulo);

file_out =   FOREACH fcoll_oldfposi_join
			     GENERATE
			 oldfposi::codicebanca    as codicebanca
			,oldfposi::ndgprincipale  as ndgprincipale
			,oldfposi::datainizioDEF  as datainizioDEF
			,oldfposi::dataFINEDEF    as dataFINEDEF
            ,( fcoll::data_inizio_DEF   is null ? oldfposi::datainizioDEF : fcoll::data_inizio_DEF )     as DATA_DEFAULT
            ,( fcoll::cd_istituto_COLL  is null ? oldfposi::codicebanca   : fcoll::cd_istituto_COLL )    as ISTITUTO_COLLEGATO
            ,( fcoll::ndg_COLL          is null ? oldfposi::ndgprincipale : fcoll::ndg_COLL )            as NDG_COLLEGATO
            ,( fcoll::data_collegamento is null ? oldfposi::datainizioDEF : fcoll::data_collegamento )   as DATA_COLLEGAMENTO
            ,fcoll::cumulo as CUMULO
            ;
            
file_out_dist =   DISTINCT ( 
             FOREACH file_out
			 GENERATE
			 codicebanca
			,ndgprincipale
			,datainizioDEF
			,dataFINEDEF
            ,DATA_DEFAULT
            ,ISTITUTO_COLLEGATO
            ,NDG_COLLEGATO
            ,DATA_COLLEGAMENTO
            ,CUMULO
            );

--STORE file_out_dist INTO '/app/drlgd/datarequest/dt/out/quad/$ufficio/fcoll' USING PigStorage (';');
