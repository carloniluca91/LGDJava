
/*
$LastChangedDate: 2016-01-08 16:13:43 +0100 (ven, 08 gen 2016) $
$Rev: 237 $		
$Author: davitali $	

Carico la TLBMIGNW		
Aggiungo il proressivo 0 
*/


tlbmignw = LOAD '$tlbmignw_path' USING PigStorage(';') 
           AS
           (
             cd_isti_ric     :chararray       
  			,ndg_ric         :chararray              
			,cd_abi_ric      :chararray       
			,cd_isti_ced     :chararray
			,ndg_ced         :chararray
			,cd_abi_ced      :chararray
			,progressivo     :int
		);
		
tlbmignw_proj_prog0 = FOREACH tlbmignw
                      GENERATE 
                           cd_isti_ric            
			  			  ,ndg_ric                       
						  ,cd_abi_ric         
						  ,cd_isti_ric as cd_isti_ced           
			  			  ,ndg_ric     as  ndg_ced                 
						  ,cd_abi_ric as cd_abi_ced
						  ,0 as progressivo
						;
						
tlbmignw_proj_prog0_dist = DISTINCT tlbmignw_proj_prog0;						  
						  
tlbmignw_proj_union = UNION tlbmignw, tlbmignw_proj_prog0_dist;
                                                              
tlbmignw_proj = FOREACH tlbmignw_proj_union 
               GENERATE 
                     cd_isti_ric
                    ,ndg_ric		
                    ,progressivo		
                    ,cd_isti_ced 
                    ,ndg_ced
                   ; 		                                                            
                                                              
STORE  tlbmignw_proj INTO '$tlbmignw_pivoted_path' USING PigStorage(';');
