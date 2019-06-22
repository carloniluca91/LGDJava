/*
$LastChangedDate: 2016-01-08 16:13:43 +0100 (ven, 08 gen 2016) $
$Rev: 237 $		
$Author: davitali $	

Sono coinvolti i seguenti parametri:
		-tlbmignw_path
		-tlbmignw_pivoted_outdir
		
*/


tlbmignw = LOAD '$tlbmignw_path' USING PigStorage(';') 
           AS
           (
             cd_isti_ric     :chararray       
  			,ndg_ric         :chararray              
			,cd_abi_ric      :chararray       
			,cd_isti_ced01   :chararray
			,ndg_ced01       :chararray
			,cd_abi_ced01    :chararray
			,cd_isti_ced02   :chararray
			,ndg_ced02      :chararray
			,cd_abi_ced02   :chararray
			,cd_isti_ced03   :chararray
			,ndg_ced03      :chararray
			,cd_abi_ced03   :chararray
			,cd_isti_ced04   :chararray
			,ndg_ced04      :chararray
			,cd_abi_ced04   :chararray
			,cd_isti_ced05   :chararray
			,ndg_ced05      :chararray
			,cd_abi_ced05   :chararray
			,cd_isti_ced06   :chararray
			,ndg_ced06      :chararray
			,cd_abi_ced06   :chararray
			,cd_isti_ced07   :chararray
			,ndg_ced07      :chararray
			,cd_abi_ced07   :chararray
			,cd_isti_ced08   :chararray
			,ndg_ced08      :chararray
			,cd_abi_ced08   :chararray
			,cd_isti_ced09   :chararray
			,ndg_ced09      :chararray
			,cd_abi_ced09   :chararray
			,cd_isti_ced10   :chararray
			,ndg_ced10      :chararray
			,cd_abi_ced10   :chararray
			,cd_isti_ced11   :chararray
			,ndg_ced11      :chararray
			,cd_abi_ced11   :chararray
			,cd_isti_ced12   :chararray
			,ndg_ced12      :chararray
			,cd_abi_ced12   :chararray
			,cd_isti_ced13   :chararray
			,ndg_ced13      :chararray
			,cd_abi_ced13   :chararray
			,cd_isti_ced14   :chararray
			,ndg_ced14      :chararray
			,cd_abi_ced14   :chararray
			,cd_isti_ced15   :chararray
			,ndg_ced15      :chararray
			,cd_abi_ced15   :chararray
			,cd_isti_ced16   :chararray
			,ndg_ced16      :chararray
			,cd_abi_ced16   :chararray
			,cd_isti_ced17   :chararray
			,ndg_ced17      :chararray
			,cd_abi_ced17   :chararray
			,cd_isti_ced18   :chararray
			,ndg_ced18      :chararray
			,cd_abi_ced18   :chararray
			,cd_isti_ced19   :chararray
			,ndg_ced19      :chararray
			,cd_abi_ced19   :chararray
			,cd_isti_ced20   :chararray
			,ndg_ced20      :chararray
			,cd_abi_ced20   :chararray
			,cd_isti_ced21   :chararray
			,ndg_ced21      :chararray
			,cd_abi_ced21   :chararray
			,cd_isti_ced22   :chararray
			,ndg_ced22      :chararray
			,cd_abi_ced22   :chararray
			,cd_isti_ced23   :chararray
			,ndg_ced23      :chararray
			,cd_abi_ced23   :chararray
			,cd_isti_ced24   :chararray
			,ndg_ced24      :chararray
			,cd_abi_ced24   :chararray
			,cd_isti_ced25   :chararray
			,ndg_ced25      :chararray
			,cd_abi_ced25   :chararray
			,cd_isti_ced26   :chararray
			,ndg_ced26      :chararray
			,cd_abi_ced26   :chararray
			,cd_isti_ced27   :chararray
			,ndg_ced27      :chararray
			,cd_abi_ced27   :chararray
			,cd_isti_ced28   :chararray
			,ndg_ced28      :chararray
			,cd_abi_ced28   :chararray
			,cd_isti_ced29   :chararray
			,ndg_ced29      :chararray
			,cd_abi_ced29   :chararray
			,cd_isti_ced30   :chararray
			,ndg_ced30      :chararray
			,cd_abi_ced30   :chararray
			,cd_isti_ced31   :chararray
			,ndg_ced31      :chararray
			,cd_abi_ced31   :chararray
			,cd_isti_ced32   :chararray
			,ndg_ced32      :chararray
			,cd_abi_ced32   :chararray
			,cd_isti_ced33   :chararray
			,ndg_ced33      :chararray
			,cd_abi_ced33   :chararray
			,cd_isti_ced34   :chararray
			,ndg_ced34      :chararray
			,cd_abi_ced34   :chararray
			,cd_isti_ced35   :chararray
			,ndg_ced35      :chararray
			,cd_abi_ced35   :chararray
			,cd_isti_ced36   :chararray
			,ndg_ced36      :chararray
			,cd_abi_ced36   :chararray
			,cd_isti_ced37   :chararray
			,ndg_ced37      :chararray
			,cd_abi_ced37   :chararray
			,cd_isti_ced38   :chararray
			,ndg_ced38      :chararray
			,cd_abi_ced38   :chararray
			,cd_isti_ced39   :chararray
			,ndg_ced39      :chararray
			,cd_abi_ced39   :chararray
			,cd_isti_ced40   :chararray
			,ndg_ced40      :chararray
			,cd_abi_ced40   :chararray
			,cd_isti_ced41   :chararray
			,ndg_ced41      :chararray
			,cd_abi_ced41   :chararray
			,cd_isti_ced42   :chararray
			,ndg_ced42      :chararray
			,cd_abi_ced42   :chararray
			,cd_isti_ced43   :chararray
			,ndg_ced43      :chararray
			,cd_abi_ced43   :chararray
			,cd_isti_ced44   :chararray
			,ndg_ced44      :chararray
			,cd_abi_ced44   :chararray
			,cd_isti_ced45   :chararray
			,ndg_ced45      :chararray
			,cd_abi_ced45   :chararray
			,cd_isti_ced46   :chararray
			,ndg_ced46      :chararray
			,cd_abi_ced46   :chararray
			,cd_isti_ced47   :chararray
			,ndg_ced47      :chararray
			,cd_abi_ced47   :chararray
			,cd_isti_ced48   :chararray
			,ndg_ced48      :chararray
			,cd_abi_ced48   :chararray
			,cd_isti_ced49   :chararray
			,ndg_ced49      :chararray
			,cd_abi_ced49   :chararray
			,cd_isti_ced50   :chararray
			,ndg_ced50      :chararray
			,cd_abi_ced50   :chararray
			,cd_isti_ced51   :chararray
			,ndg_ced51      :chararray
			,cd_abi_ced51   :chararray
			,cd_isti_ced52   :chararray
			,ndg_ced52      :chararray
			,cd_abi_ced52   :chararray
			,cd_isti_ced53   :chararray
			,ndg_ced53      :chararray
			,cd_abi_ced53   :chararray
			,cd_isti_ced54   :chararray
			,ndg_ced54      :chararray
			,cd_abi_ced54   :chararray
			,cd_isti_ced55   :chararray
			,ndg_ced55      :chararray
			,cd_abi_ced55   :chararray
			,cd_isti_ced56   :chararray
			,ndg_ced56      :chararray
			,cd_abi_ced56   :chararray
			,cd_isti_ced57   :chararray
			,ndg_ced57      :chararray
			,cd_abi_ced57   :chararray
			,cd_isti_ced58   :chararray
			,ndg_ced58      :chararray
			,cd_abi_ced58   :chararray
			,cd_isti_ced59   :chararray
			,ndg_ced59      :chararray
			,cd_abi_ced59   :chararray
			,cd_isti_ced60   :chararray
			,ndg_ced60      :chararray
			,cd_abi_ced60   :chararray
			,cd_isti_ced61   :chararray
			,ndg_ced61      :chararray
			,cd_abi_ced61   :chararray
			,cd_isti_ced62   :chararray
			,ndg_ced62      :chararray
			,cd_abi_ced62   :chararray
			,cd_isti_ced63   :chararray
			,ndg_ced63      :chararray
			,cd_abi_ced63   :chararray
			,cd_isti_ced64   :chararray
			,ndg_ced64      :chararray
			,cd_abi_ced64   :chararray
			,cd_isti_ced65   :chararray
			,ndg_ced65      :chararray
			,cd_abi_ced65   :chararray
			,cd_isti_ced66   :chararray
			,ndg_ced66      :chararray
			,cd_abi_ced66   :chararray
			,cd_isti_ced67   :chararray
			,ndg_ced67      :chararray
			,cd_abi_ced67   :chararray
			,cd_isti_ced68   :chararray
			,ndg_ced68      :chararray
			,cd_abi_ced68   :chararray
			,cd_isti_ced69   :chararray
			,ndg_ced69      :chararray
			,cd_abi_ced69   :chararray
			,cd_isti_ced70   :chararray
			,ndg_ced70      :chararray
			,cd_abi_ced70   :chararray
			,cd_isti_ced71   :chararray
			,ndg_ced71      :chararray
			,cd_abi_ced71   :chararray
			,cd_isti_ced72   :chararray
			,ndg_ced72      :chararray
			,cd_abi_ced72   :chararray
			,cd_isti_ced73   :chararray
			,ndg_ced73      :chararray
			,cd_abi_ced73   :chararray
			,cd_isti_ced74   :chararray
			,ndg_ced74      :chararray
			,cd_abi_ced74   :chararray
			,cd_isti_ced75   :chararray
			,ndg_ced75      :chararray
			,cd_abi_ced75   :chararray
			,cd_isti_ced76   :chararray
			,ndg_ced76      :chararray
			,cd_abi_ced76   :chararray
			,cd_isti_ced77   :chararray
			,ndg_ced77      :chararray
			,cd_abi_ced77   :chararray
			,cd_isti_ced78   :chararray
			,ndg_ced78      :chararray
			,cd_abi_ced78   :chararray
			,cd_isti_ced79   :chararray
			,ndg_ced79      :chararray
			,cd_abi_ced79   :chararray
			,cd_isti_ced80   :chararray
			,ndg_ced80      :chararray
			,cd_abi_ced80   :chararray
			,cd_isti_ced81   :chararray
			,ndg_ced81      :chararray
			,cd_abi_ced81   :chararray
			,cd_isti_ced82   :chararray
			,ndg_ced82      :chararray
			,cd_abi_ced82   :chararray
			,cd_isti_ced83   :chararray
			,ndg_ced83      :chararray
			,cd_abi_ced83   :chararray
			,cd_isti_ced84   :chararray
			,ndg_ced84      :chararray
			,cd_abi_ced84   :chararray
			,cd_isti_ced85   :chararray
			,ndg_ced85      :chararray
			,cd_abi_ced85   :chararray
			,cd_isti_ced86   :chararray
			,ndg_ced86      :chararray
			,cd_abi_ced86   :chararray
			,cd_isti_ced87   :chararray
			,ndg_ced87      :chararray
			,cd_abi_ced87   :chararray
			,cd_isti_ced88   :chararray
			,ndg_ced88      :chararray
			,cd_abi_ced88   :chararray
			,cd_isti_ced89   :chararray
			,ndg_ced89      :chararray
			,cd_abi_ced89   :chararray
			,cd_isti_ced90   :chararray
			,ndg_ced90      :chararray
			,cd_abi_ced90   :chararray
			,cd_isti_ced91   :chararray
			,ndg_ced91      :chararray
			,cd_abi_ced91   :chararray
			,cd_isti_ced92   :chararray
			,ndg_ced92      :chararray
			,cd_abi_ced92   :chararray
			,cd_isti_ced93   :chararray
			,ndg_ced93      :chararray
			,cd_abi_ced93   :chararray
			,cd_isti_ced94   :chararray
			,ndg_ced94      :chararray
			,cd_abi_ced94   :chararray
			,cd_isti_ced95   :chararray
			,ndg_ced95      :chararray
			,cd_abi_ced95   :chararray
			,cd_isti_ced96   :chararray
			,ndg_ced96      :chararray
			,cd_abi_ced96   :chararray
			,cd_isti_ced97   :chararray
			,ndg_ced97      :chararray
			,cd_abi_ced97   :chararray
			,cd_isti_ced98   :chararray
			,ndg_ced98      :chararray
			,cd_abi_ced98   :chararray
			,cd_isti_ced99   :chararray
			,ndg_ced99      :chararray
			,cd_abi_ced99   :chararray
			,cd_isti_ced100   :chararray
			,ndg_ced100      :chararray
			,cd_abi_ced100   :chararray
			,cd_isti_ced101   :chararray
			,ndg_ced101      :chararray
			,cd_abi_ced101   :chararray
			,cd_isti_ced102   :chararray
			,ndg_ced102      :chararray
			,cd_abi_ced102   :chararray
			,cd_isti_ced103   :chararray
			,ndg_ced103      :chararray
			,cd_abi_ced103   :chararray
			,cd_isti_ced104   :chararray
			,ndg_ced104      :chararray
			,cd_abi_ced104   :chararray
			,cd_isti_ced105   :chararray
			,ndg_ced105      :chararray
			,cd_abi_ced105   :chararray
			,cd_isti_ced106   :chararray
			,ndg_ced106      :chararray
			,cd_abi_ced106   :chararray
			,cd_isti_ced107   :chararray
			,ndg_ced107      :chararray
			,cd_abi_ced107   :chararray
			,cd_isti_ced108   :chararray
			,ndg_ced108      :chararray
			,cd_abi_ced108   :chararray
			,cd_isti_ced109   :chararray
			,ndg_ced109      :chararray
			,cd_abi_ced109   :chararray
			,cd_isti_ced110   :chararray
			,ndg_ced110      :chararray
			,cd_abi_ced110   :chararray
			,cd_isti_ced111   :chararray
			,ndg_ced111      :chararray
			,cd_abi_ced111   :chararray
			,cd_isti_ced112   :chararray
			,ndg_ced112      :chararray
			,cd_abi_ced112   :chararray
			,cd_isti_ced113   :chararray
			,ndg_ced113      :chararray
			,cd_abi_ced113   :chararray
			,cd_isti_ced114   :chararray
			,ndg_ced114      :chararray
			,cd_abi_ced114   :chararray
			,cd_isti_ced115   :chararray
			,ndg_ced115      :chararray
			,cd_abi_ced115   :chararray
			,cd_isti_ced116   :chararray
			,ndg_ced116      :chararray
			,cd_abi_ced116   :chararray
			,cd_isti_ced117   :chararray
			,ndg_ced117      :chararray
			,cd_abi_ced117   :chararray
			,cd_isti_ced118   :chararray
			,ndg_ced118      :chararray
			,cd_abi_ced118   :chararray
			,cd_isti_ced119   :chararray
			,ndg_ced119      :chararray
			,cd_abi_ced119   :chararray
			,cd_isti_ced120   :chararray
			,ndg_ced120      :chararray
			,cd_abi_ced120   :chararray
			,cd_isti_ced121   :chararray
			,ndg_ced121      :chararray
			,cd_abi_ced121   :chararray
			,cd_isti_ced122   :chararray
			,ndg_ced122      :chararray
			,cd_abi_ced122   :chararray
			,cd_isti_ced123   :chararray
			,ndg_ced123      :chararray
			,cd_abi_ced123   :chararray
			,cd_isti_ced124   :chararray
			,ndg_ced124      :chararray
			,cd_abi_ced124   :chararray
			,cd_isti_ced125   :chararray
			,ndg_ced125      :chararray
			,cd_abi_ced125   :chararray
			,cd_isti_ced126   :chararray
			,ndg_ced126      :chararray
			,cd_abi_ced126   :chararray
			,cd_isti_ced127   :chararray
			,ndg_ced127      :chararray
			,cd_abi_ced127   :chararray
			,cd_isti_ced128   :chararray
			,ndg_ced128      :chararray
			,cd_abi_ced128   :chararray
			,cd_isti_ced129   :chararray
			,ndg_ced129      :chararray
			,cd_abi_ced129   :chararray
			,cd_isti_ced130   :chararray
			,ndg_ced130      :chararray
			,cd_abi_ced130   :chararray
		);
		
tlbmignw_bag = FOREACH tlbmignw 
               GENERATE  cd_isti_ric
                        ,ndg_ric
                        ,TOBAG(
                                 TOTUPLE(0,cd_isti_ric,ndg_ric)
                                ,TOTUPLE(1,cd_isti_ced01,ndg_ced01)
								,TOTUPLE(2,cd_isti_ced02,ndg_ced02)
								,TOTUPLE(3,cd_isti_ced03,ndg_ced03)
								,TOTUPLE(4,cd_isti_ced04,ndg_ced04)
								,TOTUPLE(5,cd_isti_ced05,ndg_ced05)
								,TOTUPLE(6,cd_isti_ced06,ndg_ced06)
								,TOTUPLE(7,cd_isti_ced07,ndg_ced07)
								,TOTUPLE(8,cd_isti_ced08,ndg_ced08)
								,TOTUPLE(9,cd_isti_ced09,ndg_ced09)
								,TOTUPLE(10,cd_isti_ced10,ndg_ced10)
								,TOTUPLE(11,cd_isti_ced11,ndg_ced11)
								,TOTUPLE(12,cd_isti_ced12,ndg_ced12)
								,TOTUPLE(13,cd_isti_ced13,ndg_ced13)
								,TOTUPLE(14,cd_isti_ced14,ndg_ced14)
								,TOTUPLE(15,cd_isti_ced15,ndg_ced15)
								,TOTUPLE(16,cd_isti_ced16,ndg_ced16)
								,TOTUPLE(17,cd_isti_ced17,ndg_ced17)
								,TOTUPLE(18,cd_isti_ced18,ndg_ced18)
								,TOTUPLE(19,cd_isti_ced19,ndg_ced19)
								,TOTUPLE(20,cd_isti_ced20,ndg_ced20)
								,TOTUPLE(21,cd_isti_ced21,ndg_ced21)
								,TOTUPLE(22,cd_isti_ced22,ndg_ced22)
								,TOTUPLE(23,cd_isti_ced23,ndg_ced23)
								,TOTUPLE(24,cd_isti_ced24,ndg_ced24)
								,TOTUPLE(25,cd_isti_ced25,ndg_ced25)
								,TOTUPLE(26,cd_isti_ced26,ndg_ced26)
								,TOTUPLE(27,cd_isti_ced27,ndg_ced27)
								,TOTUPLE(28,cd_isti_ced28,ndg_ced28)
								,TOTUPLE(29,cd_isti_ced29,ndg_ced29)
								,TOTUPLE(30,cd_isti_ced30,ndg_ced30)
								,TOTUPLE(31,cd_isti_ced31,ndg_ced31)
								,TOTUPLE(32,cd_isti_ced32,ndg_ced32)
								,TOTUPLE(33,cd_isti_ced33,ndg_ced33)
								,TOTUPLE(34,cd_isti_ced34,ndg_ced34)
								,TOTUPLE(35,cd_isti_ced35,ndg_ced35)
								,TOTUPLE(36,cd_isti_ced36,ndg_ced36)
								,TOTUPLE(37,cd_isti_ced37,ndg_ced37)
								,TOTUPLE(38,cd_isti_ced38,ndg_ced38)
								,TOTUPLE(39,cd_isti_ced39,ndg_ced39)
								,TOTUPLE(40,cd_isti_ced40,ndg_ced40)
								,TOTUPLE(41,cd_isti_ced41,ndg_ced41)
								,TOTUPLE(42,cd_isti_ced42,ndg_ced42)
								,TOTUPLE(43,cd_isti_ced43,ndg_ced43)
								,TOTUPLE(44,cd_isti_ced44,ndg_ced44)
								,TOTUPLE(45,cd_isti_ced45,ndg_ced45)
								,TOTUPLE(46,cd_isti_ced46,ndg_ced46)
								,TOTUPLE(47,cd_isti_ced47,ndg_ced47)
								,TOTUPLE(48,cd_isti_ced48,ndg_ced48)
								,TOTUPLE(49,cd_isti_ced49,ndg_ced49)
								,TOTUPLE(50,cd_isti_ced50,ndg_ced50)
								,TOTUPLE(51,cd_isti_ced51,ndg_ced51)
								,TOTUPLE(52,cd_isti_ced52,ndg_ced52)
								,TOTUPLE(53,cd_isti_ced53,ndg_ced53)
								,TOTUPLE(54,cd_isti_ced54,ndg_ced54)
								,TOTUPLE(55,cd_isti_ced55,ndg_ced55)
								,TOTUPLE(56,cd_isti_ced56,ndg_ced56)
								,TOTUPLE(57,cd_isti_ced57,ndg_ced57)
								,TOTUPLE(58,cd_isti_ced58,ndg_ced58)
								,TOTUPLE(59,cd_isti_ced59,ndg_ced59)
								,TOTUPLE(60,cd_isti_ced60,ndg_ced60)
								,TOTUPLE(61,cd_isti_ced61,ndg_ced61)
								,TOTUPLE(62,cd_isti_ced62,ndg_ced62)
								,TOTUPLE(63,cd_isti_ced63,ndg_ced63)
								,TOTUPLE(64,cd_isti_ced64,ndg_ced64)
								,TOTUPLE(65,cd_isti_ced65,ndg_ced65)
								,TOTUPLE(66,cd_isti_ced66,ndg_ced66)
								,TOTUPLE(67,cd_isti_ced67,ndg_ced67)
								,TOTUPLE(68,cd_isti_ced68,ndg_ced68)
								,TOTUPLE(69,cd_isti_ced69,ndg_ced69)
								,TOTUPLE(70,cd_isti_ced70,ndg_ced70)
								,TOTUPLE(71,cd_isti_ced71,ndg_ced71)
								,TOTUPLE(72,cd_isti_ced72,ndg_ced72)
								,TOTUPLE(73,cd_isti_ced73,ndg_ced73)
								,TOTUPLE(74,cd_isti_ced74,ndg_ced74)
								,TOTUPLE(75,cd_isti_ced75,ndg_ced75)
								,TOTUPLE(76,cd_isti_ced76,ndg_ced76)
								,TOTUPLE(77,cd_isti_ced77,ndg_ced77)
								,TOTUPLE(78,cd_isti_ced78,ndg_ced78)
								,TOTUPLE(79,cd_isti_ced79,ndg_ced79)
								,TOTUPLE(80,cd_isti_ced80,ndg_ced80)
								,TOTUPLE(81,cd_isti_ced81,ndg_ced81)
								,TOTUPLE(82,cd_isti_ced82,ndg_ced82)
								,TOTUPLE(83,cd_isti_ced83,ndg_ced83)
								,TOTUPLE(84,cd_isti_ced84,ndg_ced84)
								,TOTUPLE(85,cd_isti_ced85,ndg_ced85)
								,TOTUPLE(86,cd_isti_ced86,ndg_ced86)
								,TOTUPLE(87,cd_isti_ced87,ndg_ced87)
								,TOTUPLE(88,cd_isti_ced88,ndg_ced88)
								,TOTUPLE(89,cd_isti_ced89,ndg_ced89)
								,TOTUPLE(90,cd_isti_ced90,ndg_ced90)
								,TOTUPLE(91,cd_isti_ced91,ndg_ced91)
								,TOTUPLE(92,cd_isti_ced92,ndg_ced92)
								,TOTUPLE(93,cd_isti_ced93,ndg_ced93)
								,TOTUPLE(94,cd_isti_ced94,ndg_ced94)
								,TOTUPLE(95,cd_isti_ced95,ndg_ced95)
								,TOTUPLE(96,cd_isti_ced96,ndg_ced96)
								,TOTUPLE(97,cd_isti_ced97,ndg_ced97)
								,TOTUPLE(98,cd_isti_ced98,ndg_ced98)
								,TOTUPLE(99,cd_isti_ced99,ndg_ced99)
								,TOTUPLE(100,cd_isti_ced100,ndg_ced100)
								,TOTUPLE(101,cd_isti_ced101,ndg_ced101)
								,TOTUPLE(102,cd_isti_ced102,ndg_ced102)
								,TOTUPLE(103,cd_isti_ced103,ndg_ced103)
								,TOTUPLE(104,cd_isti_ced104,ndg_ced104)
								,TOTUPLE(105,cd_isti_ced105,ndg_ced105)
								,TOTUPLE(106,cd_isti_ced106,ndg_ced106)
								,TOTUPLE(107,cd_isti_ced107,ndg_ced107)
								,TOTUPLE(108,cd_isti_ced108,ndg_ced108)
								,TOTUPLE(109,cd_isti_ced109,ndg_ced109)
								,TOTUPLE(110,cd_isti_ced110,ndg_ced110)
								,TOTUPLE(111,cd_isti_ced111,ndg_ced111)
								,TOTUPLE(112,cd_isti_ced112,ndg_ced112)
								,TOTUPLE(113,cd_isti_ced113,ndg_ced113)
								,TOTUPLE(114,cd_isti_ced114,ndg_ced114)
								,TOTUPLE(115,cd_isti_ced115,ndg_ced115)
								,TOTUPLE(116,cd_isti_ced116,ndg_ced116)
								,TOTUPLE(117,cd_isti_ced117,ndg_ced117)
								,TOTUPLE(118,cd_isti_ced118,ndg_ced118)
								,TOTUPLE(119,cd_isti_ced119,ndg_ced119)
								,TOTUPLE(120,cd_isti_ced120,ndg_ced120)
								,TOTUPLE(121,cd_isti_ced121,ndg_ced121)
								,TOTUPLE(122,cd_isti_ced122,ndg_ced122)
								,TOTUPLE(123,cd_isti_ced123,ndg_ced123)
								,TOTUPLE(124,cd_isti_ced124,ndg_ced124)
								,TOTUPLE(125,cd_isti_ced125,ndg_ced125)
								,TOTUPLE(126,cd_isti_ced126,ndg_ced126)
								,TOTUPLE(127,cd_isti_ced127,ndg_ced127)
								,TOTUPLE(128,cd_isti_ced128,ndg_ced128)
								,TOTUPLE(129,cd_isti_ced129,ndg_ced129)
								,TOTUPLE(130,cd_isti_ced130,ndg_ced130)
                              ) as toPivot;		
                              
                              
tlbmignw_pivoted = FOREACH tlbmignw_bag
                   GENERATE 
                            cd_isti_ric
                           ,ndg_ric
                           ,FLATTEN(toPivot)    
                          ;
               
/*Filtro tutti i record senza cd_isti che sono le coppie vuote. Il campo si chiama 01 perchè nella pivot lo chiama come la prima coppia ma 
in realtà contiene tutti i dati
Lo schema è:
grunt> describe tlbmignw_pivoted;
tlbmignw_pivoted: {cd_isti_ric: chararray,ndg_ric: chararray,int,toPivot::cd_isti_ced01: chararray,toPivot::ndg_ced01: chararray}
 */                          
tlbmignw_pivoted_filterd = FILTER  tlbmignw_bag BY toPivot::cd_isti_ced01<>'00000';                                                               
STORE  tlbmignw_pivoted INTO '$tlbmignw_pivoted_outdir';
                         

		
