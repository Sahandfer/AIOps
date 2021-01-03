import pandas as pd
import numpy as np
import itertools

table = pd.DataFrame({'docker_001': {'db_003': np.nan, 'db_007': 0.5774105401493298, 'db_009': 0.8024867054104673, 'docker_001': 0.8274794282774007, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 0.0, 'docker_006': 0.0, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 0.49804130255212026, 'os_021': np.nan, 'os_022': np.nan}, 'docker_002': {'db_003': np.nan, 'db_007': 0.0, 'db_009': 0.0, 'docker_001': np.nan, 'docker_002': 0.5203513126365694, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 0.0, 'docker_006': 0.0, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 0.0, 'os_021': np.nan, 'os_022': np.nan}, 'docker_003': {'db_003': np.nan, 'db_007': 0.6320367718397758, 'db_009': 0.0, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 8.20391599807121, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 29.460163120993915, 'docker_008': 0.635306471564878, 'fly_remote_001': 0.5696951760875999, 'os_021': np.nan, 'os_022': np.nan}, 'docker_004': {'db_003': np.nan, 'db_007': 0.0, 'db_009': 0.0, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': 0.0, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 8.365208925049014, 'docker_008': 0.0, 'fly_remote_001': 0.6111427835247991, 'os_021': np.nan, 'os_022': np.nan}, 'docker_005': {'db_003': 0.6706873370206965, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 0.3136523786056656, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_006': {'db_003': 0.3737358321851091, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': 0.2851554449088065, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_007': {'db_003': 0.32781962904477485, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 0.20938172835579844, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_008': {'db_003': 0.3775030882620795, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': 0.17300481549727048, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'os_021': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 12.301575689751774, 'docker_004': 1.4967385141486598, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': 0.4004707139510223, 'os_022': np.nan}, 'os_022': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': 4.66153058334673, 'docker_002': 4.981435815966565, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': 1.583862621360032}})

t2 = pd.DataFrame({'docker_001': {'db_003': np.nan, 'db_007': 0.6873751833688985, 'db_009': 0.0, 'docker_001': 0.7417640942560333, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 0.0, 'docker_006': 0.8503406025958373, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 0.0, 'os_021': np.nan, 'os_022': np.nan}, 'docker_002': {'db_003': np.nan, 'db_007': 0.0, 'db_009': 0.0, 'docker_001': np.nan, 'docker_002': 0.0, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 0.8021357468430206, 'docker_006': 0.0, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 0.0, 'os_021': np.nan, 'os_022': np.nan}, 'docker_003': {'db_003': np.nan, 'db_007': 0.5238733956673712, 'db_009': 0.0, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 26.70843007757841, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 153.67030663681567, 'docker_008': 0.0, 'fly_remote_001': 1.1990441552195739, 'os_021': np.nan, 'os_022': np.nan}, 'docker_004': {'db_003': np.nan, 'db_007': 0.45464297111868146, 'db_009': 0.722859278862883, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': 44.87607917891881, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 99.53236436034116, 'docker_008': 0.6779696055340193, 'fly_remote_001': 0.547915054892073, 'os_021': np.nan, 'os_022': np.nan}, 'docker_005': {'db_003': 0.08357618963699825, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 0.1255736277630985, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_006': {'db_003': 0.0, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': 0.07908818374175959, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_007': {'db_003': 0.06469037019559287, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 0.11285261757000288, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_008': {'db_003': 0.09489095544157534, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': 0.0, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'os_021': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 1.2092571509587517, 'docker_004': 3.7141568476318234, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': 1.817778548180819, 'os_022': np.nan}, 'os_022': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': 2.884695901268098, 'docker_002': 2.457355119592326, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': 1.1479471118633435}})

t3 = pd.DataFrame({'docker_001': {'db_003': np.nan, 'db_007': 0.0, 'db_009': 0.0, 'docker_001': 113.77767379540641, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 11.65057684936712, 'docker_006': 0.0, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 0.0, 'os_021': np.nan, 'os_022': np.nan}, 'docker_002': {'db_003': np.nan, 'db_007': 0.6693498992114044, 'db_009': 0.0, 'docker_001': np.nan, 'docker_002': 4.383874454064449, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 51.14204513198193, 'docker_006': 0.0, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 0.0, 'os_021': np.nan, 'os_022': np.nan}, 'docker_003': {'db_003': np.nan, 'db_007': 0.7468284015662664, 'db_009': 0.0, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 0.0, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 0.0, 'docker_008': 0.7868147856497596, 'fly_remote_001': 1.1153506116491751, 'os_021': np.nan, 'os_022': np.nan}, 'docker_004': {'db_003': np.nan, 'db_007': 0.0, 'db_009': 0.0, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': 0.0, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 0.0, 'docker_008': 0.0, 'fly_remote_001': 0.0, 'os_021': np.nan, 'os_022': np.nan}, 'docker_005': {'db_003': 0.0, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 0.0, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_006': {'db_003': 0.11367357037262366, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': 0.16349200261527708, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_007': {'db_003': 0.1281973460494784, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 0.0, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_008': {'db_003': 0.09727843728396875, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': 0.12043500663937376, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'os_021': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 4.439760205697623, 'docker_004': 4.768727541673832, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': 2.1429877821460703, 'os_022': np.nan}, 'os_022': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': 50.96571938320378, 'docker_002': 5.127336878252419, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': 15.937915836061281}})

t4 = pd.DataFrame({'docker_001': {'db_003': np.nan, 'db_007': 0.0, 'db_009': 0.9838169354588546, 'docker_001': 0.7861391002059482, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 0.0, 'docker_006': 0.0, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 0.0, 'os_021': np.nan, 'os_022': np.nan}, 'docker_002': {'db_003': np.nan, 'db_007': 0.0, 'db_009': 0.9363045757527446, 'docker_001': np.nan, 'docker_002': 1.5127855266378176, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 0.0, 'docker_006': 0.0, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 0.0, 'os_021': np.nan, 'os_022': np.nan}, 'docker_003': {'db_003': np.nan, 'db_007': 0.0, 'db_009': 0.0, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 0.0, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 0.0, 'docker_008': 10.881275149354117, 'fly_remote_001': 0.0, 'os_021': np.nan, 'os_022': np.nan}, 'docker_004': {'db_003': np.nan, 'db_007': 0.0, 'db_009': 0.4775332443260353, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': 0.0, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 0.0, 'docker_008': 5.827862032040949, 'fly_remote_001': 1.0050331637613457, 'os_021': np.nan, 'os_022': np.nan}, 'docker_005': {'db_003': 0.06685900176033355, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 0.0, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_006': {'db_003': 0.11316141426580627, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': 0.0, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_007': {'db_003': 0.1108128629133302, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 0.0, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_008': {'db_003': 0.0, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': 0.14300878246906945, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'os_021': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 1.2403120451532217, 'docker_004': 2.123577287145988, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': 0.35739866324750746, 'os_022': np.nan}, 'os_022': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': 1.982616568372709, 'docker_002': 0.0, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': 4.872634418760364}})

t5 = pd.DataFrame({'docker_001': {'db_003': np.nan, 'db_007': 0.0, 'db_009': 0.7553934348565373, 'docker_001': 1.40120715994194, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 11.369111472188324, 'docker_006': 0.0, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 0.4825121467415683, 'os_021': np.nan, 'os_022': np.nan}, 'docker_002': {'db_003': np.nan, 'db_007': 0.6127292209871582, 'db_009': 0.7276790874737986, 'docker_001': np.nan, 'docker_002': 0.9958623682263525, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 2.8491343269180214, 'docker_006': 0.8053690911324938, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 0.0, 'os_021': np.nan, 'os_022': np.nan}, 'docker_003': {'db_003': np.nan, 'db_007': 0.0, 'db_009': 0.0, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 0.0, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 0.5982395665824519, 'docker_008': 0.0, 'fly_remote_001': 0.0, 'os_021': np.nan, 'os_022': np.nan}, 'docker_004': {'db_003': np.nan, 'db_007': 0.0, 'db_009': 0.5590271799420702, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': 0.5014244909585741, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 0.5852287543359498, 'docker_008': 0.6930811991862449, 'fly_remote_001': 0.4135630648136472, 'os_021': np.nan, 'os_022': np.nan}, 'docker_005': {'db_003': 0.0545266510850215, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 0.07490029392586288, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_006': {'db_003': 0.0, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': 0.0, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_007': {'db_003': 0.07360636279647252, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 0.0, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_008': {'db_003': 0.0, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': 0.0, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'os_021': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 0.0, 'docker_004': 0.0, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': 0.363351579364569, 'os_022': np.nan}, 'os_022': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': 4.396017872296664, 'docker_002': 4.4900912770728, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': 2.0234945433334013}})

t6 = pd.DataFrame({'docker_001': {'db_003': np.nan, 'db_007': 5.310297835643403, 'db_009': 6.245314256879353, 'docker_001': 18.881266585704196, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 8.743714338010957, 'docker_006': 7.6382871334722005, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 2.53922591252732, 'os_021': np.nan, 'os_022': np.nan}, 'docker_002': {'db_003': np.nan, 'db_007': 7.019083588983974, 'db_009': 8.00180573923328, 'docker_001': np.nan, 'docker_002': 12.705687433478108, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 7.959962228688019, 'docker_006': 6.252514429005822, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 3.251778360164733, 'os_021': np.nan, 'os_022': np.nan}, 'docker_003': {'db_003': np.nan, 'db_007': 2.5092509538344854, 'db_009': 3.417597959419961, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 4.823370235998539, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 2.994169709691651, 'docker_008': 2.593807089276344, 'fly_remote_001': 1.6422090761497414, 'os_021': np.nan, 'os_022': np.nan}, 'docker_004': {'db_003': np.nan, 'db_007': 3.226104276247126, 'db_009': 2.874116574312004, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': 4.605565084817134, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 4.546652326984044, 'docker_008': 2.659597821495269, 'fly_remote_001': 2.090014060263234, 'os_021': np.nan, 'os_022': np.nan}, 'docker_005': {'db_003': 0.44396701993375587, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 6.084111692264034, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_006': {'db_003': 0.794866376227323, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': 4.2895434914980255, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_007': {'db_003': 0.8296633093328274, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 6.525925043225269, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_008': {'db_003': 1.1118138482006663, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': 2.499713579619135, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'os_021': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 2.002645643798566, 'docker_004': 1.7777691940651206, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': 0.9329346926393927, 'os_022': np.nan}, 'os_022': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': 6.047713392097228, 'docker_002': 3.319573472824696, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': 3.9402455508316256}})

t7 = pd.DataFrame({'docker_001': {'db_003': np.nan, 'db_007': 5.300234892163143, 'db_009': 6.229255138440632, 'docker_001': 19.658483612670395, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 15.502499252360403, 'docker_006': 4.806115666311609, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 1.5350997466222038, 'os_021': np.nan, 'os_022': np.nan}, 'docker_002': {'db_003': np.nan, 'db_007': 7.537939941879237, 'db_009': 7.9851310933507165, 'docker_001': np.nan, 'docker_002': 23.170558683194063, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 13.648816444884657, 'docker_006': 9.819171073562922, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 3.274843895310217, 'os_021': np.nan, 'os_022': np.nan}, 'docker_003': {'db_003': np.nan, 'db_007': 2.48821163580889, 'db_009': 3.123981934536233, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 4.8225659407508115, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 5.088465608465608, 'docker_008': 3.8278817634643407, 'fly_remote_001': 1.9464000154133674, 'os_021': np.nan, 'os_022': np.nan}, 'docker_004': {'db_003': np.nan, 'db_007': 2.8244133307040027, 'db_009': 2.8487272062519997, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': 4.513045353258237, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 3.9028975616851307, 'docker_008': 2.4147849223119238, 'fly_remote_001': 2.66107419744372, 'os_021': np.nan, 'os_022': np.nan}, 'docker_005': {'db_003': 0.4428782835038446, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 6.061392247864314, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_006': {'db_003': 1.3877250409165305, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': 4.5119764622209635, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_007': {'db_003': 0.8031020664274813, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 6.383666540927951, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_008': {'db_003': 1.122464285714286, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': 2.483094298352473, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'os_021': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 1.978015845263199, 'docker_004': 1.943854748603352, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': 1.0092397571054232, 'os_022': np.nan}, 'os_022': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': 7.210171919770773, 'docker_002': 2.790735029451583, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': 4.0720777439024385}})

t8 = pd.DataFrame({'docker_001': {'db_003': np.nan, 'db_007': 4.872475449323699, 'db_009': 6.220686897764984, 'docker_001': 18.41637615781417, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 8.024994034855972, 'docker_006': 4.004137240854894, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 1.5713447460781058, 'os_021': np.nan, 'os_022': np.nan}, 'docker_002': {'db_003': np.nan, 'db_007': 6.02, 'db_009': 7.9851310933507165, 'docker_001': np.nan, 'docker_002': 23.170558683194063, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 13.648816444884657, 'docker_006': 8.789574678536102, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': 3.2289603960396036, 'os_021': np.nan, 'os_022': np.nan}, 'docker_003': {'db_003': np.nan, 'db_007': 1.6873059006211184, 'db_009': 2.9910281513527637, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 4.819251626244336, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 5.088465608465608, 'docker_008': 3.366860090264346, 'fly_remote_001': 1.6642539133696195, 'os_021': np.nan, 'os_022': np.nan}, 'docker_004': {'db_003': np.nan, 'db_007': 2.8244133307040027, 'db_009': 2.8487272062519997, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': 4.513045353258237, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 3.9028975616851307, 'docker_008': 2.4147849223119238, 'fly_remote_001': 2.66107419744372, 'os_021': np.nan, 'os_022': np.nan}, 'docker_005': {'db_003': 0.4246516623600474, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': 6.061392247864314, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_006': {'db_003': 0.800278232405892, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': 4.5119764622209635, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_007': {'db_003': 0.7680146647373028, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': 6.259808709433684, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'docker_008': {'db_003': 1.0629788398394748, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': 2.617272444858652, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': np.nan}, 'os_021': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': np.nan, 'docker_002': np.nan, 'docker_003': 2.4198531273740187, 'docker_004': 8.576331969337032, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': 1.9280066821038853, 'os_022': np.nan}, 'os_022': {'db_003': np.nan, 'db_007': np.nan, 'db_009': np.nan, 'docker_001': 5.846180592047645, 'docker_002': 2.8464140350877196, 'docker_003': np.nan, 'docker_004': np.nan, 'docker_005': np.nan, 'docker_006': np.nan, 'docker_007': np.nan, 'docker_008': np.nan, 'fly_remote_001': np.nan, 'os_021': np.nan, 'os_022': 3.69313208115547}})

table = t8

dockers = ['docker_001', 'docker_002', 'docker_003', 'docker_004',
                'docker_005', 'docker_006', 'docker_007', 'docker_008']
docker_hosts = ['os_017', 'os_018', 'os_019', 'os_020']

docker_kpi_names = ['container_cpu_used', None]
os_kpi_names = ['Sent_queue', 'Received_queue']
db_kpi_names = ['Proc_User_Used_Pct', 'Proc_Used_Pct', 'Sess_Connect', 'On_Off_State', 'tnsping_result_time']

docker_lookup_table = {}
for i in range(len(dockers)):
    docker_lookup_table[dockers[i]] = docker_hosts[i % 4]

def find_anomalous_hosts(min_threshold=10):
    '''
    Find Any Anomalous Hosts
    Searches the anomaly chart for the hosts most likely to be causing the anomaly.
    min_threshold: Minimum threshold value for the search. The threshold is the
                    value over which entries are considered anomalous

    Output       : dictionary of anomalous hosts and a dictionary that states if each
                    host has local anomalous behaviour (used for docker hosts)
    '''
    # table = anomaly_chart.copy()
    # get a threshold. Either a qarter of the max value in the entire table or the min_threshold value
    threshold = max(0.2 * table.stack().max(), min_threshold)
    print('The largest value in the anomaly chart is: %f' % table.stack().max())
    print('The threshold is: %f' % threshold)

    local_abnormal = {}
    # these dictionaries store the number of anomalies in the rows/columns of a host respectively
    row_dict = {}
    column_dict = {}
    # these dictionaries store the values of the rows/columns of a host respectively
    row_confidence_dict = {}
    column_confidence_dict = {}

    check_for_most_simple_case = sum(np.array(table.stack()) >= threshold)
    if check_for_most_simple_case == 1:
        for column in table:
            for index, row in table.iterrows():
                if row[column] >= threshold:
                    print('Only 1 anomalous value in the table, so we localise on the row name: %s' % index)
                    return find_anomalous_kpi(index, index == column)

    for column in table:
        for index, row in table.iterrows():
            increment = 0
            if row[column] >= threshold:
                # add a 'count' for each row/column entry that is anomalous using increment
                increment = 1
                # initialise dictionary to avoid erros in the future
                local_abnormal[column] = local_abnormal.get(column, False)
                local_abnormal[index] = local_abnormal.get(index, False)
                if column == index:
                    # if one of the diagonals of the table is anomalous, we mark it here
                    local_abnormal[column] = True

            column_dict[column] = column_dict.get(column, 0)
            column_dict[column] += increment
            column_confidence_dict[column] = column_confidence_dict.get(column, [])
            column_confidence_dict[column].append(row[column])

            row_dict[index] = row_dict.get(index, 0)
            row_dict[index] += increment
            row_confidence_dict[index] = row_confidence_dict.get(index, [])
            row_confidence_dict[index].append(row[column])

    for key, value in column_confidence_dict.items():
        # take the sum of the entries in a column
        column_confidence_dict[key] = np.nansum(value)

    for key, value in row_confidence_dict.items():
        # take the sum of the entries in a row
        row_confidence_dict[key] = np.nansum(value)

    final_dict = {}
    for key in list(row_dict.keys()):
        if key in list(column_dict.keys()):
            # row_dict now contains the mean count of the number of anomalies in the rows and columns of a host
            # use integer division to dismiss hosts with only one anomalous non-diagonal value in the table
            row_dict[key] = (row_dict[key]*2 + column_dict[key]) // 2
            row_confidence_dict[key] = (row_confidence_dict[key] + column_confidence_dict[key]) / 2
        # multiply the mean number of anomlies by the score of the rows/columns of a host
        # if the number of anomalies (row_dict[key]) is 0, we get 0.
        final_dict[key] = row_dict[key] * row_confidence_dict[key]

    for key in set(column_confidence_dict.keys()).difference(set(row_confidence_dict.keys())):
        # if there is missing host data, we might miss out anomalies on weird tables. Thus we
        # check the column keys. This code usually does not run, as the set difference is empty.
        final_dict[key] = column_dict[key] * column_confidence_dict[key]
  
    # sort the potential anomalies by their score
    dodgy_hosts = dict(sorted(final_dict.items(), key=lambda item: item[1], reverse=True))
    # filter out unlikely anomalies by taking 10% of the max of the anomaly scores, or 1
    m = 0.1 * max(list(dodgy_hosts.values())+[10])
    dodgy_hosts = {k: v for k, v in dodgy_hosts.items() if (v > m)}

    output = localize(dodgy_hosts, local_abnormal)
    return output

def find_anomalous_kpi(cmdb_id,  local_abnormal):
    '''
    Find the KPIs Responsible for the Anomaly
    Given an anomalous host, return the root cause KPIs for that host
    in the output format specified by the project.
    cmdb_id        : the cmdb_id of the host
    local_abnormal : boolean indicating if (for docker hosts) the anomaly is
                        on local calls or not

    output         : list to send off to server
    '''
    # two inputs, cmdb_id and local_abnormal. cmdb_id is the host we sent to server, local_abnormal is a boolean used for 'docker' anomalies.
    kpi_names = []
    if 'os' in cmdb_id:
        # os is always just ['Sent_queue', 'Received_queue']
        kpi_names = os_kpi_names
    elif 'docker' in cmdb_id:
        # if the local method is abnormal, i.e. the self-calling function is abnormal, it is a cpu fault
        if local_abnormal:
            kpi_names = ['container_cpu_used']
        else:
            # if the self calling function is not abnormal, it is a network error
            kpi_names = [None]
    else:
        kpi_names = db_kpi_names
        host_data_subset = host_data.loc[(host_data.cmdb_id == cmdb_id) & (host_data.name == 'On_Off_State')]
        check = any(host_data_subset.value < 1)
        if check:
            kpi_names = kpi_names[3:]
        else:
            kpi_names = kpi_names[:3]

    to_be_sent = []
    for kpi in kpi_names:
        to_be_sent.append([cmdb_id, kpi])

    return to_be_sent

def localize(dodgy_host_dict, local_abnormal):
    '''
    Localize the Anomalous Hosts
    Narrow down the anomalous hosts given by 'find_anomalous_hosts' and look for 
    similar hosts or dependancies
    dodgy_host_dict : dictionary in the format {host: score}
    local_abnormal  : dictionary in the format {host: boolean} where the boolean
                        indicates if (for docker hosts) the anomaly occurs on local
                        calls
    
    output          : result to send off to server
    '''
    dodgy_hosts = list(dodgy_host_dict.keys())
    n = len(dodgy_host_dict)
    print('We found %d anomalies, printed below:' % n)
    print(dodgy_host_dict)
    if n < 1:
        return None
    else:
        # create lists containing all the potential os and docker anomalies
        os = [x for x in dodgy_hosts if 'os' in x]
        docker = [y for y in dodgy_hosts if 'docker' in y]

        if len(os) == 2:
            # two os means it must be os_001
            to_be_sent = find_anomalous_kpi('os_001', False)
            return to_be_sent

        if len(docker) >= 2:
            # 2 or more potential docker anomalies, so we check if its a db_003 first
            if len(docker) >= 4:
                if sorted(docker)[-4:] == ['docker_005', 'docker_006', 'docker_007', 'docker_008']:
                    to_be_sent = find_anomalous_kpi('db_003', False)
                    return to_be_sent
            # if we reach here, its not a db_003, so it might be an os_00x
            c = list(itertools.combinations(docker, 2))
            print('The combinations of docker hosts are printed below:')
            print(c)
            for a, b in c:
                shared_host = docker_lookup_table[a]
                if shared_host == docker_lookup_table[b]:
                    to_be_sent = find_anomalous_kpi(
                        shared_host, False)
                    return to_be_sent

        if 'fly' in dodgy_hosts[0]:
            # fly remote means it must be os_009
            to_be_sent = find_anomalous_kpi('os_009', False)
            return to_be_sent
        else:
            # if there are 2 or more potential anomalies and there are no similarities, we simply
            # return the 'most anomalous' one
            to_be_sent = find_anomalous_kpi(
                dodgy_hosts[0], local_abnormal[dodgy_hosts[0]])
            return to_be_sent

print(table)
x = find_anomalous_hosts()
print(x)
