import datetime
import json
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task


# TODO this is hardcoded, sanple data - the actual data should be fetched from S3
sample_data = '''
Number,Digimon,Stage,Type,Attribute,Memory,Equip Slots,Lv 50 HP,Lv50 SP,Lv50 Atk,Lv50 Def,Lv50 Int,Lv50 Spd
1,Kuramon,Baby,Free,Neutral,2,0,590,77,79,69,68,95
2,Pabumon,Baby,Free,Neutral,2,0,950,62,76,76,69,68
3,Punimon,Baby,Free,Neutral,2,0,870,50,97,87,50,75
4,Botamon,Baby,Free,Neutral,2,0,690,68,77,95,76,61
5,Poyomon,Baby,Free,Neutral,2,0,540,98,54,59,95,86
6,Koromon,In-Training,Free,Fire,3,0,940,52,109,93,52,76
7,Tanemon,In-Training,Free,Plant,3,0,1030,64,85,82,73,69
8,Tsunomon,In-Training,Free,Earth,3,0,930,54,107,92,54,76
9,Tsumemon,In-Training,Free,Dark,3,0,930,64,108,64,54,93
10,Tokomon,In-Training,Free,Neutral,3,0,640,86,76,74,74,103
11,Nyaromon,In-Training,Free,Light,3,0,540,107,54,64,103,94
12,Pagumon,In-Training,Free,Dark,3,0,550,103,60,63,102,93
13,Yokomon,In-Training,Free,Plant,3,0,1040,64,82,82,75,69
14,Bukamon,In-Training,Free,Water,3,0,830,93,54,74,103,69
15,Motimon,In-Training,Free,Neutral,3,0,1030,63,82,81,78,69
16,Wanyamon,In-Training,Free,Wind,3,0,830,82,79,75,75,82
17,Agumon,Rookie,Vaccine,Fire,5,1,1030,59,131,103,54,86
18,Agumon (Blk),Rookie,Virus,Fire,5,1,1020,56,124,108,56,85
19,Armadillomon,Rookie,Free,Earth,4,1,1160,85,67,111,65,72
20,Impmon,Rookie,Virus,Dark,5,1,530,114,83,65,114,102
21,Elecmon,Rookie,Data,Electric,4,1,930,93,82,79,79,90
22,Otamamon,Rookie,Virus,Water,4,1,930,105,52,75,113,78
23,Gaomon,Rookie,Data,Neutral,5,1,1030,69,118,74,66,101
24,Gazimon,Rookie,Virus,Dark,4,1,970,71,123,64,59,102
25,Gabumon,Rookie,Data,Fire,5,1,980,88,94,81,79,91
26,Gabumon (Blk),Rookie,Virus,Fire,5,1,950,83,99,89,74,91
27,Guilmon,Rookie,Virus,Fire,5,1,1050,69,133,74,54,101
28,Kudamon,Rookie,Vaccine,Light,5,1,590,128,53,74,117,105
29,Keramon,Rookie,Free,Dark,5,1,1030,74,123,69,61,101
30,Gotsumon,Rookie,Data,Earth,5,1,790,79,93,118,90,72
31,Goblimon,Rookie,Virus,Earth,4,1,1050,51,115,110,51,84
32,Gomamon,Rookie,Vaccine,Water,5,1,1160,69,93,93,81,79
33,Syakomon,Rookie,Virus,Water,4,1,870,100,53,86,112,78
34,Solarmon,Rookie,Vaccine,Light,4,1,1030,88,69,108,71,77
35,Terriermon,Rookie,Vaccine,Wind,4,1,690,93,84,75,84,112
36,Tentomon,Rookie,Vaccine,Plant,4,1,750,79,86,110,93,73
37,ToyAgumon,Rookie,Vaccine,Neutral,4,1,1110,87,72,112,63,71
38,Dorumon,Rookie,Data,Neutral,5,1,1020,65,128,79,56,101
39,Hagurumon,Rookie,Virus,Electric,4,1,1090,91,66,110,69,71
40,Patamon,Rookie,Data,Wind,4,1,880,93,79,74,92,90
41,Hackmon,Rookie,Data,Fire,5,1,1030,59,118,108,63,85
42,Palmon,Rookie,Data,Plant,5,1,1140,65,103,90,80,79
43,DemiDevimon,Rookie,Virus,Dark,4,1,650,87,89,76,89,111
44,Biyomon,Rookie,Vaccine,Wind,4,1,830,93,85,79,85,91
45,Falcomon,Rookie,Vaccine,Wind,5,1,740,93,94,79,79,113
46,Veemon,Rookie,Free,Neutral,5,1,1040,74,130,74,53,101
47,Salamon,Rookie,Vaccine,Light,4,1,540,118,59,69,119,97
48,Betamon,Rookie,Virus,Water,4,1,870,101,61,76,113,78
49,Hawkmon,Rookie,Free,Wind,5,1,690,88,99,79,83,114
50,Lalamon,Rookie,Data,Plant,4,1,1100,74,87,87,79,79
51,Lucemon,Rookie,Vaccine,Light,14,1,1230,148,59,104,208,119
52,Renamon,Rookie,Data,Plant,5,1,930,93,89,74,89,93
53,Lopmon,Rookie,Data,Earth,4,1,790,79,103,68,103,85
54,Wormmon,Rookie,Free,Plant,4,1,760,76,92,111,90,71
55,IceDevimon,Champion,Virus,Water,8,1,990,94,140,89,118,92
56,Aquilamon,Champion,Free,Wind,8,1,840,108,109,89,89,143
57,Ankylomon,Champion,Free,Earth,6,2,1330,98,89,133,79,79
58,Ikkakumon,Champion,Vaccine,Water,8,1,1330,84,118,102,94,90
59,Wizardmon,Champion,Data,Dark,8,1,690,138,74,79,143,118
60,Woodmon,Champion,Virus,Plant,6,2,1480,74,109,103,89,88
61,ExVeemon,Champion,Free,Neutral,8,1,1030,118,104,94,94,118
62,Angemon,Champion,Vaccine,Light,8,1,940,94,128,89,128,99
63,Ogremon,Champion,Virus,Earth,8,1,1230,64,155,118,64,97
64,Guardromon,Champion,Virus,Electric,6,2,990,84,108,138,103,79
65,GaoGamon,Champion,Data,Wind,8,1,740,108,109,94,99,138
66,Kabuterimon,Champion,Vaccine,Plant,6,2,890,89,108,128,116,81
67,ShellNumemon,Champion,Virus,Water,6,2,1280,103,79,148,74,79
68,Gargomon,Champion,Vaccine,Electric,6,2,1030,103,109,99,89,108
69,Garurumon,Champion,Vaccine,Fire,8,1,890,108,99,94,94,138
70,Garurumon (Blk),Champion,Virus,Fire,8,1,890,108,109,104,79,133
71,Kyubimon,Champion,Data,Fire,8,1,740,138,59,84,138,128
72,Growlmon,Champion,Virus,Fire,8,1,1180,79,143,113,69,109
73,Kurisarimon,Champion,Free,Dark,8,1,1280,84,153,79,64,113
74,Greymon,Champion,Vaccine,Fire,8,1,1230,74,148,118,64,104
75,Greymon (Blue),Champion,Virus,Fire,8,1,1280,74,153,118,59,99
76,Clockmon,Champion,Data,Electric,6,2,1030,118,64,101,133,92
77,Kuwagamon,Champion,Virus,Plant,6,2,1180,69,153,113,59,99
78,Gekomon,Champion,Virus,Water,6,2,1130,123,68,89,128,90
79,Geremon,Champion,Virus,Electric,6,2,1380,99,104,128,64,78
80,GoldNumemon,Champion,Virus,Light,6,2,1130,124,59,84,143,88
81,Cyclonemon,Champion,Virus,Earth,6,2,940,84,131,128,93,81
82,Sunflowmon,Champion,Data,Plant,6,2,1180,113,64,89,141,86
83,Seadramon,Champion,Data,Water,6,2,1080,118,64,99,134,88
84,GeoGreymon,Champion,Vaccine,Fire,8,1,1330,89,143,84,64,118
85,Sukamon,Champion,Virus,Earth,6,2,1430,98,89,133,69,79
86,Starmon,Champion,Data,Neutral,6,2,1080,98,104,109,91,101
87,Stingmon,Champion,Free,Plant,8,1,1130,84,143,74,74,133
88,Socerimon,Champion,Vaccine,Water,8,1,1030,123,64,93,148,90
89,Tankmon,Champion,Data,Electric,6,2,940,84,113,141,98,81
90,Tyrannomon,Champion,Data,Fire,6,2,1230,59,148,125,59,97
91,Gatomon,Champion,Vaccine,Light,8,1,640,143,69,79,143,123
92,Devimon,Champion,Virus,Dark,8,1,990,94,133,84,125,97
93,Togemon,Champion,Data,Plant,8,1,1330,84,108,113,93,90
94,Dorugamon,Champion,Data,Earth,8,1,1180,84,138,89,69,123
95,Nanimon,Champion,Virus,Earth,6,2,1070,84,108,133,98,81
96,Numemon,Champion,Virus,Earth,6,2,1380,99,84,138,69,83
97,Birdramon,Champion,Vaccine,Fire,8,1,940,113,94,84,109,128
98,Bakemon,Champion,Virus,Dark,6,2,590,148,64,74,138,128
99,Veedramon,Champion,Vaccine,Wind,8,1,1180,84,138,113,64,114
100,PlatinumSukamon,Champion,Virus,Neutral,6,2,1380,98,79,138,79,79
101,BlackGatomon,Champion,Virus,Dark,8,1,690,133,84,84,133,118
102,Vegiemon,Champion,Virus,Plant,6,2,1380,79,113,106,87,88
103,Peckmon,Champion,Vaccine,Wind,8,1,790,113,104,84,94,148
104,Meramon,Champion,Data,Fire,6,2,1130,69,138,113,79,99
105,Frigimon,Champion,Vaccine,Water,6,2,1380,83,103,98,99,90
106,Leomon,Champion,Vaccine,Earth,8,1,1180,69,143,123,71,97
107,Reppamon,Champion,Vaccine,Light,8,1,790,118,94,99,99,143
108,Waspmon,Champion,Virus,Electric,6,2,1180,74,133,99,74,113
109,MegaKabuterimon,Ultimate,Vaccine,Plant,12,2,1430,115,94,163,109,92
110,Antylamon,Ultimate,Data,Neutral,12,2,940,123,124,109,114,168
111,Andromon,Ultimate,Vaccine,Electric,12,2,1040,94,133,157,133,95
112,Meteormon,Ultimate,Data,Earth,12,2,1090,104,123,163,128,89
113,Infermon,Ultimate,Free,Dark,14,1,1330,99,198,89,74,153
114,Myotismon,Ultimate,Virus,Dark,14,1,1290,113,148,99,148,110
115,AeroVeedramon,Ultimate,Vaccine,Wind,14,1,1430,94,163,99,94,153
116,Etemon,Ultimate,Virus,Dark,12,2,1130,133,104,119,129,133
117,Angewomon,Ultimate,Vaccine,Light,14,1,890,163,69,94,188,143
118,Okuwamon,Ultimate,Virus,Plant,12,2,1330,74,158,158,74,119
119,Garudamon,Ultimate,Vaccine,Fire,12,2,1040,123,124,109,129,143
120,Gigadramon,Ultimate,Virus,Wind,12,2,1240,94,137,148,113,100
121,CannonBeemon,Ultimate,Virus,Electric,12,2,990,123,129,139,99,143
122,GrapLeomon,Ultimate,Vaccine,Electric,12,2,1580,89,163,99,79,143
123,Cyberdramon,Ultimate,Vaccine,Dark,14,1,1480,81,173,143,79,122
124,Shakkoumon,Ultimate,Free,Light,14,1,1530,135,84,158,139,92
125,Cherrymon,Ultimate,Virus,Plant,12,2,1630,108,113,133,114,100
126,Silphymon,Ultimate,Free,Wind,14,1,1040,138,119,119,124,158
127,SuperStarmon,Ultimate,Data,Light,12,2,1180,128,122,134,109,120
128,SkullGreymon,Ultimate,Virus,Dark,14,1,1230,79,203,153,69,119
129,Zudomon,Ultimate,Vaccine,Water,12,2,1630,84,150,128,104,102
130,Taomon,Ultimate,Data,Dark,12,2,990,148,69,104,173,138
131,Chirinmon,Ultimate,Vaccine,Light,14,1,940,133,124,119,124,168
132,Digitamamon,Ultimate,Data,Neutral,12,2,1380,89,128,148,111,102
133,SkullMeramon,Ultimate,Data,Fire,12,2,1530,79,183,133,70,113
134,ShogunGekomon,Ultimate,Virus,Water,12,2,1980,96,113,113,99,97
135,DoruGreymon,Ultimate,Data,Fire,14,1,1480,84,161,153,84,116
136,Knightmon,Ultimate,Data,Neutral,12,2,1140,109,135,158,123,92
137,Datamon,Ultimate,Virus,Electric,12,2,1180,133,74,114,175,102
138,Paildramon,Ultimate,Free,Neutral,14,1,1280,133,139,124,109,128
139,Panjyamon,Ultimate,Vaccine,Water,12,2,1280,128,124,114,111,126
140,Pumpkinmon,Ultimate,Data,Earth,10,3,1480,97,123,108,119,111
141,Piximon,Ultimate,Data,Light,12,2,990,123,104,104,134,153
142,BlackKingNumemon,Ultimate,Virus,Dark,10,3,1580,113,89,168,89,89
143,BlueMeramon,Ultimate,Virus,Fire,12,2,1140,109,148,94,133,119
144,Vademon,Ultimate,Virus,Dark,10,3,1130,165,64,94,173,97
145,Whamon,Ultimate,Vaccine,Water,12,2,1680,93,123,123,124,100
146,MagnaAngemon,Ultimate,Vaccine,Light,14,1,1180,143,98,119,163,105
147,MachGaogamon,Ultimate,Data,Wind,12,2,1480,89,158,89,89,158
148,Mamemon,Ultimate,Data,Earth,10,3,1480,113,111,153,84,97
149,MegaSeadramon,Ultimate,Data,Water,12,2,1330,138,86,114,158,102
150,Megadramon,Ultimate,Virus,Wind,12,2,1430,79,158,148,69,119
151,WarGrowlmon,Ultimate,Virus,Fire,14,1,1430,84,178,138,87,116
152,MetalGreymon,Ultimate,Vaccine,Fire,14,1,1530,84,168,148,80,113
153,MetalGreymon (Blue),Ultimate,Virus,Fire,14,1,1670,84,173,143,69,110
154,MetalTyrannomon,Ultimate,Virus,Electric,12,2,1090,104,130,178,118,92
155,MetalMamemon,Ultimate,Data,Electric,10,3,1040,99,123,153,128,103
156,Monzaemon,Ultimate,Vaccine,Neutral,12,2,1580,93,128,118,119,100
157,Crowmon,Ultimate,Vaccine,Wind,12,2,890,128,119,104,119,173
158,RizeGreymon,Ultimate,Vaccine,Fire,14,1,1530,94,178,109,69,143
159,Lilamon,Ultimate,Data,Plant,12,2,1280,148,69,104,168,114
160,Rapidmon,Ultimate,Vaccine,Electric,12,2,1180,113,114,109,119,143
161,Lillymon,Ultimate,Data,Plant,12,2,890,153,74,94,163,158
162,Lucemon FM,Ultimate,Virus,Neutral,22,1,1390,139,163,114,203,139
163,LadyDevimon,Ultimate,Virus,Dark,14,1,890,163,99,94,158,143
164,WereGarurumon,Ultimate,Vaccine,Earth,12,2,1430,89,178,89,79,153
165,WereGarurumon (Blk),Ultimate,Virus,Earth,12,2,1480,79,183,104,69,148
166,Wisemon,Ultimate,Virus,Dark,12,2,790,168,69,84,198,133
167,Alphamon,Mega,Vaccine,Neutral,22,1,1390,128,158,183,158,130
168,UlforceVeedramon,Mega,Vaccine,Wind,22,1,1680,129,188,109,104,198
169,Ebemon,Mega,Virus,Electric,16,3,1230,178,74,114,198,129
170,Imperialdramon DM,Mega,Free,Fire,20,2,1730,143,139,139,139,148
171,Imperialdramon FM,Mega,Free,Neutral,20,2,1780,114,198,124,114,153
172,Vikemon,Mega,Free,Water,18,3,1780,105,158,143,129,133
173,VenomMyotismon,Mega,Virus,Dark,20,2,1540,120,193,104,148,138
174,WarGreymon,Mega,Vaccine,Fire,20,2,1630,98,193,163,99,140
175,Examon,Mega,Data,Wind,22,1,1630,148,174,129,129,153
176,Ophanimon,Mega,Vaccine,Light,20,2,840,183,104,164,193,153
177,Gaiomon,Mega,Virus,Fire,18,3,1630,99,203,129,94,158
178,ChaosGallantmon,Mega,Virus,Dark,22,1,1340,139,178,139,163,144
179,Gankoomon,Mega,Data,Fire,22,1,2080,90,188,163,109,138
180,Kuzuhamon,Mega,Data,Dark,18,3,1380,163,84,129,193,139
181,GranKuwagamon,Mega,Virus,Plant,18,3,1530,88,178,178,89,140
182,GroundLocomon,Mega,Data,Electric,18,3,1140,114,144,213,133,128
183,Craniamon,Mega,Vaccine,Earth,22,1,1630,143,124,208,134,124
184,Kerpymon (Good),Mega,Vaccine,Light,20,1,1290,188,94,104,208,158
185,SaberLeomon,Mega,Data,Wind,18,3,1680,99,228,104,84,163
186,Sakuyamon,Mega,Data,Light,18,3,990,178,94,114,188,173
187,Jesmon,Mega,Data,Neutral,22,1,1480,119,198,149,114,168
188,ShineGreymon,Mega,Vaccine,Light,20,2,1880,114,203,109,84,158
189,Justimon,Mega,Vaccine,Light,18,3,1530,99,193,158,99,144
190,Kentaurosmon,Mega,Vaccine,Light,22,1,1140,153,139,154,154,183
191,Seraphimon,Mega,Vaccine,Light,20,2,1480,162,94,144,198,135
192,MegaGargomon,Mega,Vaccine,Electric,18,3,1430,132,149,139,119,144
193,TigerVespamon,Mega,Virus,Electric,18,3,1630,104,193,114,99,173
194,Titamon,Mega,Virus,Earth,18,3,1930,99,183,128,114,129
195,Dianamon,Mega,Data,Water,20,1,790,178,89,154,198,183
196,Diaboromon,Mega,Free,Dark,20,2,1680,114,243,104,79,173
197,Creepymon,Mega,Virus,Dark,22,1,1440,133,183,114,183,140
198,Gallantmon,Mega,Virus,Light,22,1,1480,148,149,154,149,148
199,Dynasmon,Mega,Data,Wind,22,1,1680,114,213,129,94,178
200,Leopardmon,Mega,Data,Light,22,1,990,188,124,114,183,188
201,Leopardmon LM,Mega,Data,Earth,25,1,1290,153,159,129,139,218
202,HiAndromon,Mega,Vaccine,Electric,18,3,1190,114,153,178,138,124
203,Barbamon,Mega,Virus,Dark,22,1,1330,184,84,129,233,133
204,BanchoLeomon,Mega,Vaccine,Earth,18,3,1630,84,193,188,80,138
205,Piedmon,Mega,Virus,Dark,18,3,890,178,129,104,183,163
206,Puppetmon,Mega,Virus,Plant,18,3,1140,114,163,163,148,124
207,PlatinumNumemon,Mega,Virus,Neutral,16,3,1830,132,94,178,109,120
208,BlackWarGreymon,Mega,Virus,Fire,20,2,1730,93,183,178,89,140
209,PrinceMamemon,Mega,Data,Neutral,16,3,1630,137,104,173,109,130
210,Plesiomon,Mega,Data,Water,18,3,1680,153,74,114,188,129
211,HerculesKabuterimon,Mega,Vaccine,Plant,18,3,1680,128,114,168,124,124
212,Beelzemon,Mega,Virus,Dark,22,1,1680,114,228,119,99,168
213,Belphemon SM,Mega,Virus,Dark,22,1,1730,178,89,114,203,139
214,Hououmon,Mega,Vaccine,Fire,18,3,1390,173,84,99,193,158
215,Magnadramon,Mega,Vaccine,Light,20,2,1880,168,89,124,183,129
216,Boltmon,Mega,Data,Electric,18,3,1580,83,198,163,84,140
217,Mastemon,Mega,Vaccine,Neutral,22,1,1340,144,173,134,173,149
218,MarineAngemon,Mega,Vaccine,Water,18,3,1190,198,64,89,203,153
219,Minervamon,Mega,Virus,Neutral,20,1,1580,114,208,114,119,168
220,MirageGaogamon,Mega,Data,Wind,18,3,1480,114,183,114,109,178
221,Machinedramon,Mega,Virus,Electric,18,3,1240,114,173,183,128,124
222,MetalEtemon,Mega,Virus,Earth,18,3,1630,128,134,188,89,124
223,MetalGarurumon,Mega,Data,Water,18,3,1140,143,154,129,129,178
224,MetalGarurumon (Blk),Mega,Virus,Electric,18,3,1190,128,163,173,133,130
225,MetalSeadramon,Mega,Data,Water,18,3,1430,148,99,139,168,129
226,RustTyranomon,Mega,Virus,Electric,18,3,1680,83,218,163,74,140
227,Leviamon,Mega,Virus,Water,22,1,1730,120,168,158,144,133
228,Lilithmon,Mega,Virus,Dark,22,1,940,203,99,104,223,173
229,Ravemon,Mega,Vaccine,Wind,18,3,1040,143,139,119,139,203
230,Crusadermon,Mega,Virus,Dark,22,1,1240,153,144,139,144,193
231,Rosemon,Mega,Data,Plant,18,3,1330,147,144,129,149,144
232,Lotosmon,Mega,Data,Plant,18,3,940,188,74,109,213,168
233,Imperialdramon PM,Ultra,Vaccine,Light,25,1,1530,158,154,154,154,153
234,Omnimon,Ultra,Vaccine,Light,25,1,1680,104,208,168,134,144
235,Omnimon Zwart,Ultra,Vaccine,Dark,25,1,1490,139,153,193,158,134
236,Belphemon RM,Ultra,Virus,Dark,25,0,1780,84,247,168,109,140
237,Lucemon SM,Ultra,Virus,Dark,25,0,1490,173,89,124,233,158
238,Flamedramon,Armor,Free,Fire,8,3,1130,93,119,99,89,138
239,Magnamon,Armor,Free,Earth,22,2,1240,124,168,208,148,124
240,Rapidmon (Armor),Armor,Vaccine,Light,18,3,1140,114,158,178,158,124
241,Kerpymon (Blk),Mega,Virus,Dark,20,1,1290,188,94,94,223,153
242,Beelzemon BM,Mega,Virus,Dark,25,1,1680,114,238,124,104,178
243,Darkdramon,Mega,Virus,Electric,18,3,1580,94,188,148,99,139
244,Chaosmon,Ultra,Vaccine,Neutral,25,2,1080,129,318,94,89,188
245,Valkyrimon,Mega,Free,Wind,18,3,1330,139,148,129,129,168
246,ShineGreymon BM,Mega,Vaccine,Fire,22,2,1980,114,228,104,84,168
247,MirageGaogamon BM,Mega,Data,Light,20,2,1440,124,178,104,158,174
248,Ravemon BM,Mega,Vaccine,Wind,20,2,1040,133,149,139,144,213
249,Rosemon BM,Mega,Data,Plant,20,2,1480,143,149,139,159,143
'''

@task(task_id="download the data")
def download_data_task():
    # TODO this is using hardcoded-data instead of a download
    with open('/usr/local/tmp/data.csv', 'w') as f:
        f.write(sample_data)


with DAG(
    dag_id="my_dag_name1",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
):
    download_task = download_data_task()
    dummy_task = EmptyOperator(task_id="Dummy task")
    download_task >> dummy_task
