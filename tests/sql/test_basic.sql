
DROP DATABASE test_db;
CREATE DATABASE test_db;
USE test_db;

CREATE TABLE info (
                      C1 INT DEFAULT 10,
                      C2 FLOAT DEFAULT 3.14,
                      C3 VARCHAR(50) DEFAULT 'default for C3',
                      C4 CHAR(50) DEFAULT 'default for C4',
                      C5 INT NOT NULL
);

CREATE TABLE info2 (
                       D1 CHAR(40),
                       D2 VARCHAR(50),
                       D3 FLOAT,
                       D4 INT
);

CREATE TABLE info3 (
                       E1 CHAR(40),
                       E2 VARCHAR(50),
                       E3 FLOAT,
                       E4 INT
);

CREATE TABLE info4 (
                       F1 FLOAT,
                       F2 VARCHAR(50),
                       F3 CHAR(40),
                       F4 INT
);

CREATE TABLE info5 (
                       G1 FLOAT,
                       G2 VARCHAR(50),
                       G3 CHAR(40),
                       G4 INT
);

CREATE TABLE crafted (
                         A INT,
                         B FLOAT,
                         C CHAR(20),
                         D VARCHAR(30)
);

DESC info;
DESC info2;
DESC info3;
DESC info4;
DESC info5;
DESC crafted;

INSERT INTO crafted VALUES (18, NULL, NULL, 'jfhgfa'), (8, NULL, 'rgqh', 'yqiihqq'), (3, NULL, 'ergr', NULL), (NULL, NULL, 'string', 'halo');

INSERT INTO info VALUES (23, 10.265, 'legitimate reviewed exterior', 'synthesis consist religion', 6), (9, 54.978, 'jon societies de formula being', 'periodically shepherd playboy loan restrictions', 21), (12, 25.033, 'sa', 'reef assessments ga module judy licenses brings', 0), (21, 90.971, 'ser ln apple', 'champagne int expiration critical remove', 13), (14, 48.561, 'bleeding enlarge soundtrack', 'thomson downloading', 8), (34, 81.537, 'zope thong fixtures construction census streaming', 'kong', 31), (37, 54.251, 'oriented', 'ultram enters interval dicks', 0), (40, 57.02, 'wait precisely animated inf error shame honors', 'rates substantial ian husband hunter', 22), (27, 7.381, 'boost nude enormous zoning resource advertiser', 'perception anywhere surge adults mayor fine reader', 8), (40, 77.067, 'penis relatives hardly', 'scenarios', 15), (22, 31.826, 'social jun enabling buyers metals', 'plugin psychiatry recording imagine', 30), (34, 98.462, 'caught worldwide beginning fellow weddings', 'fruits pleasure annotated evolution', 26), (35, 80.936, 'ne', 'directly owners firms connected charitable', 8), (18, 97.812, 'antarctica big beam antonio framework districts', 'added lease spas dealer', 12), (37, 95.391, 'quiet establish bidding struct', 'drawings clark telecharger table', 17), (17, 14.587, 'databases calcium auburn sacred cube', 'yes identify davis insert ram withdrawal strings', 38), (20, 87.857, 'friendly', 'ist fc zen munich glasses deborah', 31), (26, 88.881, 'shift played girl twins', 'belly pentium mb lanes faster', 22), (28, 52.672, 's dirt til decisions forbes physicians thrown', 'nos workplace excitement', 36), (21, 78.312, 'henderson colleagues', 'playstation blake seas overhead throws connectors', 3), (25, 20.256, 'ws kenya earning ontario history', 'technological stories collected', 38), (38, 82.494, 'loading harley berry', 'ns dealtime perception speeds synthesis beach', 16), (17, 10.513, 'lafayette cox buying gambling icq attend', 'earnings expense refrigerator silent delaware', 40), (16, 3.985, 'proceedings mon plays hull ebony', 'toolbar penis beth strain', 28), (6, 37.879, 'represents appreciate tits sentence', 'joyce willow og sim winter lexington their maui', 23), (8, 94.797, 'typically queensland', 'stuffed championship synthetic node schemes', 12), (23, 5.915, 'regions illegal hold', 'alike deutsche hdtv settled', 13), (18, 20.484, 'allow molecules support lodge heroes pine jesse', 'bored hydrogen keith protocol jam', 39), (1, 80.271, 'address lender fabrics collectables ch', 'springfield lamp globe mount garlic eu annotation', 7), (39, 37.616, 'hello collected automatically', 'abstracts ext thing', 32), (37, 69.793, 'quite cosmetic attend membrane indians', 'underwear pot vacancies mode frog phd jefferson', 19), (40, 25.271, 'nuclear heel deadline feeling', 'nut programmer energy architectural', 3), (0, 16.295, 'value telecharger enable browsers wheel mary', 'expenses', 39), (9, 94.894, 'clean pictures knives', 'championships might blues one sb headline', 27), (1, 79.531, 'toner nhs hay want quiz session varying', 'garden vocal causing', 14), (25, 11.819, 'policy importantly', 'browsing', 0), (32, 42.88, 'choosing be leather charleston lottery fo', 'albert kazakhstan preferred', 36), (40, 4.169, 'surplus significantly', 'upon allowed circular writing plane', 8), (21, 31.257, 'ceo bee beastiality dee santa finals', 'remark buses', 14), (34, 84.129, 'trash xnxx threatening', 'crime refugees mails attitude', 16), (8, 24.564, 'naturals preservation plaza', 'yale shemale valley disco poor you gadgets', 15), (13, 81.967, 'included expanding', 'expansion neutral chase', 12), (11, 70.048, 'stroke', 'aka june issn london redeem vessels sudden', 27), (17, 70.66, 'captain rhythm', 'mercury surgeons been nose bind holy rage bleeding', 36), (12, 11.814, 'calculators', 'retrieval', 14), (25, 18.922, 'mechanism utc impressive toshiba header', 'remedy postal', 31), (30, 12.381, 'locate ambient undertake', 'alert chassis voices jeff senator', 20), (36, 90.1, 'tag sharing dumb', 'comics rid implement allows techno sort france', 26), (21, 99.379, 'gardens sticky forms christian lead seat biz', 'british annually', 36), (40, 23.598, 'refuse roller mario adapters y abilities mod', 'halo', 18), (20, 63.905, 'gg grounds hotelscom sold circulation meat', 'task cons results directory', 33), (6, 31.06, 'cultural', 'help jersey manufacture jr subjects matched', 10), (35, 35.521, 'zones happen deborah parties markets mitsubishi', 'un twins announcements ext forests', 5), (33, 54.258, 'larger warning oaks removed animal student bears', 'went plant martha auburn wiki geology ranging', 4), (34, 42.841, 'gamecube concerns controls', 'unexpected', 25), (15, 96.683, 'bronze glossary overview largest nr regulated', 'bone clocks epa', 16), (35, 99.732, 'interested', 'show advance works nec received ml outside', 33), (1, 54.713, 'dpi oval nicole', 'naturals llp fired ld florist forced fresh', 14), (37, 43.494, 'web web iso promises reporters conflict', 'nevada lite', 0), (40, 82.424, 'britain album stevens', 'writes kernel intervention sox provide varying', 35), (20, 60.247, 'living gazette lending lonely', 'sleeps fear industry barrier', 4), (37, 38.698, 'pharmaceuticals four extra reporters', 'leslie bridges dt patient unix routine apparent', 28), (8, 30.327, 'confusion fitting', 'commands flights rugs containers angle undefined', 5), (16, 23.741, 'restored', 'toe psychology astronomy', 0), (22, 12.106, 'puzzles greece nm banks url howto remains data', 'brokers extras service pass gs acdbentity climb', 28), (3, 15.617, 'informal busy motorola diy', 'hotels gaming meets', 18), (37, 29.106, 'format', 'proposals provided cassette', 7), (3, 54.105, 'common engine effect', 'object suite coupon', 8), (38, 51.773, 'semester', 'extreme happiness placement holes communication', 13), (37, 40.913, 'qui', 'bathroom prayer leaves become quantity', 7), (2, 81.112, 'corruption', 'exports throwing persian', 36), (19, 41.722, 'jo million doing porsche', 'anger rational little scholarships', 6), (4, 66.857, 'friendship', 'routine exhibition solaris', 2), (14, 23.149, 'geography crimes results writes zone air facility', 'projected meat fishing chancellor', 7), (14, 54.107, 'evident', 'patrol ui wax expiration transmit gain undo', 38), (17, 1.272, 'incentives uruguay ericsson', 'break mpeg shelter luxury activity', 39), (38, 10.458, 'greece', 'workforce sunset fatal decorative little alive inn', 17), (34, 77.349, 'liz bio rid professional behalf', 'fill', 39), (19, 90.563, 'patricia fellow religious continuity knowing', 'vessel fathers shared penalties regime', 18), (25, 93.085, 'coated quantity', 'formal chief jewel', 24), (13, 68.55, 'jean lo stunning mountains edmonton muslims', 'sales snow nice moderate probability allocated', 4), (6, 96.565, 'leon brighton general gulf blogs', 'lives quarter chile greeting waters', 19), (36, 55.529, 'scene dentists traveling weapon carriers', 'confirm lance', 11), (17, 87.417, 'god subsidiaries prototype', 'drink', 28), (4, 78.755, 'cv athens', 'appendix barbie lbs vast back ec accredited', 11), (9, 84.017, 'priority palace brunei so rod ty charts mistress', 'dealt ballot encounter snapshot shadow blend', 5), (28, 51.45, 'cited michelle implied hon retain', 'robertson mens announcements', 23), (14, 40.294, 'es mainland petersburg', 'appointments pot engineer', 17), (30, 21.275, 'scout graphics necessity', 'sensitive eos elements bi architect', 16), (36, 75.896, 'buffalo', 'sensors dog acquire eagles booking', 9), (20, 50.557, 'helping glasgow', 'analyze boom wings tower cab significant isolated', 12), (27, 11.844, 'solve shakespeare', 'christmas reader packaging', 1), (21, 74.361, 'stupid coach computing ie', 'horror headline have fancy assault myrtle wy', 6), (29, 97.206, 'club except babe', 'layout roller forests provide meditation slovakia', 40), (19, 49.617, 'hepatitis jade chef consistency impossible visits', 'erp train', 6), (35, 4.284, 'analyze bishop', 'respond french sense searches commitments', 27), (33, 65.224, 'za homes ion fits ass vendors dp don wichita forms', 'heavily', 17), (7, 54.205, 'conspiracy', 'and th sounds frames continent none chance', 15), (23, 68.271, 'effort body lu organizing terrain advocacy vector', 'grammar operations', 11), (27, 42.835, 'boutique recovered neutral rabbit fully', 'brave canal whilst liked pixels fake save tribunal', 17), (17, 46.858, 'amy pairs worse characters remix angels mere echo', 'ben console', 28), (31, 31.05, 'additions water holmes ecuador measured repair', 'hopes empirical lender named serve array', 35), (21, 64.125, 'and phases causing bruce incident', 'athletic egg causing unable webmasters always', 1), (39, 54.043, 'brush cottage automated evil lyrics valium debate', 'soil jo dl pda june wives', 8), (33, 42.943, 'consideration symbols pumps modes physically', 'degrees estates dog notified', 39), (27, 60.183, 'lens pontiac securely cite shorter cant pdf anyone', 'sc anytime towns classifieds improving ensemble', 27), (4, 48.918, 'forms expand', 'tgp between access', 21), (34, 80.813, 'territory speaks secrets', 'insured brisbane spring returned fundraising', 25), (39, 83.595, 'suck scientific sage user', 'visitors belgium productions', 0), (14, 77.817, 'relationships', 'dryer voyuer', 14), (3, 31.766, 'let hk or taxation walnut mike express ron', 'components fabulous tomatoes mate visitor', 3), (32, 20.734, 'threats dow favorite', 'reporters simply advisor', 21), (13, 65.621, 'lions complaints dry ky combine puts', 'themselves phillips porter soon expenditure', 28), (23, 52.27, 'monica infections assist opinions eagles ed', 'entire recorders', 27), (36, 11.619, 'miles churches exclude bon href examine', 'walter ky alto saver ob largest lightning', 10), (23, 39.075, 'stationery', 'mental cams protest', 27), (37, 45.939, 'clean anniversary rocky afterwards responses', 'regardless passed tri const pets lord binding', 6), (10, 28.995, 'police stanford structure', 'anatomy', 22), (21, 80.165, 'highlighted', 'cassette spots stated maker belarus electronic', 37), (8, 72.339, 'checkout wars cruise live', 'meal toilet create situations', 8), (36, 19.04, 'exposure murder', 'memo sporting', 36), (19, 90.136, 'path donations sierra shortly', 'vc replaced', 10), (7, 53.731, 'agreements fear', 'letter linux neural selected sc capability', 15), (0, 51.27, 'entertainment', 'equity alfred duties exam telescope continues', 33), (16, 15.674, 'speeches jessica impossible', 'department', 26), (1, 91.998, 'polyphonic', 'previous specify largest premier newly received', 21), (40, 57.64, 'compiled customise holders requires', 'teenage los rice buildings genome injection', 31), (4, 69.536, 'tue prizes peer carriers baby', 'resulting missions opinions cooperative elder', 23), (28, 19.56, 'oct rna enterprise contemporary trend amy', 'mainland mapping cambodia gerald dried soc', 34), (15, 91.186, 'uri happiness bringing beneficial', 'our puts gibson dicke dad previews resistant', 16), (25, 16.835, 'colleagues escape stripes treated', 'carried proudly magnificent', 40), (21, 74.199, 'flowers implied logos odds glenn owner', 'particles media', 17), (15, 70.807, 'transmission', 'erotic lucy housewares', 32), (19, 82.473, 'interstate class materials latitude thanks born', 'proposed earlier', 30), (21, 48.728, 'throws judges side opportunity', 'items', 1), (0, 26.063, 'hamilton george rover jan', 'julian', 6), (11, 29.122, 'sad glossary td headers genesis electro', 'nail greene helped cities publisher rehab', 27), (20, 91.892, 'holocaust', 'faculty', 21), (26, 34.104, 'pipeline cancer plastic', 'alter vitamins open', 36), (34, 83.845, 'hepatitis critics specially', 'instead arthritis lost sunshine graduated', 23), (15, 24.347, 'rendering experiments breeds logs pendant', 'bride consultation', 24), (20, 10.666, 'tract', 'paul ky securely newcastle continent mart yoga pro', 13), (28, 89.948, 'ace hb retention', 'synthesis holocaust towns secretariat vocal', 10), (13, 36.007, 'manage civic corporate cole west foto', 'pdt operations strikes validation', 22), (22, 20.852, 'maintained', 'cyprus gs elder teacher plain', 21), (34, 4.294, 'arlington bottles lab fireplace za has', 'dealtime participate huntington', 25), (25, 94.367, 'plaintiff', 'mrs stops', 9), (25, 9.745, 'jokes unless enemy dose tried', 'commit original permanent corps cb nickel', 19), (21, 34.548, 'investor passage', 'mesh cylinder sh opens anchor', 29), (4, 96.043, 'manage sponsor', 'zoo quest ccd fuzzy mentor attributes flying', 10), (8, 18.255, 'automation', 'most', 22), (33, 68.058, 'steering theater', 'rank mood practitioners nice strand sms rendering', 0), (36, 34.192, 'rounds', 'dicke waste wheel', 32), (16, 6.2, 'rush', 'featured niagara parenting scope faq', 30), (6, 33.711, 'ou favors coupled lloyd', 'strong trip singh incident', 23), (40, 36.81, 'stood dividend make comply physician', 'restrictions thread pregnant respected', 11), (37, 78.271, 'mac articles waiver new reference', 'grow gentleman', 30), (10, 58.736, 'supposed ministers community webpage computer', 'jim ports capabilities former myers', 25), (35, 8.77, 'show winners range ez jessica intelligent', 'reprint lesbian carriers lanka butts nothing', 13), (3, 36.678, 'eye little', 'xerox effectively antibodies lou month travelers', 36), (35, 18.887, 'applications considering', 'parcel softball motorcycles cnet air', 13), (7, 45.882, 'homes quit nails port mainland', 'bloom cow bosnia', 15), (25, 47.156, 'injured necessarily potter', 'say abs particles forward inserted', 0), (38, 38.634, 'fallen horny intense', 'connecting center circle jam', 27), (2, 23.961, 'tribe art tries contests democracy', 'walls desk thesaurus usual', 10), (3, 39.056, 'episodes loc alarm legends gear salvador', 'jam watching observations', 7), (39, 12.526, 'marilyn physics', 'establishing inquiry cause prepare issue editorial', 36), (1, 21.573, 'alignment technology amend sms structural', 'trusts', 34), (19, 77.866, 'instrumentation', 'hip continuity', 1), (8, 48.977, 'threshold', 'wild strap forty projection', 13), (36, 33.678, 'adobe mike abortion goal monitor', 'bookings sensitivity dirt s tom prepared', 31), (10, 34.805, 'proposed rays suffering stronger nancy worn', 'question employ', 21), (26, 84.582, 'describing slightly attack', 'episodes kyle lease marine prev quad mailed', 16), (31, 2.251, 'encryption solely slovakia detected', 'cunt physical', 21), (6, 36.372, 'john hollow wellness', 'alert', 25), (7, 77.017, 'omaha exciting', 'gods civilian lf evaluations', 37), (22, 56.02, 'estimated', 'areas surely', 18), (39, 26.234, 'omissions', 'funk organization not lows dark closing ceo nearby', 33), (31, 21.81, 'jill paraguay mirror yours libraries', 'ugly jp ep undefined', 4), (15, 84.442, 'luxury using fisher mine fi explanation', 'warcraft upper revised halfcom', 14), (18, 72.936, 'portrait controls', 'casual analog peru consultants', 18), (0, 1.884, 'dying passengers quiz hired attract', 'broken scanners calculator configuring', 18), (22, 1.035, 'holidays cable toddler', 'cloth chance', 16), (17, 94.87, 'had rid more men toys realize dealing julia', 'geographical ciao harrison andrews incorporate', 18), (6, 62.507, 'lil lucky', 'absorption after eventually', 19), (6, 41.819, 'multi mountains piece milan positioning', 'bbc', 10), (3, 90.074, 'merit', 'has meters paragraph workout', 22), (15, 26.001, 'awarded graduation appeals arm doe tuition cr', 'legends queue', 2), (24, 12.085, 'professionals', 'cartoon', 6), (22, 22.926, 'rid worn', 'we lewis licenses', 15), (2, 98.244, 'columbia lots', 'except these bin surround mating broadway', 2), (5, 4.579, 'stability', 'clocks', 5), (22, 31.568, 'alpine boss clara prisoners', 'gs have logs mayor clarity', 26), (7, 90.659, 'johns babe waiting bangladesh', 'bibliography broadway uganda elderly', 26), (21, 25.735, 'lp dm input updates pearl', 'tft biological comfort palace eliminate os walking', 6), (2, 87.61, 'congressional', 'clearly vanilla temporal goto purchasing', 15), (11, 71.696, 'boxes dominant neon ind arbitrary let writings', 'moscow messenger yet', 26), (16, 7.313, 'voice whose interfaces auburn creative cyprus', 'gardens sum approved rotation nm shops', 40), (5, 65.556, 'appointed ken end schools', 'exotic elliott', 7), (19, 39.481, 'ieee annotation', 'museums', 16);
INSERT INTO info2 VALUES ('boating', 'preferred pray citysearch survival', 12.301, 3), ('divide thriller', 'eclipse scenarios newspaper', 25.221, 40), ('farmer compounds future', 'gathering california courier talent', 79.483, 20), ('roughly botswana hiking souls hands', 'jail spears infectious worm syndrome', 53.186, 6), ('ali reprint rolls lean firm librarian', 'climate', 29.068, 9), ('label', 'german pepper adventures cry bless content', 28.562, 17), ('survivor bag market', 'wiring photographs games saver ours merchandise', 84.595, 34), ('penis wolf headset', 'count', 86.511, 13), ('incorrect peak', 'expectations gloves stadium partly', 43.308, 33), ('rainbow clinics calvin maker desk', 'capitol wealth parliament bangladesh', 51.108, 3), ('motorcycle nothing whatever alter status', 'inside onion investigations', 5.866, 6), ('mug verify safety erik curriculum', 'crafts mpeg rates necklace divine', 7.323, 40), ('given interests janet', 'shades perry niger achievements', 61.405, 7), ('wiring polo characterized', 'jewellery mixture conclude graduated', 14.801, 32), ('rebates arlington discharge ind', 'annoying', 33.731, 30), ('christians indicates dream rogers', 'infants', 89.657, 10), ('jewellery flower shade migration', 'worlds ad u sara portion information', 24.121, 13), ('dodge hate', 'wrist albuquerque', 19.314, 10), ('structural andy kirk synopsis', 'equations eagle cnet survivor sullivan', 50.493, 36), ('advised battery ridge anti distance', 'sims', 25.544, 20), ('epic las', 'males generator frank promotional', 80.474, 8), ('licking lawyers fda ted fame alabama', 'crowd appeal monday minority bulletin lie', 31.261, 6), ('tiles piss boundaries colonial marilyn', 'incidents humanitarian lower libraries', 35.165, 5), ('structures', 'exterior lessons challenge', 58.981, 30), ('fig apt hundreds lucas pre', 'tissue', 49.939, 30), ('possibly courage screw attach watson', 'cet scheduled porn essay', 90.485, 28), ('textile', 'mai opened ee distributors japanese', 82.911, 20), ('rehab profiles brochure findings', 'sentences modified astrology heath prospect', 56.919, 27), ('kerry jewellery', 'stocks uw remedy surprise arlington previews', 29.863, 6), ('bridge reseller', 'intel clone', 34.26, 0), ('proc ui hate mother template rendered', 'boobs households spoke', 53.007, 39), ('improvements editors remains clearance', 'late tribune', 67.181, 5), ('satisfy syracuse comments sharing', 'classics entering jd portal scenes jeff incident', 56.498, 1), ('specializing surprising', 'totals une remainder choir fl star club', 32.192, 6), ('trigger lottery geek elderly ceremony', 'fail tobacco bugs listprice star', 53.48, 6), ('sip parameters', 'far metallica', 99.151, 26), ('issue we addition collaboration', 'declare', 15.434, 22), ('pillow muslim extend operators', 'badly delicious', 9.459, 39), ('deliver lane battle', 'centre shade outstanding', 69.035, 0), ('russia asp cookbook desert unfortunately', 'street tournaments', 18.389, 35), ('harry knights west verzeichnis somewhat', 'cheats nursery medicaid routers', 29.11, 38), ('long kyle', 'checklist advertising aol injury wear polo', 66.404, 18), ('moderators og prisoners owners sc items', 'jewelry carolina', 28.665, 23), ('header rio mixer pharmaceutical', 'technician ribbon deserve released note violence', 83.739, 16), ('lounge gs copied spend desktops cure', 'tft account areas skin christ britannica enclosure', 0.536, 11), ('kay wings classics slovakia', 'wolf ma encouraging', 56.199, 35), ('nor documents hardwood', 'automatic', 51.56, 18), ('editor ski admissions', 'consistently', 0.907, 21), ('climate diverse', 'embassy network east racial cherry', 81.546, 13), ('reward sf', 'translator keyword reactions depends crisis', 5.395, 17), ('tools cookies switched', 'being invisible took q swim make', 12.919, 17), ('authentic desktop grain soundtrack', 'pe economies translation crew msn celebrities', 57.437, 9), ('athletic verizon perry', 'wearing', 49.856, 38), ('judges scanned definition ww francisco', 'ships hist singh surrounded plaza dsc platform', 19.797, 13), ('broad fuck guitars facility', 'denmark rid collins', 48.313, 24), ('nicaragua vampire', 'week chance cafe floyd viewers scientist', 72.277, 33), ('enabling wow statistical', 'starring shown slide dk literally', 13.026, 19), ('nudity', 'tgp cheese machinery modem teacher', 15.276, 16), ('mba pipes', 'permanent its military birmingham deutsche', 99.91, 35), ('insider walter convention bio', 'wrap uk stud efficiency numerical iraq cedar e', 59.484, 37), ('wool magic mardi planners approved', 'embassy were tcp logistics dick plates arab saver', 6.8, 2), ('lt cst grown burner gives ejaculation', 'swing pixel damage suse difficulties improvement', 59.129, 31), ('cope toy toys bargains niagara', 'wrist temp owned breasts limit nevada temporal', 29.956, 17), ('workshops hull fragrances', 'richardson finished casinos writings diversity', 57.124, 14), ('closer holds', 'rhythm informed society centers', 95.194, 35), ('antibody tub', 'smile ons', 7.1, 33), ('waiting pupils savings direction', 'cleanup travesti walker ind tribunal cigarettes', 39.742, 27), ('arthritis mounting cooked margin', 'administered thanksgiving holocaust material', 29.912, 4), ('camera', 'tumor wt review pollution smithsonian', 45.591, 12), ('suppliers known utilization blue', 'gym stack reduces angle expensive', 10.536, 12), ('almost liz iceland', 'ws thanksgiving event solid suggestions', 20.216, 2), ('superb war pets gabriel headed issn maui', 'benz shell municipal mega statistical', 66.442, 6), ('engaging div murray long', 'flash aaron nuclear ware citizen', 27.888, 9), ('staying database terrorism', 'integer longitude raise yellow holiday interior', 32.459, 8), ('extra', 'second motherboard indeed predict austria goal', 30.457, 34), ('ps invalid requiring theater', 'twice deaths', 89.225, 14), ('potentially collections tickets swing', 'prompt generations', 15.755, 20), ('portuguese', 'surge opera inherited boxes', 56.198, 7), ('carpet screw legends postings evident', 'juvenile apparently margaret mixing beef', 69.811, 18), ('categories viii eval vista descending', 'citizenship hotelscom mandate august', 88.735, 25), ('rewards dairy issn searchcom grammar', 'lovers tops trend', 33.939, 18), ('shake ns dependent relevant jelsoft', 'recorded grill', 82.73, 38), ('escape nature volt economy contracts', 'verified interests lite depot increasingly', 8.118, 21), ('conditioning tee xxx ignored', 'promo hilton', 75.295, 21), ('integrate accommodate signing baltimore', 'write born houses devon raw nike headline', 95.487, 27), ('nike shapes pit', 'highland completed humanities gerald gentle wings', 8.863, 28), ('trio burke grew citizens nr reading', 'venture tutorial employees vista premises visible', 95.28, 35), ('nursery brazil der zero widely', 'happen geek alumni julian editor', 82.95, 13), ('atomic we availability', 'extend', 16.154, 4), ('fitted genius copied increasingly', 'involves', 87.497, 19), ('marina', 'western transexuales ease nova boob catch', 98.825, 22), ('apps atom bodies continental', 'happen signal junior position', 80.657, 7), ('fitted', 'oriental packing', 79.293, 36), ('petroleum postcard explicitly', 'who mostly partnership aa colors cases syndication', 30.198, 13), ('assistant um happiness sets', 'bids methodology gazette beatles', 85.071, 27), ('sparc raleigh hayes first', 'systematic manually interests brazilian', 33.788, 17), ('sealed launched', 'arctic specify ann thy pc stream', 26.839, 14), ('banner', 'rebel dns subcommittee sculpture ca bs filed shot', 70.004, 5), ('processed revolution titten scholarships', 'enquiry convention avg households', 48.859, 36), ('associate metric twist cleared summaries', 'kit receiving category', 34.803, 20);
INSERT INTO info3 VALUES ('assets rep reel apparatus settle', 'cruises findings contributed responsible nuclear', 46.69, 2), ('newer vip printable compensation', 'ford referring genuine because brown dubai ill', 8.385, 18), ('affairs outputs', 'north examined purposes navy desktops', 61.648, 13), ('emotions spencer tin port similarly', 'signup mailman boolean england', 61.419, 32), ('mg tagged liked belkin', 'vocals aviation', 84.113, 11), ('sink granted', 'documents tuition va atomic changed ideal items', 13.561, 5), ('gem century', 'colorado nearly selecting', 52.031, 31), ('nasa baseline', 'subcommittee memories defendant', 78.351, 34), ('musical afford', 'liz tumor personnel coming', 66.737, 10), ('nipple bless sp bbc bow writes uni', 'extended traveller ti messaging', 97.05, 34), ('condo rica it joining', 'thumbs anaheim rebel weddings phase gabriel donald', 74.712, 13), ('tr respiratory', 'y exclusive', 28.163, 29), ('exposure', 'mainly mumbai kenya', 71.952, 29), ('versus', 'stretch', 51.924, 37), ('condos shipping similar punishment', 'windows beats authority', 3.678, 22), ('devel preservation', 'regions many widescreen review cst postage', 21.729, 9), ('az ddr adverse when eva toolbar scroll', 'bargains joshua dish blessed', 90.711, 20), ('investigated ups canal jail', 'defects guarantee responsible', 73.174, 0), ('possibly aggregate enhancing wrong', 'layers', 4.384, 6), ('mhz andrews lost currently', 'fits dated nl', 96.032, 20), ('refund board minimum method', 'issues circle institutional', 58.019, 14), ('monaco baseline prints', 'informational appears golden larry sierra', 76.172, 36), ('sell discussing', 'menus planets experiment host earlier dependence', 68.659, 24), ('thread fbi yes mime challenging porn', 'archived dosage simplified prominent', 85.815, 26), ('alaska stevens greatly', 'incorporate emacs', 16.998, 16), ('christian none circus', 'inspector holy quantum campus fault networks', 41.807, 23), ('elementary jet', 'love items', 78.427, 4), ('tucson conversation belgium unlike', 'granny harvest losses', 27.077, 9), ('referrals db melissa value', 'correlation ages falling fur animal traffic', 86.856, 31), ('win proper', 'lambda alice prints adventure', 51.646, 13), ('some andrea grande', 'exploring', 67.614, 12), ('asin greg other skilled', 'hamburg cnet', 28.576, 4), ('depend presence included experience gig', 'sheet highest logo context tournament', 92.137, 34), ('pushed watershed greetings mtv', 'certificates', 31.316, 40), ('iii altered dual greatest request', 'sheets goat pays customers pride because perform', 11.035, 40), ('camera annual fans founded', 'ben tomorrow boobs one', 79.273, 15), ('muscles', 'aware addition', 80.363, 28), ('fortune health legislation documents', 'phys ob functionality', 99.064, 3), ('bullet bennett addition games', 'module pills conflict retailers', 80.42, 15), ('eliminate', 'ellen commonly cologne mine trailers immediately', 80.034, 11), ('partial surface', 'astronomy biological estimation sleeps amounts', 42.118, 34), ('operation gain reports', 'alexandria song markets relatives supplied', 16.142, 34), ('seem telephony emirates slowly chemical', 'obtaining divided really', 70.083, 24), ('sweden', 'julia accessibility sega longer', 5.004, 35), ('sage chest consoles dish mysterious', 'jean navy hood modular maryland animation', 34.276, 28), ('consoles tmp', 'sodium higher generations completing', 45.686, 17), ('shape', 'tab sap seeds cup yn bool oclc', 13.818, 28), ('unknown', 'bucks', 9.488, 11), ('ann struct sudden tab dressed poly suite', 'bridges township deadline', 90.603, 15), ('survivors cope dh', 'salon', 34.503, 2), ('operating police designing supplement', 'bukkake guarantee artistic prophet broker', 57.971, 34), ('bukkake cake affect does', 'manor activation combinations grows execute old', 47.991, 5), ('apache alternatives', 'ceo outside wound invest', 76.51, 32), ('alliance anniversary letter', 'judicial', 98.942, 33), ('clips iron commitments', 'templates installed', 32.14, 5), ('participated progress', 'options producing', 28.907, 9), ('nurses highest ws members', 'produces endorsed line developer bruce welsh', 46.607, 32), ('situation', 'pregnancy singer pixels uzbekistan formatting', 6.496, 10), ('kijiji shaved', 'martha rel cheese johnny bouquet', 75.814, 15), ('continuously statistical', 'excitement', 1.488, 39);
INSERT INTO info4 VALUES (18.607, 'production', 'recommend meyer diesel instructions', 22), (62.972, 'dynamic columbia', 'sender digital dive utc', 23), (82.849, 'remainder autumn default statement leo tax', 'keywords tunnel defined decline', 14), (19.097, 'stereo doubt kentucky keen globe walked', 'issued', 18), (73.719, 'understanding everywhere nursing communications', 'careers selling', 7), (51.69, 'restriction continue compatibility', 'fraction region dh backgrounds its', 5), (10.893, 'locking nuke', 'receptors unlock', 14), (73.499, 'boots', 'life coupons preliminary', 13), (77.253, 'outcomes afternoon since', 'believes classified enhancing', 6), (63.513, 'metadata disabled please', 'wealth dramatic bike', 20), (69.139, 'therapist appearing', 'thee dirt freebsd selective', 1), (65.368, 'ti fence border harvey bc interpretation', 'challenged comfort', 16), (39.172, 'lace fist indicate excluded hughes ya anatomy', 'lemon construction', 19), (68.623, 'backup faces tape resistance channel', 'cuts organizing jackie twenty', 0), (15.906, 'liquid rear ka framing date', 'container foul overseas mins icons', 28), (77.623, 'productions ideal evans uniprotkb', 'nest emphasis miller bolt', 15), (90.467, 'parenting asbestos audience', 'cancellation', 27), (23.291, 'echo range commitment', 'verde', 12), (94.323, 'establishment titans', 'indexed program explorer', 26), (68.892, 'aspnet larry endorsed democratic pierce tower troy', 'successfully', 34), (80.1, 'walk advisors foto flood consumption yearly', 'destruction structural configuring', 39), (93.021, 'bike', 'delhi blood entered one genome joseph', 23), (31.432, 'duplicate covers klein', 'concord faqs', 3), (65.758, 'beats changes mutual commerce victor lie', 'lotus swingers', 38), (15.874, 'industrial porn reproduced du additional', 'violent ta addiction corner', 10), (76.869, 'assure', 'airlines knife being or commerce ian hit', 39), (42.932, 'cardiac matthew reservation grant assault cliff', 'reward bar nos lens constitutes', 33), (79.035, 'upskirt safely summary thousands webcast', 'niger ada', 11), (16.722, 'minimize chan communications louise collector', 'inline bay nike', 2), (0.262, 'jump applies road mailman number reno endorsed', 'sexy', 4), (99.306, 'qualifying chrysler else dangerous', 'companion', 26), (74.283, 'load cute', 'birthday', 17), (70.722, 'durham cottages fascinating', 'oecd i fighter kentucky cabin', 10), (51.47, 'peru weapons marble ind wiring rim', 'eng contemporary', 31), (13.229, 'saves czech pain ted directions', 'mental sharp forth tickets', 21), (60.023, 'trained biggest asbestos', 'contest', 19), (34.327, 'freely princeton transparent', 'kim menu students successfully', 28), (68.937, 'structural ibm len sold poland dimension', 'thirty specification theories', 37), (94.156, 'named floor gallery claire rob', 'fares', 26), (50.26, 'story justice publicity', 'referring habitat', 32);
INSERT INTO info5 VALUES (44.447, 'freely margaret theology inherited', 'tank', 13), (42.809, 'marble lu huge', 'slow cr facing dame commentary', 21), (57.359, 'commission chief hb viagra', 'happy preparation nor waters', 23), (23.211, 'catalogs solved dude', 'louisville enhancements whole', 14), (60.855, 'demonstrate proposition endangered', 'pottery', 21), (99.905, 'api mayor racing sql balloon alto gl johnson easy', 'licence', 34), (70.129, 'completing informative failures', 'singh interference sports groups cloth', 5), (53.499, 'observations broken propecia increased left uw', 'expansion plane incorrect', 35), (7.549, 'bolt entities corrections', 'pupils number', 35), (89.761, 'recorders cement tampa installations indicates', 'drives', 12), (41.768, 'tutorials disaster opinions virtue domains rates', 'framed mouth variance bibliography', 12), (64.386, 'adapter haven llc', 'purchasing', 16), (18.148, 'committee twice moments added', 'samba appendix', 35), (75.138, 'currency tear shops lotus years employer', 'craig reprint align users showing', 27), (94.17, 'sep chairs', 'wow username allowing occasions', 8), (77.345, 'performing gps packing prizes', 'bomb determines composed naturally op', 12), (57.404, 'industry settle parties', 'italia', 35), (0.675, 'patient volunteers', 'asthma flexible eddie kinda jurisdiction', 4), (90.726, 'travelling developed stroke', 'climbing arlington indiana undertake', 37), (59.575, 'central', 'exciting animal updating', 36), (41.775, 'dream apple botswana prep responsibilities nutten', 'pussy joins grid', 6), (61.001, 'worth cams', 'contribute disappointed', 0), (8.859, 'exhibits buffer', 'inner', 25), (23.049, 'bang exploration distribute', 'zealand', 20), (81.732, 'rational sphere', 'teen relevant creating bikes blonde', 33), (49.258, 'humidity preferred shock mongolia tr land', 'acoustic diet assistance', 20), (58.006, 'intake directly sega', 'stops generator skip garage buttons', 33), (81.33, 'personally earned villas mba bloggers', 'blast nearby projector', 15), (61.853, 'undo assist dogs travis snake michigan', 'dd purpose contemporary', 29), (22.011, 'reveals tutorials pharmaceuticals recognize', 'scope yet', 6), (91.642, 'lace decent flower', 'cylinder', 26), (58.937, 'counting observe il redhead', 'maintenance sleep', 8), (93.176, 'pledge premises democracy prerequisite vic', 'greek females hopkins stream t opinions', 31), (15.461, 'stephanie debt hint know ever bound workplace', 'estate roger exceptional', 7), (53.348, 'labeled fonts que appointment lists', 'astronomy', 15), (41.644, 'tissue dollars manitoba', 'wide spectrum borders', 0), (72.85, 'academy', 'erp', 0), (3.566, 'bitch taste ia upload wax orange', 'porn suspension healthy entity', 13), (24.463, 'desired contributors host bluetooth amazoncouk', 'uniform scanning colleagues', 11), (79.505, 'icq smile shaw ringtone lift informed oils repairs', 'spouse motel saskatchewan', 26);

INSERT INTO info VALUES (10, 1, 'hello', 'world', 20);

ALTER TABLE info ADD INDEX (C1);

SELECT * FROM info;

UPDATE info SET C1 = 999 WHERE info.C1 < 5;

SELECT * FROM info;

DELETE FROM info where info.C1 = 999;

SELECT * FROM info;

UPDATE info SET C3 = '----------------------------------------------' WHERE info.C1 > 8;

SELECT * FROM info;