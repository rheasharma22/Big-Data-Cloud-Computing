G = LOAD '$G' USING PigStorage(',') AS (key:long, val:long);
grouped_g = GROUP G BY key;
grp1 = FOREACH grouped_g GENERATE group, COUNT(G) as part; 
grp2 = group grp1 BY part;
final = FOREACH grp2 GENERATE group, COUNT(grp1);
STORE final INTO '$O';