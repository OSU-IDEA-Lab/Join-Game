# # python Plotter.py /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-11-25_110003.log /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-11-25_114059.log /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-11-25_122035.log /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-11-25_140556.log /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-11-25_144617.log /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-11-25_152610.log /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-11-27_124550.log /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-11-27_133052.log /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-11-27_141419.log


# # python Plotter_comparer.py /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-12-03_192341.log /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-12-03_193039.log /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-12-03_193720.log /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-12-03_220030.log /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-12-03_220633.log /data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-12-03_221243.log

#--------------------------------------------------------------LV=1--------------------------------------------------------------------------------

#------------- Simple M=1/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #simple
# sed -i '165s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '163s~.*~double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 1
#                                     LIMIT 797181;
# EOF

# #-------------Simple M=2/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*2~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #simple
# sed -i '165s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '163s~.*~double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 1
#                                     LIMIT 797181;
# EOF

# #-------------Simple M=full------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #simple
# sed -i '165s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '163s~.*~double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 1
#                                     LIMIT 797181;
# EOF

# #------------- 4.2.1 M=1/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.1
# sed -i '421s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '422s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '423s~.*~double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 1
#                                     LIMIT 797181;
# EOF

# #-------------4.2.1 M=2/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*2~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.1
# sed -i '421s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '422s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '423s~.*~double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 1
#                                     LIMIT 797181;
# EOF

# #-------------4.2.1 M=full------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.1
# sed -i '421s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '422s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '423s~.*~double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 1
#                                     LIMIT 797181;
# EOF

# #------------- 4.2.2 M=1/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.2
# sed -i '311s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '310s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '309s~.*~double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~//calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 1
#                                     LIMIT 797181;
# EOF

# #-------------4.2.2 M=2/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*2~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.2
# sed -i '311s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '310s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '309s~.*~double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~//calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 1
#                                     LIMIT 797181;
# EOF

# #-------------4.2.2 M=full------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.2
# sed -i '311s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '310s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '309s~.*~double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~//calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 1
#                                     LIMIT 797181;
# EOF

# #--------------------------------------------------------------LV=2--------------------------------------------------------------------------------

# #------------- Simple M=1/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #simple
# sed -i '165s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '163s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 2  
#                                     LIMIT 1962523;
# EOF

# #-------------Simple M=2/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*2~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #simple
# sed -i '165s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '163s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 2  
#                                     LIMIT 1962523;
# EOF

# #-------------Simple M=full------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #simple
# sed -i '165s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '163s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 2  
#                                     LIMIT 1962523;
# EOF

# #------------- 4.2.1 M=1/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.1
# sed -i '421s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '422s~.*~double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '423s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 2  
#                                     LIMIT 1962523;
# EOF

# #-------------4.2.1 M=2/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*2~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.1
# sed -i '421s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '422s~.*~double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '423s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 2  
#                                     LIMIT 1962523;
# EOF

# #-------------4.2.1 M=full------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.1
# sed -i '421s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '422s~.*~double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '423s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 2  
#                                     LIMIT 1962523;
# EOF

# #------------- 4.2.2 M=1/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.2
# sed -i '311s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '310s~.*~double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '309s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~//calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 2  
#                                     LIMIT 1962523;
# EOF

# #-------------4.2.2 M=2/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*2~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.2
# sed -i '311s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '310s~.*~double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '309s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~//calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 2  
#                                     LIMIT 1962523;
# EOF

# #-------------4.2.2 M=full------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.2
# sed -i '311s~.*~//double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '310s~.*~double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '309s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~//calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 2  
#                                     LIMIT 1962523;
# EOF

# #---------------------------------------------------------------------LV = 3-----------------------------------------------------------------------

# #------------- Simple M=1/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #simple
# sed -i '165s~.*~double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '163s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 3  
#                                     LIMIT 7313656;
# EOF

# #-------------Simple M=2/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*2~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #simple
# sed -i '165s~.*~double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '163s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 3  
#                                     LIMIT 7313656;
# EOF

# #-------------Simple M=full------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #simple
# sed -i '165s~.*~double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '163s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 3  
#                                     LIMIT 7313656;
# EOF

# #------------- 4.2.1 M=1/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.1
# sed -i '421s~.*~double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '422s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '423s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 3  
#                                     LIMIT 7313656;
# EOF

# #-------------4.2.1 M=2/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*2~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.1
# sed -i '421s~.*~double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '422s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '423s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 3  
#                                     LIMIT 7313656;
# EOF

# #-------------4.2.1 M=full------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.1
# sed -i '421s~.*~double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '422s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '423s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~//calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 3  
#                                     LIMIT 7313656;
# EOF

# #------------- 4.2.2 M=1/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.2
# sed -i '311s~.*~double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '310s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '309s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~//calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 3  
#                                     LIMIT 7313656;
# EOF

# #-------------4.2.2 M=2/3------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*2~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.2
# sed -i '311s~.*~double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '310s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '309s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~//calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# psql -p 1506 -h localhost -d mettas <<EOF
# SELECT Car_brands3.make, parking_tickets3.vehicle_make 
#                                     FROM Car_brands3 
#                                     JOIN parking_tickets3 
#                                     ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 3  
#                                     LIMIT 7313656;
# EOF

# #-------------4.2.2 M=full------------------
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '69s~.*~#define PGNST8_LEFT_PAGE_MAX_SIZE (1588/3)*3~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# #4.2.2
# sed -i '311s~.*~double actual_count = 731365682.0; //cars - lv=3 - 731365682~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '310s~.*~//double actual_count = 196252372.0; //cars - lv=2 - 196252372~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '309s~.*~//double actual_count = 79718102.0; //cars - lv=1 - 79718102~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# # method selector
# sed -i '750s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '749s~.*~//calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '748s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c

# sed -i '1297s~.*~calculateConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1296s~.*~// calculateMiddleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '1295s~.*~//calculateSimpleConfidenceInterval(node);~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Movies LV = 8
#!/bin/bash

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Movies LV = 8
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '50s~.*~define MEMORY_MAX        4751070~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '226s~.*~		double actual_count = 12458184816269176.0;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)441028105215 * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.title1, sub.title2, actors1.knownForTitles
# FROM (
#     SELECT imdb1.title AS title1, omdbMovies1.title AS title2
#     FROM imdb1
#     JOIN omdbMovies1 ON levenshtein(trim(imdb1.title::varchar(10)), trim(omdbMovies1.title::varchar(10))) <= 8
# ) sub
# JOIN actors1
#     ON levenshtein(trim(sub.title1::varchar(10)), trim(actors1.knownForTitles::varchar(10))) <= 8
#     AND levenshtein(trim(sub.title2::varchar(10)), trim(actors1.knownForTitles::varchar(10))) <= 8
# LIMIT 1245818;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# # sed -i '226s~.*~		double actual_count = 33496909439427.0;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.title1, sub.title2, actors2.knownForTitles
# FROM (
#     SELECT imdb2.title AS title1, omdbMovies2.title AS title2
#     FROM imdb2
#     JOIN omdbMovies2 ON levenshtein(trim(imdb2.title::varchar(10)), trim(omdbMovies2.title::varchar(10))) <= 8
# ) sub
# JOIN actors2
#     ON levenshtein(trim(sub.title1::varchar(10)), trim(actors2.knownForTitles::varchar(10))) <= 8
#     AND levenshtein(trim(sub.title2::varchar(10)), trim(actors2.knownForTitles::varchar(10))) <= 8
# LIMIT 1245818;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# # sed -i '226s~.*~		double actual_count = 46050233317763.0;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.title1, sub.title2, actors3.knownForTitles
# FROM (
#     SELECT imdb3.title AS title1, omdbMovies3.title AS title2
#     FROM imdb3
#     JOIN omdbMovies3 ON levenshtein(trim(imdb3.title::varchar(10)), trim(omdbMovies3.title::varchar(10))) <= 8
# ) sub
# JOIN actors3
#     ON levenshtein(trim(sub.title1::varchar(10)), trim(actors3.knownForTitles::varchar(10))) <= 8
#     AND levenshtein(trim(sub.title2::varchar(10)), trim(actors3.knownForTitles::varchar(10))) <= 8
# LIMIT 1245818;
# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # Movies LV = 7
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '226s#.*#		double actual_count = 430416738703914.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)94726938069 * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.title1, sub.title2, actors1.knownForTitles
# FROM (
#     SELECT imdb1.title AS title1, omdbMovies1.title AS title2
#     FROM imdb1
#     JOIN omdbMovies1 ON levenshtein(trim(imdb1.title::varchar(10)), trim(omdbMovies1.title::varchar(10))) <= 7
# ) sub
# JOIN actors1
#     ON levenshtein(trim(sub.title1::varchar(10)), trim(actors1.knownForTitles::varchar(10))) <= 7
#     AND levenshtein(trim(sub.title2::varchar(10)), trim(actors1.knownForTitles::varchar(10))) <= 7
# LIMIT 4304167;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# # sed -i '226s#.*#		double actual_count = 5714005199559.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.title1, sub.title2, actors2.knownForTitles
# FROM (
#     SELECT imdb2.title AS title1, omdbMovies2.title AS title2
#     FROM imdb2
#     JOIN omdbMovies2 ON levenshtein(trim(imdb2.title::varchar(10)), trim(omdbMovies2.title::varchar(10))) <= 7
# ) sub
# JOIN actors2
#     ON levenshtein(trim(sub.title1::varchar(10)), trim(actors2.knownForTitles::varchar(10))) <= 7
#     AND levenshtein(trim(sub.title2::varchar(10)), trim(actors2.knownForTitles::varchar(10))) <= 7
# LIMIT 4304167;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# # sed -i '226s#.*#		double actual_count = 7819691403231.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.title1, sub.title2, actors3.knownForTitles
# FROM (
#     SELECT imdb3.title AS title1, omdbMovies3.title AS title2
#     FROM imdb3
#     JOIN omdbMovies3 ON levenshtein(trim(imdb3.title::varchar(10)), trim(omdbMovies3.title::varchar(10))) <= 7
# ) sub
# JOIN actors3
#     ON levenshtein(trim(sub.title1::varchar(10)), trim(actors3.knownForTitles::varchar(10))) <= 7
#     AND levenshtein(trim(sub.title2::varchar(10)), trim(actors3.knownForTitles::varchar(10))) <= 7
# LIMIT 4304167;
# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # Movies LV = 6
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '226s#.*#		double actual_count = 16052360355641.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)12346832945 * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.title1, sub.title2, actors1.knownForTitles
# FROM (
#     SELECT imdb1.title AS title1, omdbMovies1.title AS title2
#     FROM imdb1
#     JOIN omdbMovies1 ON levenshtein(trim(imdb1.title::varchar(10)), trim(omdbMovies1.title::varchar(10))) <= 6
# ) sub
# JOIN actors1
#     ON levenshtein(trim(sub.title1::varchar(10)), trim(actors1.knownForTitles::varchar(10))) <= 6
#     AND levenshtein(trim(sub.title2::varchar(10)), trim(actors1.knownForTitles::varchar(10))) <= 6
# LIMIT 1605236;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# # sed -i '226s#.*#		double actual_count = 1147910074287.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.title1, sub.title2, actors2.knownForTitles
# FROM (
#     SELECT imdb2.title AS title1, omdbMovies2.title AS title2
#     FROM imdb2
#     JOIN omdbMovies2 ON levenshtein(trim(imdb2.title::varchar(10)), trim(omdbMovies2.title::varchar(10))) <= 6
# ) sub
# JOIN actors2
#     ON levenshtein(trim(sub.title1::varchar(10)), trim(actors2.knownForTitles::varchar(10))) <= 6
#     AND levenshtein(trim(sub.title2::varchar(10)), trim(actors2.knownForTitles::varchar(10))) <= 6
# LIMIT 1605236;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# # sed -i '226s#.*#		double actual_count = 1457191249772.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.title1, sub.title2, actors3.knownForTitles
# FROM (
#     SELECT imdb3.title AS title1, omdbMovies3.title AS title2
#     FROM imdb3
#     JOIN omdbMovies3 ON levenshtein(trim(imdb3.title::varchar(10)), trim(omdbMovies3.title::varchar(10))) <= 6
# ) sub
# JOIN actors3
#     ON levenshtein(trim(sub.title1::varchar(10)), trim(actors3.knownForTitles::varchar(10))) <= 6
#     AND levenshtein(trim(sub.title2::varchar(10)), trim(actors3.knownForTitles::varchar(10))) <= 6
# LIMIT 1605236;
# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # WDC LV = 3
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '50s~.*~define MEMORY_MAX        4751070~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '226s#.*#		double actual_count = 38094610856469768.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)643362131662 * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.brand1, sub.brand2, wdc3Brands1.brand
# FROM (
#     SELECT wdc1Brands1.brand AS brand1, wdc2Brands1.brand AS brand2
#     FROM wdc1Brands1
#     JOIN wdc2Brands1 ON levenshtein(trim(wdc1Brands1.brand::varchar(10)), trim(wdc2Brands1.brand::varchar(10))) <= 3
# ) sub
# JOIN wdc3Brands1
#     ON levenshtein(trim(sub.brand1::varchar(10)), trim(wdc3Brands1.brand::varchar(10))) <= 3
#     AND levenshtein(trim(sub.brand2::varchar(10)), trim(wdc3Brands1.brand::varchar(10))) <= 3
# LIMIT 3809461;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.brand1, sub.brand2, wdc3Brands2.brand
# FROM (
#     SELECT wdc1Brands2.brand AS brand1, wdc2Brands2.brand AS brand2
#     FROM wdc1Brands2
#     JOIN wdc2Brands2 ON levenshtein(trim(wdc1Brands2.brand::varchar(10)), trim(wdc2Brands2.brand::varchar(10))) <= 3
# ) sub
# JOIN wdc3Brands2
#     ON levenshtein(trim(sub.brand1::varchar(10)), trim(wdc3Brands2.brand::varchar(10))) <= 3
#     AND levenshtein(trim(sub.brand2::varchar(10)), trim(wdc3Brands2.brand::varchar(10))) <= 3
# LIMIT 3809461;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.brand1, sub.brand2, wdc3Brands3.brand
# FROM (
#     SELECT wdc1Brands3.brand AS brand1, wdc2Brands3.brand AS brand2
#     FROM wdc1Brands3
#     JOIN wdc2Brands3 ON levenshtein(trim(wdc1Brands3.brand::varchar(10)), trim(wdc2Brands3.brand::varchar(10))) <= 3
# ) sub
# JOIN wdc3Brands3
#     ON levenshtein(trim(sub.brand1::varchar(10)), trim(wdc3Brands3.brand::varchar(10))) <= 3
#     AND levenshtein(trim(sub.brand2::varchar(10)), trim(wdc3Brands3.brand::varchar(10))) <= 3
# LIMIT 3809461;
# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # WDC LV = 2
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '226s#.*#		double actual_count = 9133633043646500.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)200104045466 * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.brand1, sub.brand2, wdc3Brands1.brand
# FROM (
#     SELECT wdc1Brands1.brand AS brand1, wdc2Brands1.brand AS brand2
#     FROM wdc1Brands1
#     JOIN wdc2Brands1 ON levenshtein(trim(wdc1Brands1.brand::varchar(10)), trim(wdc2Brands1.brand::varchar(10))) <= 2
# ) sub
# JOIN wdc3Brands1
#     ON levenshtein(trim(sub.brand1::varchar(10)), trim(wdc3Brands1.brand::varchar(10))) <= 2
#     AND levenshtein(trim(sub.brand2::varchar(10)), trim(wdc3Brands1.brand::varchar(10))) <= 2
# LIMIT 9133633;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.brand1, sub.brand2, wdc3Brands2.brand
# FROM (
#     SELECT wdc1Brands2.brand AS brand1, wdc2Brands2.brand AS brand2
#     FROM wdc1Brands2
#     JOIN wdc2Brands2 ON levenshtein(trim(wdc1Brands2.brand::varchar(10)), trim(wdc2Brands2.brand::varchar(10))) <= 2
# ) sub
# JOIN wdc3Brands2
#     ON levenshtein(trim(sub.brand1::varchar(10)), trim(wdc3Brands2.brand::varchar(10))) <= 2
#     AND levenshtein(trim(sub.brand2::varchar(10)), trim(wdc3Brands2.brand::varchar(10))) <= 2
# LIMIT 9133633;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.brand1, sub.brand2, wdc3Brands3.brand
# FROM (
#     SELECT wdc1Brands3.brand AS brand1, wdc2Brands3.brand AS brand2
#     FROM wdc1Brands3
#     JOIN wdc2Brands3 ON levenshtein(trim(wdc1Brands3.brand::varchar(10)), trim(wdc2Brands3.brand::varchar(10))) <= 2
# ) sub
# JOIN wdc3Brands3
#     ON levenshtein(trim(sub.brand1::varchar(10)), trim(wdc3Brands3.brand::varchar(10))) <= 2
#     AND levenshtein(trim(sub.brand2::varchar(10)), trim(wdc3Brands3.brand::varchar(10))) <= 2
# LIMIT 9133633;
# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # WDC LV = 1
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '226s#.*#		double actual_count = 8042489787442205.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)132825725525 * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.brand1, sub.brand2, wdc3Brands1.brand
# FROM (
#     SELECT wdc1Brands1.brand AS brand1, wdc2Brands1.brand AS brand2
#     FROM wdc1Brands1
#     JOIN wdc2Brands1 ON levenshtein(trim(wdc1Brands1.brand::varchar(10)), trim(wdc2Brands1.brand::varchar(10))) <= 1
# ) sub
# JOIN wdc3Brands1
#     ON levenshtein(trim(sub.brand1::varchar(10)), trim(wdc3Brands1.brand::varchar(10))) <= 1
#     AND levenshtein(trim(sub.brand2::varchar(10)), trim(wdc3Brands1.brand::varchar(10))) <= 1
# LIMIT 8042489;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.brand1, sub.brand2, wdc3Brands2.brand
# FROM (
#     SELECT wdc1Brands2.brand AS brand1, wdc2Brands2.brand AS brand2
#     FROM wdc1Brands2
#     JOIN wdc2Brands2 ON levenshtein(trim(wdc1Brands2.brand::varchar(10)), trim(wdc2Brands2.brand::varchar(10))) <= 1
# ) sub
# JOIN wdc3Brands2
#     ON levenshtein(trim(sub.brand1::varchar(10)), trim(wdc3Brands2.brand::varchar(10))) <= 1
#     AND levenshtein(trim(sub.brand2::varchar(10)), trim(wdc3Brands2.brand::varchar(10))) <= 1
# LIMIT 8042489;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.brand1, sub.brand2, wdc3Brands3.brand
# FROM (
#     SELECT wdc1Brands3.brand AS brand1, wdc2Brands3.brand AS brand2
#     FROM wdc1Brands3
#     JOIN wdc2Brands3 ON levenshtein(trim(wdc1Brands3.brand::varchar(10)), trim(wdc2Brands3.brand::varchar(10))) <= 1
# ) sub
# JOIN wdc3Brands3
#     ON levenshtein(trim(sub.brand1::varchar(10)), trim(wdc3Brands3.brand::varchar(10))) <= 1
#     AND levenshtein(trim(sub.brand2::varchar(10)), trim(wdc3Brands3.brand::varchar(10))) <= 1
# LIMIT 8042489;
# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # Cars LV = 3
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '50s~.*~define MEMORY_MAX        4751070~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '226s#.*#		double actual_count = 10389021710255.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)731365682 * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.make, sub.vehicle_make, car_accidents1.Vehicle_Make
# FROM (
#     SELECT Car_brands1.make, parking_tickets1.vehicle_make
#     FROM Car_brands1
#     JOIN parking_tickets1 ON levenshtein(trim(Car_brands1.make::varchar(10)), trim(parking_tickets1.vehicle_make::varchar(10))) <= 3
# ) sub
# JOIN car_accidents1
#     ON levenshtein(trim(sub.make::varchar(10)), trim(car_accidents1.Vehicle_Make::varchar(10))) <= 3
#     AND levenshtein(trim(sub.vehicle_make::varchar(10)), trim(car_accidents1.Vehicle_Make::varchar(10))) <= 3
# LIMIT 1038902;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.make, sub.vehicle_make, car_accidents2.Vehicle_Make
# FROM (
#     SELECT Car_brands2.make, parking_tickets2.vehicle_make
#     FROM Car_brands2
#     JOIN parking_tickets2 ON levenshtein(trim(Car_brands2.make::varchar(10)), trim(parking_tickets2.vehicle_make::varchar(10))) <= 3
# ) sub
# JOIN car_accidents2
#     ON levenshtein(trim(sub.make::varchar(10)), trim(car_accidents2.Vehicle_Make::varchar(10))) <= 3
#     AND levenshtein(trim(sub.vehicle_make::varchar(10)), trim(car_accidents2.Vehicle_Make::varchar(10))) <= 3
# LIMIT 1038902;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.make, sub.vehicle_make, car_accidents3.Vehicle_Make
# FROM (
#     SELECT Car_brands3.make, parking_tickets3.vehicle_make
#     FROM Car_brands3
#     JOIN parking_tickets3 ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 3
# ) sub
# JOIN car_accidents3
#     ON levenshtein(trim(sub.make::varchar(10)), trim(car_accidents3.Vehicle_Make::varchar(10))) <= 3
#     AND levenshtein(trim(sub.vehicle_make::varchar(10)), trim(car_accidents3.Vehicle_Make::varchar(10))) <= 3
# LIMIT 1038902;
# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # Cars LV = 2
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '226s#.*#		double actual_count = 977530737135.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)196252372 * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.make, sub.vehicle_make, car_accidents1.Vehicle_Make
# FROM (
#     SELECT Car_brands1.make, parking_tickets1.vehicle_make
#     FROM Car_brands1
#     JOIN parking_tickets1 ON levenshtein(trim(Car_brands1.make::varchar(10)), trim(parking_tickets1.vehicle_make::varchar(10))) <= 2
# ) sub
# JOIN car_accidents1
#     ON levenshtein(trim(sub.make::varchar(10)), trim(car_accidents1.Vehicle_Make::varchar(10))) <= 2
#     AND levenshtein(trim(sub.vehicle_make::varchar(10)), trim(car_accidents1.Vehicle_Make::varchar(10))) <= 2
# LIMIT 9775307;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.make, sub.vehicle_make, car_accidents2.Vehicle_Make
# FROM (
#     SELECT Car_brands2.make, parking_tickets2.vehicle_make
#     FROM Car_brands2
#     JOIN parking_tickets2 ON levenshtein(trim(Car_brands2.make::varchar(10)), trim(parking_tickets2.vehicle_make::varchar(10))) <= 2
# ) sub
# JOIN car_accidents2
#     ON levenshtein(trim(sub.make::varchar(10)), trim(car_accidents2.Vehicle_Make::varchar(10))) <= 2
#     AND levenshtein(trim(sub.vehicle_make::varchar(10)), trim(car_accidents2.Vehicle_Make::varchar(10))) <= 2
# LIMIT 9775307;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.make, sub.vehicle_make, car_accidents3.Vehicle_Make
# FROM (
#     SELECT Car_brands3.make, parking_tickets3.vehicle_make
#     FROM Car_brands3
#     JOIN parking_tickets3 ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 2
# ) sub
# JOIN car_accidents3
#     ON levenshtein(trim(sub.make::varchar(10)), trim(car_accidents3.Vehicle_Make::varchar(10))) <= 2
#     AND levenshtein(trim(sub.vehicle_make::varchar(10)), trim(car_accidents3.Vehicle_Make::varchar(10))) <= 2
# LIMIT 9775307;
# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Cars LV = 1
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '226s#.*#		double actual_count = 222608581099.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)79718102 * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.make, sub.vehicle_make, car_accidents1.Vehicle_Make
# FROM (
#     SELECT Car_brands1.make, parking_tickets1.vehicle_make
#     FROM Car_brands1
#     JOIN parking_tickets1 ON levenshtein(trim(Car_brands1.make::varchar(10)), trim(parking_tickets1.vehicle_make::varchar(10))) <= 1
# ) sub
# JOIN car_accidents1
#     ON levenshtein(trim(sub.make::varchar(10)), trim(car_accidents1.Vehicle_Make::varchar(10))) <= 1
#     AND levenshtein(trim(sub.vehicle_make::varchar(10)), trim(car_accidents1.Vehicle_Make::varchar(10))) <= 1
# LIMIT 2226085;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.make, sub.vehicle_make, car_accidents2.Vehicle_Make
# FROM (
#     SELECT Car_brands2.make, parking_tickets2.vehicle_make
#     FROM Car_brands2
#     JOIN parking_tickets2 ON levenshtein(trim(Car_brands2.make::varchar(10)), trim(parking_tickets2.vehicle_make::varchar(10))) <= 1
# ) sub
# JOIN car_accidents2
#     ON levenshtein(trim(sub.make::varchar(10)), trim(car_accidents2.Vehicle_Make::varchar(10))) <= 1
#     AND levenshtein(trim(sub.vehicle_make::varchar(10)), trim(car_accidents2.Vehicle_Make::varchar(10))) <= 1
# LIMIT 2226085;
# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE
# SELECT sub.make, sub.vehicle_make, car_accidents3.Vehicle_Make
# FROM (
#     SELECT Car_brands3.make, parking_tickets3.vehicle_make
#     FROM Car_brands3
#     JOIN parking_tickets3 ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 1
# ) sub
# JOIN car_accidents3
#     ON levenshtein(trim(sub.make::varchar(10)), trim(car_accidents3.Vehicle_Make::varchar(10))) <= 1
#     AND levenshtein(trim(sub.vehicle_make::varchar(10)), trim(car_accidents3.Vehicle_Make::varchar(10))) <= 1
# LIMIT 2226085;
# EOF

# Movies LV = 8
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
sed -i '50s~.*~#define MEMORY_MAX        36464~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
sed -i "18s#.*#    lv = ['8']#" /data/saketh_temp/PSQL1506/Join-Game/similarity_movies.py
make
make install
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
python similarity_movies.py full_similarity sum_similarity ripple

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Movies LV = 7
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
sed -i "18s#.*#    lv = ['7']#" /data/saketh_temp/PSQL1506/Join-Game/similarity_movies.py
make
make install
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
python similarity_movies.py full_similarity sum_similarity ripple

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Movies LV = 6
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
sed -i "18s#.*#    lv = ['6']#" /data/saketh_temp/PSQL1506/Join-Game/similarity_movies.py
make
make install
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
python similarity_movies.py full_similarity sum_similarity ripple

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# WDC LV = 3
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
sed -i '50s~.*~#define MEMORY_MAX        181002~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
sed -i "20s#.*#    lv = ['3']#" /data/saketh_temp/PSQL1506/Join-Game/similarity_WDC.py
make
make install
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
python similarity_WDC.py full_similarity sum_similarity ripple

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# WDC LV = 2
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
sed -i "20s~.*~    lv = ['2']~" /data/saketh_temp/PSQL1506/Join-Game/similarity_WDC.py
make
make install
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
python similarity_WDC.py full_similarity sum_similarity ripple

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# WDC LV = 1
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
sed -i "20s~.*~    lv = ['1']~" /data/saketh_temp/PSQL1506/Join-Game/similarity_WDC.py
make
make install
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
python similarity_WDC.py full_similarity sum_similarity ripple

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Cars LV = 3
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
sed -i '50s~.*~#define MEMORY_MAX        27172~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
sed -i "21s~.*~    lv = ['3']~" /data/saketh_temp/PSQL1506/Join-Game/similarity_Cars.py
make
make install
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
python similarity_Cars.py full_similarity sum_similarity ripple

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Cars LV = 2
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
sed -i "21s~.*~    lv = ['2']~" /data/saketh_temp/PSQL1506/Join-Game/similarity_Cars.py
make
make install
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
python similarity_Cars.py full_similarity sum_similarity ripple

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Cars LV = 1
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
sed -i "21s~.*~    lv = ['1']~" /data/saketh_temp/PSQL1506/Join-Game/similarity_Cars.py
make
make install
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
python similarity_Cars.py full_similarity sum_similarity ripple


# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '226s#.*#		double actual_count = 8000000.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)8000000 * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ROSL
# psql -p 1506 -h localhost -d mettas <<EOF
# set enable_material = OFF;
# set enable_seqscan = ON;
# set enable_indexonlyscan = OFF;
# set enable_indexscan = OFF;
# set enable_bitmapscan = OFF;
# set enable_block = OFF;
# set enable_fastjoin = OFF;
# set enable_fliporder = OFF;
# set enable_nestloop = ON;
# set enable_hashjoin = OFF;
# set enable_mergejoin = OFF;
# set statement_timeout = 1800000;
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE select * from part110, supplier110, partsupp110 where p_partkey = ps_partkey and s_suppkey = ps_suppkey LIMIT 80000;
# EOF

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Movies LV = 8
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
sed -i '50s~.*~#define MEMORY_MAX        33784~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
sed -i '214s~.*~		double actual_count = 441028105215.0;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
sed -i '164s~.*~    long long total = (long long)node->numOuterTuples * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
make
make install
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# python similarity_Cars.py full_similarity sum_similarity ripple
psql -p 1506 -h localhost -d mettas <<EOF
SET max_parallel_workers_per_gather = 0;
SET join_collapse_limit = 1;
SET from_collapse_limit = 1;
EXPLAIN ANALYSE SELECT imdb1.title, omdbMovies1.title FROM imdb1 JOIN omdbMovies1 ON levenshtein(trim(imdb1.title::varchar(10)), trim(omdbMovies1.title::varchar(10))) <= 8 LIMIT 4410281;

EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT imdb2.title, omdbMovies2.title FROM imdb2 JOIN omdbMovies2 ON levenshtein(trim(imdb2.title::varchar(10)), trim(omdbMovies2.title::varchar(10))) <= 8 LIMIT 4410281;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT imdb3.title, omdbMovies3.title FROM imdb3 JOIN omdbMovies3 ON levenshtein(trim(imdb3.title::varchar(10)), trim(omdbMovies3.title::varchar(10))) <= 8 LIMIT 4410281;

# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # Movies LV = 7
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '226s#.*#		double actual_count = 94726938069.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)node->numOuterTuples * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT imdb1.title, omdbMovies1.title FROM imdb1 JOIN omdbMovies1 ON levenshtein(trim(imdb1.title::varchar(10)), trim(omdbMovies1.title::varchar(10))) <= 7 LIMIT 9472693;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# # sed -i '226s#.*#		double actual_count = 5714005199559.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT imdb2.title, omdbMovies2.title FROM imdb2 JOIN omdbMovies2 ON levenshtein(trim(imdb2.title::varchar(10)), trim(omdbMovies2.title::varchar(10))) <= 7 LIMIT 9472693;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# # sed -i '226s#.*#		double actual_count = 7819691403231.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT imdb3.title, omdbMovies3.title FROM imdb3 JOIN omdbMovies3 ON levenshtein(trim(imdb3.title::varchar(10)), trim(omdbMovies3.title::varchar(10))) <= 7 LIMIT 9472693;

# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # Movies LV = 6
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '226s#.*#		double actual_count = 12346832945.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)node->numOuterTuples * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT imdb1.title, omdbMovies1.title FROM imdb1 JOIN omdbMovies1 ON levenshtein(trim(imdb1.title::varchar(10)), trim(omdbMovies1.title::varchar(10))) <= 6 LIMIT 1234683;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# # sed -i '226s#.*#		double actual_count = 1147910074287.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT imdb2.title, omdbMovies2.title FROM imdb2 JOIN omdbMovies2 ON levenshtein(trim(imdb2.title::varchar(10)), trim(omdbMovies2.title::varchar(10))) <= 6 LIMIT 1234683;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# # sed -i '226s#.*#		double actual_count = 1457191249772.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT imdb3.title, omdbMovies3.title FROM imdb3 JOIN omdbMovies3 ON levenshtein(trim(imdb3.title::varchar(10)), trim(omdbMovies3.title::varchar(10))) <= 6 LIMIT 1234683;

# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # WDC LV = 3
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '50s~.*~#define MEMORY_MAX        160117~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '226s#.*#		double actual_count = 643362131662.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)node->numOuterTuples * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT wdc1Brands.brand, wdc2Brands.brand FROM wdc1Brands JOIN wdc2Brands ON levenshtein(trim(wdc1Brands.brand::varchar(10)), trim(wdc2Brands.brand::varchar(10))) <= 3 limit 6433621;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT wdc1Brands2.brand, wdc2Brands2.brand FROM wdc1Brands2 JOIN wdc2Brands2 ON levenshtein(trim(wdc1Brands2.brand::varchar(10)), trim(wdc2Brands2.brand::varchar(10))) <= 3 limit 6433621;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT wdc1Brands3.brand, wdc2Brands3.brand FROM wdc1Brands3 JOIN wdc2Brands3 ON levenshtein(trim(wdc1Brands3.brand::varchar(10)), trim(wdc2Brands3.brand::varchar(10))) <= 3 limit 6433621;

# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # WDC LV = 2
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '226s#.*#		double actual_count = 200104045466.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)node->numOuterTuples * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT wdc1Brands.brand, wdc2Brands.brand FROM wdc1Brands JOIN wdc2Brands ON levenshtein(trim(wdc1Brands.brand::varchar(10)), trim(wdc2Brands.brand::varchar(10))) <= 2 limit 2001040;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT wdc1Brands2.brand, wdc2Brands2.brand FROM wdc1Brands2 JOIN wdc2Brands2 ON levenshtein(trim(wdc1Brands2.brand::varchar(10)), trim(wdc2Brands2.brand::varchar(10))) <= 2 limit 2001040;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT wdc1Brands3.brand, wdc2Brands3.brand FROM wdc1Brands3 JOIN wdc2Brands3 ON levenshtein(trim(wdc1Brands3.brand::varchar(10)), trim(wdc2Brands3.brand::varchar(10))) <= 2 limit 2001040;

# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # WDC LV = 1
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '226s#.*#		double actual_count = 132825725525.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)node->numOuterTuples * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT wdc1Brands.brand, wdc2Brands.brand FROM wdc1Brands JOIN wdc2Brands ON levenshtein(trim(wdc1Brands.brand::varchar(10)), trim(wdc2Brands.brand::varchar(10))) <= 1 limit 1328257;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT wdc1Brands2.brand, wdc2Brands2.brand FROM wdc1Brands2 JOIN wdc2Brands2 ON levenshtein(trim(wdc1Brands2.brand::varchar(10)), trim(wdc2Brands2.brand::varchar(10))) <= 1 limit 1328257;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT wdc1Brands3.brand, wdc2Brands3.brand FROM wdc1Brands3 JOIN wdc2Brands3 ON levenshtein(trim(wdc1Brands3.brand::varchar(10)), trim(wdc2Brands3.brand::varchar(10))) <= 1 limit 1328257;

# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Cars LV = 3
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '50s~.*~#define MEMORY_MAX        25302~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '226s#.*#		double actual_count = 731365682.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)node->numOuterTuples * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT Car_brands1.make, parking_tickets1.vehicle_make FROM Car_brands1 JOIN parking_tickets1 ON levenshtein(trim(Car_brands1.make::varchar(10)), trim(parking_tickets1.vehicle_make::varchar(10))) <= 3 LIMIT 7313656;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT Car_brands2.make, parking_tickets2.vehicle_make FROM Car_brands2 JOIN parking_tickets2 ON levenshtein(trim(Car_brands2.make::varchar(10)), trim(parking_tickets2.vehicle_make::varchar(10))) <= 3 LIMIT 7313656;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT Car_brands3.make, parking_tickets3.vehicle_make FROM Car_brands3 JOIN parking_tickets3 ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 3 LIMIT 7313656;

# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # Cars LV = 2
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '226s#.*#		double actual_count = 196252372.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)node->numOuterTuples * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT Car_brands1.make, parking_tickets1.vehicle_make FROM Car_brands1 JOIN parking_tickets1 ON levenshtein(trim(Car_brands1.make::varchar(10)), trim(parking_tickets1.vehicle_make::varchar(10))) <= 2 LIMIT 1962523;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT Car_brands2.make, parking_tickets2.vehicle_make FROM Car_brands2 JOIN parking_tickets2 ON levenshtein(trim(Car_brands2.make::varchar(10)), trim(parking_tickets2.vehicle_make::varchar(10))) <= 2 LIMIT 1962523;

# EOF

/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
make
make install
/data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT Car_brands3.make, parking_tickets3.vehicle_make FROM Car_brands3 JOIN parking_tickets3 ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 2 LIMIT 1962523;

# EOF

# # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # Cars LV = 1
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# sed -i '226s#.*#		double actual_count = 79718102.0;#' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# sed -i '164s~.*~    long long total = (long long)node->numOuterTuples * node->numInnerTuples;~' /data/saketh_temp/PSQL1506/Join-Game/src/backend/executor/nodeNestloop.c
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT Car_brands1.make, parking_tickets1.vehicle_make FROM Car_brands1 JOIN parking_tickets1 ON levenshtein(trim(Car_brands1.make::varchar(10)), trim(parking_tickets1.vehicle_make::varchar(10))) <= 1 LIMIT 797181;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT Car_brands2.make, parking_tickets2.vehicle_make FROM Car_brands2 JOIN parking_tickets2 ON levenshtein(trim(Car_brands2.make::varchar(10)), trim(parking_tickets2.vehicle_make::varchar(10))) <= 1 LIMIT 797181;

# EOF

# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile stop -m immediate
# make
# make install
# /data/saketh_temp/PSQL1506/executables/bin/pg_ctl -D /data/saketh_temp/PSQL1506/Join-Game/DemoDir -o "-p 1506" -l logfile start
# # python similarity_Cars.py full_similarity sum_similarity ripple
# psql -p 1506 -h localhost -d mettas <<EOF
# SET max_parallel_workers_per_gather = 0;
# SET join_collapse_limit = 1;
# SET from_collapse_limit = 1;
# EXPLAIN ANALYSE SELECT Car_brands3.make, parking_tickets3.vehicle_make FROM Car_brands3 JOIN parking_tickets3 ON levenshtein(trim(Car_brands3.make::varchar(10)), trim(parking_tickets3.vehicle_make::varchar(10))) <= 1 LIMIT 797181;

# EOF
