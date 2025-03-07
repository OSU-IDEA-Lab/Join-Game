# #!/bin/bash

# # Define paths and scripts
# c_file="/data/mettas/Join-Game/src/backend/executor/nodeNestloop.c"
# exec_file="/data/mettas/Join-Game/src/include/nodes/execnodes.h"

# #algorithm paths
# ripple_c="/data/mettas/Join-Game/Scripts/nodeNestloop(RippleJoin) - Copy.c"
# ripple_exec="/data/mettas/Join-Game/Scripts/execnodes(RippleJoin) - Copy.h"
# orl_c="/data/mettas/Join-Game/Scripts/nodeNestloop(OnlineRipple) - Copy.c"
# orl_exec="/data/mettas/Join-Game/Scripts/execnodes(OnlineRipple) - Copy.h"
# osl_c="/data/mettas/Join-Game/Scripts/nodeNestloop(ImprovedOSL) - Copy.c"
# osl_exec="/data/mettas/Join-Game/Scripts/execnodes(ImprovedOSL) - Copy.h"
# cl_c="/data/mettas/Join-Game/Scripts/nodeNestloop(CL) - Copy.c"
# cl_exec="/data/mettas/Join-Game/Scripts/execnodes(CL) - Copy.h"
# icl_c="/data/mettas/Join-Game/Scripts/nodeNestloop(ImprovedICL) - Copy.c"
# icl_exec="/data/mettas/Join-Game/Scripts/execnodes(ImprovedICL) - Copy.h"
# nestloop_c="/data/mettas/Join-Game/Scripts/nodeNestloop_original - Copy.c"
# nestloop_exec="/data/mettas/Join-Game/Scripts/execnodes_original - Copy.h"

# # Loop through each algorithm
# cars_script_osl="python similarity_Cars.py full_similarity sum_similarity osl"
# wdc_script_osl="python similarity_WDC.py full_similarity sum_similarity osl"
# cars_script_cl="python similarity_Cars.py full_similarity sum_similarity cl"
# wdc_script_cl="python similarity_WDC.py full_similarity sum_similarity cl"
# cars_script_icl="python similarity_Cars.py full_similarity sum_similarity icl"
# wdc_script_icl="python similarity_WDC.py full_similarity sum_similarity icl"
# cars_script_ripple="python similarity_Cars.py full_similarity sum_similarity ripple"
# wdc_script_ripple="python similarity_WDC.py full_similarity sum_similarity ripple"
# cars_script_orl="python similarity_Cars.py full_similarity sum_similarity orl"
# wdc_script_orl="python similarity_WDC.py full_similarity sum_similarity orl"

# movies_script_osl="python similarity_movies.py full_similarity sum_similarity osl"
# movies_script_cl="python similarity_movies.py full_similarity sum_similarity cl"
# movies_script_icl="python similarity_movies.py full_similarity sum_similarity icl"
# movies_script_ripple="python similarity_movies.py full_similarity sum_similarity ripple"
# movies_script_orl="python similarity_movies.py full_similarity sum_similarity orl"


# similarity_script_osl="python similarity_averager.py osl"
# similarity_script_cl="python similarity_averager.py cl"
# similarity_script_ripple="python similarity_averager.py ripple"
# similarity_script_icl="python similarity_averager.py icl"
# similarity_script_orl="python similarity_averager.py orl"


# #OSL, CL and ICL for Cars

# M=472
# cp "$osl_c" "$c_file"
# cp "$osl_exec" "$exec_file"
# sed -i "s/^#define MUST_EXPLORE_TUPLE_COUNT_N .*/#define MUST_EXPLORE_TUPLE_COUNT_N $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $cars_script_osl
# else
#     echo "Compilation failed for M=$M"
# fi

# cp "$cl_c" "$c_file"
# cp "$cl_exec" "$exec_file"
# sed -i "s/^#define MUST_EXPLORE_TUPLE_COUNT_N .*/#define MUST_EXPLORE_TUPLE_COUNT_N $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $cars_script_cl
# else
#     echo "Compilation failed for M=$M"
# fi

# cp "$icl_c" "$c_file"
# cp "$icl_exec" "$exec_file"
# sed -i "s/^#define MUST_EXPLORE_TUPLE_COUNT_N .*/#define MUST_EXPLORE_TUPLE_COUNT_N $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $cars_script_icl
# else
#     echo "Compilation failed for M=$M"
# fi

# #OSL, CL and ICL for WDC

# M=2069
# cp "$osl_c" "$c_file"
# cp "$osl_exec" "$exec_file"
# sed -i "s/^#define MUST_EXPLORE_TUPLE_COUNT_N .*/#define MUST_EXPLORE_TUPLE_COUNT_N $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $wdc_script_osl
# else
#     echo "Compilation failed for M=$M"
# fi

# cp "$cl_c" "$c_file"
# cp "$cl_exec" "$exec_file"
# sed -i "s/^#define MUST_EXPLORE_TUPLE_COUNT_N .*/#define MUST_EXPLORE_TUPLE_COUNT_N $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $wdc_script_cl
# else
#     echo "Compilation failed for M=$M"
# fi

# cp "$icl_c" "$c_file"
# cp "$icl_exec" "$exec_file"
# sed -i "s/^#define MUST_EXPLORE_TUPLE_COUNT_N .*/#define MUST_EXPLORE_TUPLE_COUNT_N $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $wdc_script_icl
# else
#     echo "Compilation failed for M=$M"
# fi

# #Ripple and ORL for Cars

# M=27171
# cp "$ripple_c" "$c_file"
# cp "$ripple_exec" "$exec_file"
# sed -i "s/^#define MEMORY_MAX .*/#define MEMORY_MAX $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $cars_script_ripple
# else
#     echo "Compilation failed for M=$M"
# fi

# cp "$orl_c" "$c_file"
# cp "$orl_exec" "$exec_file"
# sed -i "s/^#define MEMORY_MAX .*/#define MEMORY_MAX $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $cars_script_orl
# else
#     echo "Compilation failed for M=$M"
# fi

# # #Ripple and ORL for WDC

# M=100728
# cp "$ripple_c" "$c_file"
# cp "$ripple_exec" "$exec_file"
# sed -i "s/^#define MEMORY_MAX .*/#define MEMORY_MAX $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $wdc_script_ripple
# else
#     echo "Compilation failed for M=$M"
# fi

# cp "$orl_c" "$c_file"
# cp "$orl_exec" "$exec_file"
# sed -i "s/^#define MEMORY_MAX .*/#define MEMORY_MAX $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $wdc_script_orl
# else
#     echo "Compilation failed for M=$M"
# fi

# # #OSL, ICL, CL for Movies dataset

# M=1508
# cp "$osl_c" "$c_file"
# cp "$osl_exec" "$exec_file"
# sed -i "s/^#define MUST_EXPLORE_TUPLE_COUNT_N .*/#define MUST_EXPLORE_TUPLE_COUNT_N $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $movies_script_osl
# else
#     echo "Compilation failed for M=$M"
# fi

# cp "$cl_c" "$c_file"
# cp "$cl_exec" "$exec_file"
# sed -i "s/^#define MUST_EXPLORE_TUPLE_COUNT_N .*/#define MUST_EXPLORE_TUPLE_COUNT_N $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $movies_script_cl
# else
#     echo "Compilation failed for M=$M"
# fi

# cp "$icl_c" "$c_file"
# cp "$icl_exec" "$exec_file"
# sed -i "s/^#define MUST_EXPLORE_TUPLE_COUNT_N .*/#define MUST_EXPLORE_TUPLE_COUNT_N $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $movies_script_icl
# else
#     echo "Compilation failed for M=$M"
# fi

# #Ripple and ORL for Movies dataset

# M=34063
# cp "$ripple_c" "$c_file"
# cp "$ripple_exec" "$exec_file"
# sed -i "s/^#define MEMORY_MAX .*/#define MEMORY_MAX $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $movies_script_ripple
# else
#     echo "Compilation failed for M=$M"
# fi

# cp "$orl_c" "$c_file"
# cp "$orl_exec" "$exec_file"
# sed -i "s/^#define MEMORY_MAX .*/#define MEMORY_MAX $M/" "$c_file"

# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
# make; make install
# /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# if [ $? -eq 0 ]; then
#     echo "Compilation successful, running Python script with M=$M"
#     $movies_script_orl
# else
#     echo "Compilation failed for M=$M"
# fi

# $similarity_script_osl
# $similarity_script_ripple
# $similarity_script_cl
# $similarity_script_icl
# $similarity_script_orl

python3 queryRunner.py