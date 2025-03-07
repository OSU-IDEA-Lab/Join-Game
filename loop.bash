# # #!/bin/bash
# # make
# # make install
# # /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

# # # python similarity_lv5_cord19.py full_similarity_lv5_cord19 sum_similarity_lv5_cord19
# # # python similarity_lv4_cord19.py full_similarity_lv4_cord19 sum_similarity_lv4_cord19
# # # python similarity_lv3_cord19.py full_similarity_lv3_cord19 sum_similarity_lv3_cord19
# # # python similarity_lv2_cord19.py full_similarity_lv2_cord19 sum_similarity_lv2_cord19
# # # python similarity_lv1_cord19.py full_similarity_lv1_cord19 sum_similarity_lv1_cord19

# # #----------------------------------------------------------------ORL------------------------------------------------------------------------------
# # # ----------------------------------------0.01%--------------------------------------------------
# # # for t in 321
# # # do
# # # export MEMORY_MAX=$t
# # # python similarity_ABR.py full_similarity sum_similarity
# # # done

# # # for t in 1645
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_WDC.py full_similarity sum_similarity
# # # done

# # # for t in 5439
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_GRBR.py full_similarity sum_similarity
# # # done

# # # for t in 253
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_Cars.py full_similarity sum_similarity
# # # done

# # # for t in 188
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_Spotify.py full_similarity sum_similarity
# # # done

# # # python similarity_averager.py

# # # -----------------------------------------1%----------------------------------------------------

# # # for t in 32124
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_ABR.py full_similarity sum_similarity
# # # done

# # # for t in 57904 for teritiary 100727
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_WDC.py full_similarity sum_similarity
# # # done

# # # for t in 543900
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_GRBR.py full_similarity sum_similarity
# # # done

# # # for t in 25310 for teritiary join 27171
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_Cars.py full_similarity sum_similarity cl
# # # done

# # # for t in 18809
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_Spotify.py full_similarity sum_similarity
# # # done

# # # 34000 for python similarity_movies.py full_similarity sum_similarity

# # # python similarity_averager.py

# # # ----------------------------------------10%----------------------------------------------------

# # # for t in 321240
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_ABR.py full_similarity sum_similarity
# # # done

# # # for t in 1645140
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_WDC.py full_similarity sum_similarity
# # # done

# # # for t in 5439000
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_GRBR.py full_similarity sum_similarity
# # # done

# # # for t in 253104
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_Cars.py full_similarity sum_similarity
# # # done

# # # for t in 188093
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_Spotify.py full_similarity sum_similarity
# # # done

# # # python similarity_averager.py

# # #----------------------------------------------------------------Ripple------------------------------------------------------------------------------

# # # ----------------------------------------0.01%--------------------------------------------------
# # # for t in 321
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_ABR.py full_similarity sum_similarity
# # # done

# # # for t in 1645
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_WDC.py full_similarity sum_similarity
# # # done

# # # for t in 5439
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_GRBR.py full_similarity sum_similarity
# # # done

# # # for t in 253
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_Cars.py full_similarity sum_similarity
# # # done

# # # for t in 188
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_Spotify.py full_similarity sum_similarity
# # # done

# # # python similarity_averager.py

# # # -----------------------------------------1%----------------------------------------------------

# # # for t in 32124
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_ABR.py full_similarity sum_similarity
# # # done

# # # for t in 164514
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_WDC.py full_similarity sum_similarity
# # # done

# # # for t in 543900
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_GRBR.py full_similarity sum_similarity
# # # done

# # # for t in 25310
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_Cars.py full_similarity sum_similarity
# # # done

# # # for t in 18809
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_Spotify.py full_similarity sum_similarity
# # # done

# # # python similarity_averager.py

# # # ----------------------------------------10%----------------------------------------------------

# # # for t in 321240
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_ABR.py full_similarity sum_similarity
# # # done

# # # for t in 1645140
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_WDC.py full_similarity sum_similarity
# # # done

# # # for t in 5439000
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_GRBR.py full_similarity sum_similarity
# # # done

# # # for t in 253104
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_Cars.py full_similarity sum_similarity
# # # done

# # # for t in 188093
# # # do
# # # echo MEMORY_MAX = $t
# # # python similarity_Spotify.py full_similarity sum_similarity
# # # done

# # # python similarity_averager.py

# # #----------------------------------------------------------------OSL------------------------------------------------------------------------------
# # # -------------------------------------------0.01%--------------------------------------------
# # # declare -a values=(
# # #     "46 275"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# # #     echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# # #     echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE =$n
# #     # python similarity_ABR.py full_similarity sum_similarity
# # # done

# # # declare -a values=(
# # #     "235 1410"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# #     # echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# #     # echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE = $n
# #     # python similarity_WDC.py full_similarity sum_similarity
# # # done

# # # declare -a values=(
# # #     "777 4662"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# #     # echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# #     # echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE = $n
# #     # python similarity_GRBR.py full_similarity sum_similarity
# # # done

# # # declare -a values=(
# # #     "36 217"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# #     # echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# #     # echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE = $n
# #     # python similarity_Cars.py full_similarity sum_similarity
# # # done

# # # declare -a values=(
# # #     "250 1503"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# #     # echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# #     # echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE = $n
# #     # python similarity_Spotify.py full_similarity sum_similarity
# # # done

# # # python similarity_averager.py

# # # -------------------------------------------1%--------------------------------------------
# # # declare -a values=(
# # #     "4589 27534"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# # #     echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# # #     echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE =$n
# #     # python similarity_ABR.py full_similarity sum_similarity
# # # done

# # # declare -a values=(
# # #     "23502 141012"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# #     # echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# #     # echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE = $n
# #     # python similarity_WDC.py full_similarity sum_similarity
# # # done

# # # declare -a values=(
# # #     "77700 466200"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# #     # echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# #     # echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE = $n
# #     # python similarity_GRBR.py full_similarity sum_similarity
# # # done

# # # declare -a values=(
# # #     "3614 21687"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# #     # echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# #     # echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE = $n
# #     # python similarity_Cars.py full_similarity sum_similarity
# # # done

# # # declare -a values=(
# # #     "25044 150266"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# #     # echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# #     # echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE = $n
# #     # python similarity_Spotify.py full_similarity sum_similarity
# # # done

# # # python similarity_averager.py

# # # -------------------------------------------10%--------------------------------------------
# # # declare -a values=(
# # #     "45890 275340"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# # #     echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# # #     echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE =$n
# #     # python similarity_ABR.py full_similarity sum_similarity
# # # done

# # # declare -a values=(
# # #     "235020 1410120"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# #     # echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# #     # echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE = $n
# #     # python similarity_WDC.py full_similarity sum_similarity
# # # done

# # # declare -a values=(
# # #     "777000 4662000"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# #     # echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# #     # echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE = $n
# #     # python similarity_GRBR.py full_similarity sum_similarity
# # # done

# # # declare -a values=(
# # #     "36146 216877"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# #     # echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# #     # echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE = $n
# #     # python similarity_Cars.py full_similarity sum_similarity
# # # done

# # # declare -a values=(
# #         # 26870 161223"
# # # )
# # # for pair in "${values[@]}"
# # # do
# # #     read m n <<< "$pair"
# #     # echo PGNST8_LEFT_PAGE_MAX_SIZE = $m
# #     # echo OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE = $n
# #     # python similarity_Spotify.py full_similarity sum_similarity
# # # done

# # # python similarity_averager.py

# # #-----------------------------------------------------------------Shuffling commands-----------------------------------------------------------------
# # #Shuffler codes
# # # python dataset_shuffler_ABR.py clean
# # # python dataset_shuffler_GRBR.py clean
# # # python dataset_shuffler_WDC.py clean
# # # python dataset_shuffler_Spotify.py clean
# # # python dataset_shuffler_Cars.py clean

# # /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop

c_file="/data/mettas/Join-Game/src/backend/executor/nodeNestloop.c"
exec_file="/data/mettas/Join-Game/src/include/nodes/execnodes.h"

osl_c="/data/mettas/Join-Game/Scripts/nodeNestloop(ImprovedOSL) - Copy.c"
osl_exec="/data/mettas/Join-Game/Scripts/execnodes(ImprovedOSL) - Copy.h"
cl_c="/data/mettas/Join-Game/Scripts/nodeNestloop(CL) - Copy.c"
cl_exec="/data/mettas/Join-Game/Scripts/execnodes(CL) - Copy.h"
icl_c="/data/mettas/Join-Game/Scripts/nodeNestloop(ImprovedICL) - Copy.c"
icl_exec="/data/mettas/Join-Game/Scripts/execnodes(ImprovedICL) - Copy.h"
orl_c="/data/mettas/Join-Game/Scripts/nodeNestloop(OnlineRipple) - Copy.c"
orl_exec="/data/mettas/Join-Game/Scripts/execnodes(OnlineRipple) - Copy.h"

cars_script_osl="python similarity_Cars.py full_similarity sum_similarity osl"
cars_script_cl="python similarity_Cars.py full_similarity sum_similarity cl"
cars_script_icl="python similarity_Cars.py full_similarity sum_similarity icl"
cars_script_orl="python similarity_Cars.py full_similarity sum_similarity orl"

similarity_script_osl="python similarity_averager.py osl"
similarity_script_cl="python similarity_averager.py cl"
similarity_script_icl="python similarity_averager.py icl"
similarity_script_orl="python similarity_averager.py orl"


failure_counts=(50 100 150 200)

for N in "${failure_counts[@]}"; do
    echo "Processing FAILURE_COUNT_N=$N"

    M=944
    cp "$osl_c" "$c_file"
    cp "$osl_exec" "$exec_file"
    sed -i "s/^#define OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE .*/#define OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE $((2*N))/" "$c_file"
    sed -i "s/^#define FAILURE_COUNT_N .*/#define FAILURE_COUNT_N $N/" "$c_file"
    sed -i "s/^#define MUST_EXPLORE_TUPLE_COUNT_N .*/#define MUST_EXPLORE_TUPLE_COUNT_N $M/" "$c_file"

    /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
    make
    make install
    /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

    if [ $? -eq 0 ]; then
        echo "Compilation successful, running Python script with M=$Mf or OSL"
        $cars_script_osl $N
    else
        echo "Compilation failed for M=$M"
    fi

    cp "$cl_c" "$c_file"
    cp "$cl_exec" "$exec_file"
    sed -i "s/^#define OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE .*/#define OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE $((2*N))/" "$c_file"
    sed -i "s/^#define OSL_BND8_LEFT_TABLE_CACHE_MAX_SIZE .*/#define OSL_BND8_LEFT_TABLE_CACHE_MAX_SIZE $((2*N))/" "$c_file"
    sed -i "s/^#define FAILURE_COUNT_N .*/#define FAILURE_COUNT_N $N/" "$c_file"
    sed -i "s/^#define MUST_EXPLORE_TUPLE_COUNT_N .*/#define MUST_EXPLORE_TUPLE_COUNT_N $M/" "$c_file"

    /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
    make
    make install
    /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

    if [ $? -eq 0 ]; then
        echo "Compilation successful, running Python script with M=$M" for CL
        $cars_script_cl $N
    else
        echo "Compilation failed for M=$M"
    fi

    cp "$icl_c" "$c_file"
    cp "$icl_exec" "$exec_file"
    sed -i "s/^#define OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE .*/#define OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE $((2*N))/" "$c_file"
    sed -i "s/^#define OSL_BND8_LEFT_TABLE_CACHE_MAX_SIZE .*/#define OSL_BND8_LEFT_TABLE_CACHE_MAX_SIZE $((2*N))/" "$c_file"
    sed -i "s/^#define FAILURE_COUNT_N .*/#define FAILURE_COUNT_N $N/" "$c_file"
    sed -i "s/^#define MUST_EXPLORE_TUPLE_COUNT_N .*/#define MUST_EXPLORE_TUPLE_COUNT_N $M/" "$c_file"

    /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile stop
    make
    make install
    /data/mettas/executables/bin/pg_ctl -D /data/mettas/Join-Game/DemoDir -o "-p 1997" -l logfile start

    if [ $? -eq 0 ]; then
        echo "Compilation successful, running Python script with M=$M for ICL"
        $cars_script_icl $N
    else
        echo "Compilation failed for M=$M"
    fi

    $similarity_script_osl $N
    $similarity_script_cl $N
    $similarity_script_icl $N

done
