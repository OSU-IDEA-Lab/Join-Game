# git clone -b BaseCodeNoIndex https://github.com/OSU-IDEA-Lab/Join-Game.git
mkdir execu
cd Join-Game
./configure --prefix=/postgres/shrestaa-psql/execu --enable-depend --enable-assert --enable-debug
make
make install
export PATH=/postgres/shrestaa-psql/execu/bin:$PATH
export PGDATA=DemoDir
initdb

/postgres/shrestaa-psql/execu/bin/pg_ctl -D /postgres/shrestaa-psql/Join-Game/DemoDir -o '-p 4488' -l logfile start
/postgres/shrestaa-psql/execu/bin/pg_ctl -D /postgres/shrestaa-psql/Join-Game/DemoDir -o '-p 4488' -l logfile stop
make;make install
psql -p 4488 shrestaa

/postgres/shrestaa-psql/execu/bin/pg_ctl -D /postgres/shrestaa-psql/Join-Game/DemoDir -o '-p 4488' -l logfile stop
lsof -i :4488
psql -p 4488 template1
psql -p 4488 shrestaa


create database shrestaa;
create database tpch;
\q

\q

CREATE TABLE PROFESSOR ( P_PID INTEGER NOT NULL, P_NAME VARCHAR(25) NOT NULL);
CREATE TABLE STUDENT  ( S_ID INTEGER NOT NULL, S_PID INTEGER NOT NULL);

copy PROFESSOR FROM '/postgres/shrestaa-psql/data/professor.tbl' WITH DELIMITER AS '|';
copy STUDENT FROM '/postgres/shrestaa-psql/data/student.tbl' WITH DELIMITER AS '|';


# Make File Changes
/postgres/shrestaa-psql/execu/bin/pg_ctl -D /postgres/shrestaa-psql/Join-Game/DemoDir -o '-p 4488' -l logfile stop
make;make install
/postgres/shrestaa-psql/execu/bin/pg_ctl -D /postgres/shrestaa-psql/Join-Game/DemoDir -o '-p 4488' -l logfile start
psql -p 4488 shrestaa
psql -h /tmp/ -p 4488 shrestaa


select count(*) from professor p1, student s1 , student s2 where p1.p_pid = s1.s_pid and p1.p_pid = s2.s_pid;

 /postgres/shrestaa-psql/execu/bin/pg_ctl -D /postgres/shrestaa-psql/Join-Game/DemoDir -o '-p 4488' -l logfile stop ; make;make install; /postgres/shrestaa-psql/execu/bin/pg_ctl -D /postgres/shrestaa-psql/Join-Game/DemoDir -o '-p 4488' -l logfile start;psql -h /tmp/ -p 4488 shrestaa