<h1>Join Game Experiment</h1>
To upload the datasets that have been used for the join game experiment, you just need to run the  "data_uploader.bash" file with the 
updated database_name, username and port of your PostgreSQL database. You would also need to update the path of the file location 
that contains all the table data. The path from the data directory is present, any path that preceeds it, needs to be updated.

=====================================

Installation steps for PostgreSQL:

git clone https://github.com/OSU-IDEA-Lab/Join-Game.git
Create a newdirectory called "executables" outside of the Join-game directory. Replace everything in quotations with the path asked along with the quotations.
./configure --prefix="/path/to/executables/directory" --enable-depend --enable-assert --enable-debug
make
make install
export PATH="/path/to/executables/directory"/bin:$PATH
export PGDATA=DemoDir
initdb
In the below command replace portNumber with your port number of choice
"/path/to/executables/directory"/bin/pg_ctl -D "path/to/Join-Game"/DemoDir -o "-p portNumber" -l logfile start
psql -p portNumber template1
replace databaseName by the name of your database
create database databaseName;

\q
"/path/to/executables/directory"/bin/pg_ctl -D "path/to/Join-Game"/DemoDir -o "-p 1997" -l logfile stop
lsof -i :portNumber
psql -p portNumber databaseName

=====================================

To run similarity joins, follow the below steps:
1. make world
2. make install-world
3. restart the postgres server
4. connect to psql
5. CREATE EXTENSION fuzzystrmatch;
Now, you can run the similarity joins.