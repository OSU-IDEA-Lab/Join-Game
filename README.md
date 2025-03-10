<h1>How to run experiments?</h1>
To upload the datasets that have been used for the join game experiment, you just need to run the  "data_uploader.bash" file with the 
updated database_name, username and port of your PostgreSQL database. You would also need to update the path of the file location 
that contains all the table data. The path from the data directory is present, any path that preceeds it, needs to be updated.

=====================================

<h2>Installation steps for PostgreSQL:</h2>

git clone https://github.com/OSU-IDEA-Lab/Join-Game.git <br>
Create a newdirectory called "executables" outside of the Join-game directory. Replace everything in quotations with the path asked along with the quotations.<br>
./configure --prefix="/path/to/executables/directory" --enable-depend --enable-assert --enable-debug<br>
make<br>
make install<br>
export PATH="/path/to/executables/directory"/bin:$PATH<br>
export PGDATA=DemoDir<br>
initdb<br>
In the below command replace portNumber with your port number of choice<br>
"/path/to/executables/directory"/bin/pg_ctl -D "path/to/Join-Game"/DemoDir -o "-p portNumber" -l logfile start<br>
psql -p portNumber template1<br>
replace databaseName by the name of your database<br>
create database databaseName;<br>

\q<br>
"/path/to/executables/directory"/bin/pg_ctl -D "path/to/Join-Game"/DemoDir -o "-p 1997" -l logfile stop<br>
lsof -i :portNumber<br>
psql -p portNumber databaseName<br>

=====================================

To run similarity joins, follow the below steps:<br>
1. make world<br>
2. make install-world<br>
3. restart the postgres server<br>
4. connect to psql<br>
5. CREATE EXTENSION fuzzystrmatch;<br>
Now, you can run the similarity joins.<br>

=====================================

<h2>Data uploading to your PostgreSQL database.:</h2>

<h3>Compiling Code</h3>
Linux User<br>
Run make -f makefile_linux.original. You will see dbgen and dists.dss files.<br>

These two will be used for TPC-H data generation.<br>

<h3>Preparing TPC-H dataset</h3>
The command ./dbgen -h shows list of options.<br>

1. Inside dbgen folder, run the below command.<br>
./dbgen -s 10.0 -z 0 The above command will create z = 0 data with 10GB size data.<br>

2. Do the below command to see the first few rows of a table.<br>
head customer.tbl If you observe carefully, each tuple will have a | symbol at the end.<br>
For query loading and processing, the | at the end is not required. So, we need to remove this in the next step.<br>

3. Remove the "|" at the end of the tuple in all the tables.<br>
sed -i 's/|$//' *.tbl<br>

After running the above command, we can see that | is removed for all the tuples in all the tables.<br>

You can also run the script remove.sh for multiple tables.<br>

4. Shuffle each table as below. If you do not shuffle, the tuples will be in sorted order.<br>
Run the script shuffle_tables.sh for multiple tables.<br>

<h3>Preparing Similarity Join dataset</h3>

1. To upload the Cars, WDC and Movies datasets, just run the data_uploader.bash file.<br>

2. You can download the above datasets at the below links.<br>

    1. Cars Dataset: parking_tickets table: http://www.kaggle.com/datasets/new-york-city/nyc-parking-tickets and Car_brands table: http://www.back4app.com/database/back4app/car-make-model-dataset<br>
        Query used: SELECT Car_brands1.make, parking_tickets1.vehicle_make FROM Car_brands1 JOIN parking_tickets1 ON levenshtein(trim(Car_brands1.make::varchar(10)), trim(parking_tickets1.vehicle_make::varchar(10))) <= (1, 2 and 3);<br>
    2. WDC Dataset: webdatacommons.org/largescaleproductcorpus/v2 You can divide the data into two separate tables to join them as WDC1 and WDC2<br>
        Query Used: SELECT wdc1Brands.brand, wdc2Brands.brand FROM wdc1Brands JOIN wdc2Brands ON levenshtein(trim(wdc1Brands.brand::varchar(10)), trim(wdc2Brands.brand::varchar(10))) <= (1, 2 and 3);<br>
    3. Movies Dataset: IMDB table: https://developer.imdb.com/non-commercial-datasets/ and OMDB table: https://www.omdbapi.com/<br>
        Query Used: EXPLAIN ANALYSE SELECT imdb.title, omdbMovies.title FROM imdb JOIN omdbMovies ON levenshtein(trim(imdb.title::varchar(50)), trim(omdbMovies.title::varchar(50))) <= (9, 10 and 11);<br>

3. Open the data_uploader.bash file and then replace "database_name" "username" "port" with your database_name, username and port. Also, you would need to open the python files mentioned in the bash file and then replace the file path of the data files for all these tables with their correct file paths.<br>