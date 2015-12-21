//start hadoop
//load data into hdfs
//bin/hdfs dfs -put ~/Downloads/DSNY_Monthly_Tonnage_Data.csv /user/hashaalb

//start pig or execute using pig joining_pig_script.pig

//load rates table
rates = LOAD '/Users/hashaalb/Desktop/BDA/project/thedatasets/Diversion_and_Capture_Rates.csv' USING PigStorage(',') AS (Borough, District, D_Rate, Paper_C_Rate, MGP_C_Rate, Total_C_Rate);

DUMP rates;

//load population table
population = LOAD '/Users/hashaalb/Desktop/BDA/project/thedatasets/New_York_City_Population_By_Community_Districts.csv' USING PigStorage(',') AS (Borough, District, Population);

DUMP population;


//load tonnage table
tonnage = LOAD '/Users/hashaalb/Desktop/BDA/project/thedatasets/DSNY_avg_tonnage_data.csv' USING PigStorage(',') AS (Borough, District, Refuse, Paper, MGP);

DUMP tonnage;

//first join population with tonnage by District
popu_w_tons = JOIN population BY District LEFT OUTER, tonnage BY District;

//store the intermediate result to check them and modify some duplicate columns
//can be done using pig filtering
STORE popu_w_tons INTO '/Users/hashaalb/Desktop/BDA/project/thedatasets/first_join' USING PigStorage(',');

//load the table again after filtering some duplicate columns
pt = LOAD '/Users/hashaalb/Desktop/BDA/project/thedatasets/popu_w_tons.csv' USING PigStorage(',') AS (Borough, District, Population, Refuse, Paper, MGP);

//join the resulting table with rates by District as well
jjj = JOIN pt BY District LEFT OUTER, rates BY District;

//store the final result to local drive modify some duplicate columns
//can modify duplicate columns using pig filtering or outside of pig
STORE jjj INTO '/Users/hashaalb/Desktop/BDA/project/thedatasets/last_join' USING PigStorage(',');