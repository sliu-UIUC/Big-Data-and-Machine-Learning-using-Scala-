1. Which aggregation level codes are for county-level data? How many entries are in the main data file for each of the county level codes?

|agglvl_code|count(agglvl_code)|
|-----------|------------------|
|         73|            277932|
|         71|             51964|
|         70|             13084|
|         75|           1100372|
|         78|           3984876|
|         77|           3396052|
|         72|             76460|
|         74|            406868|
|         76|           2162440|

2. How many entries does the main file have for Bexar County?
* There are 9244 Bexar County entries. 

3. What are the three most common industry codes by the number of records? How many records for each?

|industry_code|count|
|-------------|-----|
|           10|76952|
|          102|54360|
|         1025|40588|

4. What three industries have the largest total wages for 2016? What are those total wages? (Consider only NAICS 6-digit County values.)

|industry_code|industryTitle                                      |sum(total_qtrly_wages)|
|-------------|---------------------------------------------------|----------------------|
|611110       |NAICS 611110 Elementary and secondary schools      |2.4432185309E11       |
|551114       |NAICS 551114 Managing offices                      |2.1938760563E11       |
|622110       |NAICS 622110 General medical and surgical hospitals|2.05806016743E11      |

5. Explore clustering options on the BLS data to try to find clusters that align with voting tendencies. You will do this for two clusters and for more clusters. 
 * Using month3_emplvl, taxable_qtrly_wages, avg_wkly_wage and oty_month3_emplvl_chg columnsI achieved 33.43% accuracy using 2 clusters and 31.92% using 3 clusters. 

6. Make scatter plots of the voting results of the 2016 election (I suggest ratio of votes for each party) and each of your groupings. Use color schemes that make it clear how good a job the clustering did.

 * ![alt_text](https://github.com/CSCI3395-F18/big-data-assignments-f18-sliu-trinity/blob/master/src/main/scala/sparkml/graphs/result2Cluster.png)
