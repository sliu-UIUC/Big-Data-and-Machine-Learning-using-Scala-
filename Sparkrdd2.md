1. What location has reported the largest temperature difference in one day (TMAX-TMIN) in 2017? What is the difference and when did it happen?
* The largest daily temperature difference is 125.2 degress Celcius (T-flag failed error). It is measured in station USS0047P03S, Fairbanks F.O., AK, US in 2017.07.19. 

2. What location has reported the largest difference between min and max temperatures overall in 2017. What was the difference?
* The USS0045R01S station in Fort Yukon has the largest difference between min and max temperatures overall in 2017. The difference is 129.9 degrees Celcius.

3. What is the standard deviation of the high temperatures for all US stations in 2017? What about low temperatures?
* The standard deviation of U.S high temperatures is 118.01571248027781. That for low temperatures is 105.55764819304729. 

4. How many stations reported data in both 1897 and 2017?
* There are 1871 stations that reported data in both 1897 and 2017. 

5. Does temperature variability change with latitude in the US in 2017? Consider three groups of latitude: lat<35, 35<lat<42, and 42<lat. Answer this question in the following three ways.
    * a. Standard deviation of high temperatures.
    * * Standard deviation of high temperatures for lat<35, 35<lat<42, and 42<lat are: 77.4718697979876, 105.08936590234607, and 122.58541587072514 respectively. 
     
    * b. Standard deviation of average daily temperatures (when you have both a high and a low for a given day at a given station).
    * * The standard deviations of average daily temperatures for these three groups in order are: 76.46104076820222, 96.71013431970741 and 109.77906417438031.
    
    * c. Make histograms of the high temps for all stations in each of the regions so you can visually inspect the breadth of the distribution.
    ![alt_text](https://github.com/CSCI3395-F18/big-data-assignments-f18-sliu-trinity/blob/master/src/main/scala/sparkrdd2/graphs/histTempLess35) 
    
    ![alt_text](https://github.com/CSCI3395-F18/big-data-assignments-f18-sliu-trinity/blob/master/src/main/scala/sparkrdd2/graphs/histTemp35To42) 
    
    ![alt_text](https://github.com/CSCI3395-F18/big-data-assignments-f18-sliu-trinity/blob/master/src/main/scala/sparkrdd2/graphs/histTempMore42) 
    
6. Plot the average high temperature for every station that has reported temperature data in 2017 with a scatter plot using longitude and latitude for x and y and the average daily temperature for color. Make 100F or higher solid red, 50F solid green, and 0F or lower solid blue. Use a SwiftVis2 ColorGradient for the scatter plot color.
      ![alt_text](https://github.com/CSCI3395-F18/big-data-assignments-f18-sliu-trinity/blob/master/src/main/scala/sparkrdd2/graphs/avgHighTemp2017) 
      
7. How much has the average land temperature changed from 1897 to 2017? We will calculate this in a few ways.
    * a. Calculate the average of all temperature values of all stations for 1897 and compare to the same for 2017.
    * * The average of all temperature is 10.852 degrees Celcius for 1897 and 11.290 degrees Celcius for 2017.
    The average temperature in 2017 is about 0.438 degree Celcius higher than that in 1897. 
    
    * b. Calculate the average of all temperature values only for stations that reported temperature data for both 1897 and 2017.
    * * The average of all temperature is 10.927 degrees Celcius for 1897 and 11.972 degrees Celcius for 2017.
    The average temperature in 2017 is 1.045 degree Celcius higher than that in 1897.
    
    * c. Plot data using approach (a) for all years I give you data for from 1897 to 2017. (On the Pandora machines under /data/BigData/ghcn-daily you will find a file for every 10 years from 1897 to 2017.)
    ![alt_text](https://github.com/CSCI3395-F18/big-data-assignments-f18-sliu-trinity/blob/master/src/main/scala/sparkrdd2/graphs/avgHTAll)
    * d. Plot data using approach (b) for all years I give you data for from 1897 to 2017. (Use the files for every 10 years.)
    ![alt_text](https://github.com/CSCI3395-F18/big-data-assignments-f18-sliu-trinity/blob/master/src/main/scala/sparkrdd2/graphs/avgHTCommon) 
    
8. Describe the relative merits and flaws with approach (a) and (b) for question 7. What would be a better approach to answering that question?
   * Approach (a) has more data to work with, because the number of stations it has is much larger than the common stations in 1897 and 2017. But a lot of those stations don't report data in both 1897 and 2017. When we analyze the average temperature in both year, those that only have 1 year's data become less significant and even biased to our result, because they are irrelevant to the difference between 1897 and 2017. Approach (b) has much less data since it only covers the common stations that report data in both year, but it can correctly show the temperature differences between these two years. Since the amount of data it has is generally smaller than approach (a), the standard deviation might be high. Approach(b) that only chooses data from stations that reported in all these years seem to be the better approach. It eliminates the bias generated by including irrelevant stations. Also, we can see it visually from graph in (a) that because we include so many stations that don't report data in every year, the variation between each year is big and unpredictable, and we can not see a trend from (a), where as in graph (b) because we only include stations that have reported data in all these years, we are able to see a upward trend of temperature, which matches the reality of global warming.

9. While answering any of the above questions, did you find anything that makes you believe there is a flaw in the data? If so, what was it, and how would you propose identifying and removing such flaws in serious analysis?
   * Some of these data are just wrong, like in question 1 and 2 the biggest differences are over 120 degree Celcius (impossible!). The lowest temperature in question 1, for instance, is -99.9 degrees Celcius.It makes no since that a place could have -99.9 degrees Celcius, not mentioning that the biggest difference is nearly 130 degree Celcius. The reason is that some of these data actually failed to pass some tests and should be treated as data with error. These data are labeled with different flags indicating different kinds of failures and errors in their measurement. If we don't single out those data with flags we will most likely get wrong results when we try to find answer to questions like largest daily difference. We can do a filter over the data first to get rid of datas with fail flags and then work on the rest of the data, which are considered correct measurements without failures. 
