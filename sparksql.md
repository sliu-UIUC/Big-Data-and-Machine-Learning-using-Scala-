1. How many data series does the state of New Mexico have?
   * There are 99496 data entries. 
2. What is the highest unemployment level (not rate) reported for a county or equivalent in the time series for the state of New Mexico?
   * It is 86751.0. 
3. How many cities/towns with more than 25,000 people does the BLS track in New Mexico?
   * 10 cities or towns have more than 25,000 people in New Mexico. 
4. What was the average unemployment rate for New Mexico in 2017? Calculate this in three ways:
    a. Averages of the months for the BLS series for the whole state.
	* The average is 6.449230769230769% (annual total rate). 
	
    b. Simple average of all unemployment rates for counties in the state.
	* The average is 6.557575757575757%. 
5. This is a continuation of the last question. Calculate the unemployment rate in a third way and discuss the differences.

    c. Weighted average of all unemployment rates for counties in the state where the weight is the labor force in that month.
    	
	* The weighted average is 6.152186167397469%.
	
    d. How do your two averages compare to the BLS average? Which is more accurate and why?
	
	* The previous two are higher compare to the weighted average. Weighted average is clearly more accurate since it takes the different amount of labor force into consideration, thus the calculated average is a more accurate reflection of reality. The first two methods did not consider different amount of labor force, it has more bias in its result, since the unemployment rate is only the average of percentage instead of how many people got unemployed compare to total labor force. For example, it could be the case that at some time in some area the total labor force is really low. Even though unemployed people are only a few, the calculated percentage of unemployment can be high. Without considering weight it could drive up the total result if you simply take the average of percentage. 

6. What is the highest unemployment rate for a series with a labor force of at least 10,000 people in the state of Texas? When and where? (raise labor force limit for next year)
   * The highest unemployment rate in Texas is 54.1%. It happened twice: both are in Feburary,1990. One is in Rio Grande City, TX Micropolitan Statistical Area and one is in Starr County. 

The following questions involve all the states.

7. What is the highest unemployment rate for a series with a labor force of at least 10,000 people in the full dataset? When and where? (raise labor force limit for next year)

   * The highest unemployment rate in all states is 58.7%. It is in July, 2013 in San Luis city, Arizona. 
   
8. Which state has most distinct data series? How many series does it have?

   * Nebraska has the most distinct data series. It has 476 distinct data series. (I used la.series dataset, which claims to have all series.)
   
9. We will finish up by looking at unemployment geographically and over time. I want you to make a grid of scatter plots for the years 2000, 2005, 2010, and 2015. Each point is plotted at the X, Y for latitude and longitude and it should be colored by the unemployment rate. If you are using SwiftVis2, the Plot.scatterPlotGrid method is particularly suited for this. Only plot data from the continental US, so don't include data from Alaska, Hawaii, or Puerto Rico.

   * ![alt_text](https://github.com/CSCI3395-F18/big-data-assignments-f18-sliu-trinity/blob/master/src/main/scala/sparksql/graphs/unemploymentGridPlot)
