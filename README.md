# Big Data and Machine Learning - Fall 2018

1st Data Set: 
--	Data set link: https://www.kaggle.com/drgilermo/nba-players-stats 

--	Description: The total size of the data is 5.47 MB. The Seasons_Stats.csv files includes basic statistics like number of points, rebounds and assists, percentagewise statistics like 3-point field goal percentage and free throw percentage, and high-level statistics like PER (player efficiency rating) and WS (win shares) for more than 3000 different players from each season since 1950 to 2017. 
  The player_data.csv file have player’s height, weight and college graduated information. Meanwhile, the years started and years ended can be used to calculate the number of years that player plays in the league. The birth date info can be used to calculate things like how old the player entered the league and the average retire age the player has. 
  The player.csv file contains information that is basically all covered in player_data file. Additionally, it also provides us with birth cities and birth states for all players. 
  From our data we immediately noticed that some data like number of steals and blocks weren’t being calculated until 1974. Three point field goal wasn’t introduced to National Basketball Association(NBA) until the 1979-1980 season. To answer questions related to these data, we need to filter out the time before to have a valid data set to work with. 

-- Questions: 1. What’s the scoring/rebounds/ assists leaders in each year? 
	      2. What’s the average scores, rebounds and assists in 1987 and 2017 respectively? How are they different? 
	      3. Which team scores the most points in a game since 2010? 
	      4. Do the differences between scores for different players become larger in the 2010s compare to those in the 1990s?
	      5. Does the game become more fast-paced nowadays compare to the 90s? (Plots might be shown to see the trend of average points made by each team) 
              
-- -	Explanations: I think that NBA statistics have always been interesting to analyze, especially from the fact that globally NBA has always been the most popular sports from the U.S. Each year, tons of professional analysists are analyzing the data for each NBA team, providing useful information for coaches to train their players and for players to notice their strengths and weaknesses. I think that analyzing NBA statistics will in a sense help NBA players improve and become more specialized and professional, thus making basketball games more enjoyable and eventually increasing the amount of revenues gained each year for this franchise. While some questions like question 1 and 2 might be easier to answer, in question 3 you need to filter all the players in that year, categorize them by team, and calculate the total number of points and pick the highest. I noticed that to calculate the scores for the whole team you actually need to know: all the players in the team in that year, how many games each player played, total number of games in that year and the amount of scores made by each player.  To answer question 4 we might calculate the standard deviation for scores for these two period seperately.  For question 5 we might not only calculate the number of points per minute by each team, but also efficiencies like the field goal percentage in each team. For example, it might be the case that teams don’t necessarily play faster, but because there is an increase in overall field goal percentage and less turn overs, they were able to increase the number of points they get.

2nd Data Set: 
--	Data set link: https://www.kaggle.com/ixioixi/3-million-russian-troll-tweets-538

--	Description: This data set is 656 MB in size after decompressing. It contains 3 million tweet entries that are suspected to be manipulated by Russian agencies that favors Donald Trump’s presidency.  All the tweets in the dataset were sent between 2012 to 2018. It contains information like user id, author, tweets content, region, language, publish date, account types and account category. Account types and account categories are specific account theme and general account them coded by Linvill and Warren, so there might be some potential biases. 

-- Questions: 1. Among those tweets, how many are from regions other than the U.S? How many are from Russia? 
              2. Draw a frequency distribution over the period from 2012 to 2018. Which period has the most amount of tweets? 
              3. Based on the account type, what fraction of these tweets are sent by non-English? What is the lefttroll –righttroll ratio? 
              4. What can you tell from the periods where the amounts of tweets are the highest? Do they come closely with important dates like presidential debates? 
              5. Is there any relationship between the length of a tweet and the region that the tweet comes from? What about the relationship between tweet length and account category (left, right or non-English)? 

--	Explanations: I think that this presidential election has always been a debatable one, even it has been 2 years. The most interesting part is that I think people tend to hide their true supporting party in real life than on the internet. Almost every people I have met supports Hillary, but eventually Trump becomes the president. Of course, there might be some other unspoken factors that might lead to this result. I keep hearing news about Russian’s interference about the election and don’t know if it is true or not. But with this data at least it can provide me with some clues about the truth. If we can find from the data that the periods, where the numbers of Russian troll tweets are at its peak, are closely tied with important dates like presidential debate, or if lefttroll-righttroll ratio is really small, we might suspect that there are some clandestine factors that are trying to interfere by using tweets to manipulate the election on the internet. Also, it is also interesting to know whether left-troll accounts tweet less than right-troll accounts or whether there is any relationship between the length of tweets and regions those tweets come from. These questions might give us some information about how different regions and different believes might lead to different online behaviors. 

3rd Data Set: 
--	Data set link: https://catalog.data.gov/dataset?tags=unemployment-rate 
                   https://www.kaggle.com/lislejoem/us-minimum-wage-by-state-from-1968-to-2017/home 

--	Description: In the first link, to get the file, we actually need to click in the link (csv format), and the third file is the file we are looking for. This dataset is 3.46 MB in size and contains different counties labor force number, number employed and unemployment rate for each month from year 1968 to 2018. 
  The second file has a size of 176 KB. It is the US minimum wage data by state from 1968 to 2017 and 2018 equivalent dollars. Some data in this file has multiple values in its table_data. High.Value is used to indicate the highest minimum wage among those values. Similarly, Low.Value is used to indicate the lowest minimum wage in these multiple values. CPI.Average is the average consumer price index in that year. High.2018 and Low.2018 is used to indicate the 2018-equivalent dollars for High-Value and Low-Value. 

-- Questions:  1. How did the labor force population in New York change over time? 
               2. Draw the unemployment rate trend for area New York State from 1968 to 2018. What is trend of the unemployment rate? 
               3. What is the participation rate for each county in New York in 2017? 
               4. Is there any relationship between participation rate and unemployment rate? 
               5. How does unemployment correlate with minimum wages from 1968 to 2017? If minimum wage increases, will it increase or decrease unemployment rate? 

--	Explanations: I find it really interesting about the versatility of big data that it can also be used to analyze economics problems. I happen to find that both sources in this data set contains information from 1968 to 2017. It might be a good combination to find the trend of unemployment rate and participation rate in New York city. I think that it is also interesting to know that whether there is a relationship between work force participation rate and employment rate. Another question that is interesting to analyze is the how minimum wage correlates with unemployment. I personally think it is debatable: some people argues that an increase in minimum wage will be followed by an increase in unemployment since companies don’t have enough money to employ the same amount of workers, while others think that the company can always balance it by increasing the price of the product.  I think it would be interesting to find out what is the case for New York.
