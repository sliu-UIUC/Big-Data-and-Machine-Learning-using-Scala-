# Final Project: 3 million Russian Trolling Tweets

## Description
This data set is 656 MB in total size after decompressing. It contains 3 million tweet entries that are suspected to be manipulated by Russian agencies (IRA, the Internet Research Agency) that favors Donald Trump’s presidency. All the tweets in the dataset were sent between 2012 to 2018. Mostly, the high frequency periods focuses on 2016 and 2017. It contains information like user id, author, tweets content, region, language, publish date, account types and account category. Account types and account categories are specific account theme and general account them coded by Linvill and Warren.


## Basic Analysis
We first look at this data by looking at variaous values in its major columns. We found out that among the 2973371 tweet entries, about 70.9% of them were written in English and the second highest language is Russian, which counts as 20.9%. Coincidentally, the second highest region type is unknown, which is really suspicious considering 21% of the data is written in Russian but region percentage for Russsian is so much smaller. The peaks of trolling activities focus on periods in year 2015 to 2017. Specificially, it has 150 thousands of tweets in October,2016, which is just before the Wikileak's reveal of Hilary Clinton's emails and the election day. It also recorded anouther 150 thousands of tweets after the election. 2017 summer is the highest season for right trolling activities. 
During the process of basic analysis, we found out that each tweet is labeled with different account types by people who collected data. List includes Non-English, right trolls, left trolls, commercials etc. We believe this could be interesting to predict account types based on values in other fields. 


## Machine Learning Result
We first get region, language, following, followers, updates, retweet, account_type and account_category as columns used in the analysis. 
We user defined functions to map all string entries to numerical types. Since in the basic analysis we suspect that the unknown region type could be correlated to trolling activity, we set all entries with unknown value 1 and others 0. Similarly, we set the Russian language to be 1 and other languages 0. In the account_type field we set "right" to be 1 and others 0. In our label column account_category, we assign different values to different type of behaviors from the most righty aggressive behaviors to lefty behaviors(rightTroll is 6, fearmonger 5, news feed and Commercial are considered neutral which is 3 and 2. Left troll is 1). We first try to predict the value of last columns using previous ones.
Then, we created a Pearson correlation matrix, and found out that region and account type column is highly correlated with account category, with correlation coefficient of 0.7804926825301549 and 0.28461562021831405 repectively. Using these two columns we performed a linear regression on the data. We found that region's coefficient is 0.6963116252820617 and that for account_type is2.865998779166602. The interception is The interception is: 2.9248982084337265.

|summary|        prediction|
|-------|------------------|
|  count|           2932420|
|   mean|3.7515874260848783|
| stddev| 1.297472227187376|
|    min|2.9248982084337265|
|    max| 6.487208612882391|

In further analysis, we used multiclass classifications to predict the last column using the previous ones. Using random forest algorithm, we acheived 98.15% accuracy on 20% randomly chossen test set.


## Conclusion
After the project I believe that the Russian Internet Research Agency is involved in trolling tweets to interfere with elections. Notice that there are some mixed up types of tweets, either in the form of left trolls or commercials, to confuse others. Yet we see a strong correlation between region, account type and account category among these data. Unknown regions and right-leaning account types tend to result in more right-leaning or aggressive behaviors, like fear mongering. Random forrest classfier acchieves very high accuracy on predicting account types. 


## Limitations
Of course, this data has its limitations as well. First off, it has thousands of duplicate entries that accounts for more than 15% of the data set, which could bias the result. Also, there are many mislabeled data, and most of them are mismatched between different columns; for example, the region column might be filled with tweet content or language instead of region. Although only taking up a small portion of the total dataset, it does make reading the data properly and debugging more challenging. 
Another limitation is that since the account_category is filled with string types, we have to define a mechanics to map them into different values, which can bring bias. The choice for what value to assign to each category has a strong impact on our regression and classification result. 
