1. This dataset is challenging to read in, so all I'm going to ask you for is to tell me the meaning of the following fields and give me the statistics you get from "describe" for each.
    * a. GENHLTH
    * b. PHYSHLTH
    * c. MENTHLTH
    * d. POORHLTH    
    * e. EXERANY2
    * f. SLEPTIM1
    * * GENHLTH represents the response to the question asking how well your health is in general. It has integer value from 1 to 5 representing "Excellent" to "Poor". A 7 means "Dont know/Not sure". A 9 means the responder refused to answer. Blank means not asked or missing (Represent with -0.001 at this moment).
    * * PHYSHLTH represents the number of days during the past 30 days that the responder feels not good about his or her physical health, taking physical illness and injury into consideration. Number of days ranges from 0 to 30. 77 means "Not sure/ Don't know". 99 represents "Refuse to answer".
    * * MENTHLTH represents the number of days during the past 30 days that the responder feelts not good about his or her mental health, including stress, depression and problems with emotions. 0 to 30 represents the number of days. 77 means "dont know/not sure" and 99 means the responder refuses to answer. 
    * * POORHLTH represents the number of days that keeps the responder from doing daily activities due to poor mental or physical health. 0 to 30 represents the number od days. 77 means dont know and 99 means refused to ask.
    * * EXERANY2 represents whether the responder participate in any physical activities or exercises such as running, calisthenics, golf, gardening or walking for exercise, other than regular job, during the past month. 1 means yes and 0 means no. 7 means unsure and 9 means refuse to answer. -0.001 means missing or not asked.
    * * SLEPTIM1 represents the average hours of sleep in a 24 hour period. 1 to 24 represents the number of hours. 77 means unsure and 99 means refuse to answer. -0.001 means missing or not asked.   
    
|summary|     GENHLTH      |
|-------|------------------|
|  count|            486303|
|   mean| 5.473836264633366|
| stddev|3.4470045959749496|
|    min|            -0.001|
|    max|               9.0|

|summary|         PHYSHLTH|
|-------|-----------------|
|  count|           486303|
|   mean| 65.5723818277905|
| stddev|31.16934023520132|
|    min|              0.0|
|    max|             99.0|

|summary|          MENTHLTH|
|-------|------------------|
|  count|            486303|
|   mean|30.810548156190688|
| stddev|32.125000004590426|
|    min|               0.0|
|    max|              99.0|

|summary|          POORHLTH|
|-------|------------------|
|  count|            486303|
|   mean|29.994067484675192|
| stddev| 36.05172810537815|
|    min|               1.0|
|    max|              99.0|

(notice that for PHYSHLTH, MENTHLTH and POORHLTH there is no blank entry.)

|summary|          EXERANY2|
|-------|------------------|
|  count|            486303|
|   mean|0.1218088270070306|
| stddev|0.8013319781081388|
|    min|            -0.001|
|    max|               9.0|
 
|summary|         SLEPTIM1|
|-------|-----------------|
|  count|           486303|
|   mean|68.85272555999039|
| stddev|16.74112923552049|
|    min|           -0.001|
|    max|             99.0|

2. The fields GENHLTH, PHYSHLTH, MENTHLTH, and POORHLTH give the respondents self-assessment of their health in different ways. Using regression analysis, I want you to try to predict the values of these using other values in the data set. What other survey questions are most significant in predicting each of these four values? Does this result make sense?

* I first filtered out all the values (for instance, date asked, whether using cell phones or races) that, I believe, are irrelevant to our predicted value. I then filtered our all the data that either have no recorded entries or have a large portion (>=20%) of entries that are underministic or simply blank. Finally, we are left with about 20 variables out of our nearly 300 variables dataset. I first run a linear regression and correlation on all of them. I then picked 5 variables with the highest correlation with our predicted value and get the result. (For more thorough result check the regResult.txt file in my code repository. Here I am only going to include result with the 5 most significant variables). 

For general health (GENHLTH), the 5 most significant variables are CHCCOPD1, HAVARTH3, INCOME2, SLEPTIM1 and CHECKUP1 in descending order. The result makes sense to me. Level of income, amount of sleep and how often you go to see doctors definitely influence your health in general. It is also not surprising that diseases like COPD or arthritis have a strong impact on people's perception about their general health condition, since diseases like these are very common and have very obvious symptoms. These diseases can also be really severe and can greatly change people's daily life.

For physical health (PHYSHLTH), the 5 most significant variables are the same as general health. CHECKUP1 is slightly more significant than SLEPTIM1 now. Others remain in the same order. Explainations same as for general health. It is interesting to point out that people tend to be more observant about their physical health. 

For mental health (MENTHLTH), the 5 most significant variables are RFHLTH, PHYS14D, CHCCOPD1, VETERAN3 and DIABETE3 (DESC). The 2 most significant are RFHLTH, PHYS14D. It is not surprising that veterans tend to have more days with bad mental condition. Some of them may suffer from PTSD, which is really common among old veterans. It raises our attention to treat veterans better and take good care of them not just materially but also mentally. It is also reasonable that people with diabetes suffer are not happy. Except from possible physical pain, they also cannot enjoy so many delicious food, which I can deeply understand and hope I will never get diabet. It is surprising that adults with good health and arthritis have significant correlation with mental health, especially the sign of arthrtis alike diseases correlating to mental health is negative (which means if you get diseases like arthritis you will be more happy?) 

For poor health (POORHLTH), the 2 most significant variables are the same as mental health. Here we only included 2 variables because these 2 have really high correlation with our predicted value POORHLTH, while all other variabels are not significant. 

GENEHLTH prediction: 

|summary|          prediction|
|-------|--------------------|
|  count|              486303|
|   mean|   5.473836264633337|
| stddev|  0.7513793837437006|
|    min|-0.03766409053359965|
|    max|  15.141711039918189|

PHYSHLTH prediction: 

|summary|        prediction|
|-------|------------------|
|  count|            486303|
|   mean| 65.57238182779273|
| stddev| 8.669707000580416|
|    min|-9.333757437932888|
|    max|169.85861828218066|

MENTHLTH prediction (five variables) : 

|summary|        prediction|
|-------|------------------|
|  count|            486303|
|   mean|30.810548156190286|
| stddev|15.144153559232537|
|    min| 8.219185452205503|
|    max| 122.9319579156381|

MENTHLTH prediciton (on_RFHLTH, PHYS14d only): 

|summary|        prediction|
|-------|------------------|
|  count|            486303|
|   mean| 30.81054815619021|
| stddev|15.086402888506774|
|    min| 22.91018858115651|
|    max|119.32403272381589|

POORHLTH prediction ( on rfhlth, PHYS14D): 

|summary|        prediction|
|-------|------------------|
|  count|            486303|
|   mean|29.994067484677764|
| stddev| 15.17115394934401|
|    min|20.774557585695142|
|    max|144.38420925779346|

3. How do things change if you can include each of the original four values in trying to predict the other ones?
 * When I include each of the other values in predicting one value (for instance, while predicting GENHLTH we include PHYSHLTH, MENTHLTH, POORHLTH), I noticed that GENHLTH and PHYSHLTH has very strong, positive correlation with each other (close to 0.9). GENHLTH have an average negative correlation with MENTHLTH and POORHLTH. MENTHLTH also have strong positive correlation with POORHLTH, which is kind of interesting and yet reasonable. People do think about their health in terms of physical health more than mental health and sometimes they tend to overlook mental health. Interestingly, it also makes sense that people suffer from mental problems tend to be less productive and even drop out of their work because of that. 
 While it is interesting to know their relationship to each other, it might not be ideal to include them along with other variables in our regression. For instance, when we include physical health, mental health and poor health variables in predicting general health, the correlation coefficient for variables like sleep time and lung diseases become less significant, but clearly those variables are actually significant in predicting mental health both from a empirical stand point and our previous regression. The reason is that we we introduce variables that are really strongly correlated with each other, we introduced the multicollinearity problem that biases our data. Multicollinearity happens because we introduce some variables that you can predict using other variables with substantial accuracy, thus there is no need to introduce this variable. For example, because general health and physical health is so highly correlated with each other that we can almost surely predict one given the other, introducing one into predicting the other will bias our result by introducing the problem of multicollinearity.  
