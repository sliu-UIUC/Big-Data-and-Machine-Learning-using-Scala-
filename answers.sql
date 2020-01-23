/*question 1*/ 
use BDA_VQ; 
show tables; 
/*
+---------------------------+
| Tables_in_BDA_VQ          |
+---------------------------+
| code_answers              |
| courses                   |
| expression_assoc          |
| expression_questions      |
| function_assoc            |
| function_questions        |
| lambda_assoc              |
| lambda_questions          |
| mc_answers                |
| multiple_choice_assoc     |
| multiple_choice_questions |
| quiz_course_close_assoc   |
| quizzes                   |
| user_course_assoc         |
| users                     |
| variable_specifications   |
+---------------------------+
16 rows in set (0.00 sec)
*/

/*question 2*/ 
SELECT COUNT(*) FROM users;
/*
+----------+
| COUNT(*) |
+----------+
|      222 |
+----------+
*/

/* question 3 */
SELECT
	table_name, 
	table_rows
FROM
	INFORMATION_SCHEMA.TABLES;

/* 
| code_answers                          |       9488 |
| courses                               |         18 |
| expression_assoc                      |         16 |
| expression_questions                  |         16 |
| function_assoc                        |         27 |
| function_questions                    |         26 |
| lambda_assoc                          |          1 |
| lambda_questions                      |          1 |
| mc_answers                            |       5487 |
| multiple_choice_assoc                 |        111 |
| multiple_choice_questions             |        108 |
| quiz_course_close_assoc               |        253 |
| quizzes                               |         55 |
| user_course_assoc                     |        295 |
| users                                 |        222 |
| variable_specifications               |         36 |
+---------------------------------------+------------+
*/ 

/*question 4*/
SELECT * FROM mc_answers;

/*question 5*/
SELECT * FROM courses;
SELECT COUNT(*) FROM quiz_course_close_assoc WHERE courseid = "3";
/*
+----------+----------+----------+---------+
| courseid | code     | semester | section |
+----------+----------+----------+---------+
|        1 | CSCI1302 | F15      |       6 |
|        2 | Dummy123 | abc      |       0 |
|        3 | CSCI1320 | F15      |       6 |
...
+----------+
| COUNT(*) |
+----------+
|       22 |
+----------+
/*

/*question 6*/
SELECT courseid, COUNT(*) FROM quiz_course_close_assoc GROUP BY courseid ORDER BY COUNT(*) DESC;
SELECT * FROM courses WHERE courseid = "12";
/*
+----------+----------+
| courseid | COUNT(*) |
+----------+----------+
|       12 |       26 |
|       15 |       25 |
...
+----------+----------+----------+---------+
| courseid | code     | semester | section |
+----------+----------+----------+---------+
|       12 | CSCI1320 | F16      |       5 |
+----------+----------+----------+---------+
*/

/*question 7*/ 
SELECT quiz_course_close_assoc.courseid, SUM(q.a)
FROM quiz_course_close_assoc, 
(SELECT quizid, (count(*)) AS a FROM (
    SELECT * FROM function_assoc
    UNION ALL
    SELECT * FROM expression_assoc
    UNION ALL
    SELECT * FROM lambda_assoc
    UNION ALL
    SELECT * FROM multiple_choice_assoc 
) AS q GROUP BY quizid) AS q
WHERE quiz_course_close_assoc.quizid = q.quizid GROUP BY quiz_course_close_assoc.courseid ORDER BY SUM(q.a);
/*65*/

/*question 8*/
/* function: 1, lambda : 2, expression:3*/
SELECT count(*) FROM (   SELECT DISTINCT userid, quizid   FROM code_answers   WHERE correct = 1 AND question_type = 1 ) AS dist;
SELECT count(*) FROM (SELECT DISTINCT userid, quizid FROM code_answers WHERE question_type = 1) AS dist;
SELECT 585/ 816;
/*
+----------+
| COUNT(*) |
+----------+
|      585 |
+----------+

+----------+
| COUNT(*) |
+----------+
|     816 |
+----------+

+----------+
| 585/816  |
+----------+
|   0.7169 |
+----------+
*/

/*question 9*/
SELECT quizid, question_id, answer_time FROM code_answers WHERE question_type = "2" GROUP BY answer_time; 
select * from quiz_course_close_assoc where quizid = "11";

/*question 10*/
SELECT userid, COUNT(*) FROM (SELECT userid FROM code_answers UNION ALL SELECT userid FROM mc_answers) AS t WHERE correct = "1" GROUP BY userid ORDER BY COUNT(*) ASC; 
/* 
+--------+----------+
| userid | COUNT(*) |
+--------+----------+
|     39 |       26 |
|    137 |       25 |
|    136 |       25 |
|    140 |       24 |
|    129 |       24 | 
... */
/* question 11 */ 
SELECT spec_type, COUNT(*) FROM variable_specifications GROUP BY spec_type ORDER BY COUNT(*) DESC; 
/*
+-----------+----------+
| spec_type | COUNT(*) |
+-----------+----------+
|         0 |       13 |
|         2 |        6 |
|         3 |        5 |
|         4 |        4 |
|         6 |        4 |
|         1 |        2 |
|         5 |        1 |
|         7 |        1 |
+-----------+----------+ */
/*question 12 */
SELECT DISTINCT quizid FROM code_answers ; 
SELECT COUNT(*) FROM quizzes;
SELECT 21/55;

/*question 13 */ 
SELECT COUNT(*) FROM multiple_choice_assoc;
SELECT COUNT(*) FROM quizzes;
SELECT 111/55;  /*2.0182*/

/*question 14*/
SELECT DISTINCT quizid, question_id FROM code_answers; 
SELECT 42/55; 

/*question 15*/ 
/*SELECT userid, quizid, COUNT(*) FROM code_answers GROUP BY userid, quizid ORDER BY COUNT(*) ASC; */
SELECT userid, quizid, COUNT(*) FROM (SELECT userid, quizid from code_answers UNION ALL SELECT userid, quizid FROM mc_answers) AS t GROUP BY userid, quizid ORDER BY COUNT(*) ASC;
/* 58 47 212*/
