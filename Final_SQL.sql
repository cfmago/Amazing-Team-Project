SELECT school, COUNT(*)
FROM comments
GROUP BY school
ORDER BY COUNT(*) DESC;

SELECT school, COUNT(*)
FROM courses
GROUP BY school
ORDER BY COUNT(*) DESC;

SELECT school, COUNT(*)
FROM location
GROUP BY school
ORDER BY COUNT(*) DESC;

SELECT school, AVG(overallScore) AS ovscore, AVG(overall), AVG(curriculum) AS cv, AVG(jobSupport) AS js
FROM comments
GROUP BY school
ORDER BY js DESC;


SELECT keyword, overallScore, c.school 
FROM badges AS b 
INNER JOIN schools as s 
ON s.school = b.school
INNER JOIN comments AS c
ON s.school = c.school
GROUP BY c.school;

SELECT school, graduatingYear, overallScore, overall, curriculum, jobSupport
FROM comments

GROUP BY graduatingYear, school
ORDER BY graduatingYear DESC;

SELECT school, graduatingYear, AVG(overallScore), AVG(overall), AVG(curriculum), AVG(jobSupport)
FROM comments
WHERE school = 'ironhack'
GROUP BY graduatingYear, school
ORDER BY graduatingYear DESC;




