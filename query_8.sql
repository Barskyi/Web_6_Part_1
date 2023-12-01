
SELECT id, fullname
FROM teachers;

SELECT ROUND(AVG(grade), 2) AS average_grade
FROM grades
WHERE subject_id IN (
    SELECT id
    FROM subjects
    WHERE teacher_id = 3
    );
