
SELECT id, fullname FROM students;

SELECT students.fullname, subjects.name AS course_name
FROM students
JOIN groups ON students.group_id = groups.id
JOIN grades ON students.id = grades.student_id
JOIN subjects ON grades.subject_id = subjects.id
WHERE students.id = 33;
