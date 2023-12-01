
SELECT
    teachers.fullname AS teacher_name,
    subjects.name AS course_name
FROM
    teachers
JOIN
    subjects ON teachers.id = subjects.teacher_id;

