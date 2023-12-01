
SELECT s.fullname AS student_name, t.fullname AS teacher_name, su.name AS subject_name
FROM students s
JOIN groups g ON s.group_id = g.id
JOIN grades gr ON gr.student_id = s.id
JOIN subjects su ON gr.subject_id = su.id
JOIN teachers t ON su.teacher_id = t.id
WHERE s.id = 44;