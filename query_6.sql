
SELECT students.id, students.fullname, groups.name AS group_name
FROM students
JOIN groups ON students.group_id = groups.id;
