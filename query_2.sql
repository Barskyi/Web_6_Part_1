
select
	s.id,
	s.fullname,
ROUND(AVG(g.grade), 2) as average_grade
from grades g
join students s on s.id = g.student_id
where g.subject_id = 1
group by s.id
order by average_grade desc
limit 1;
