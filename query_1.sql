
select
	s.id,
	s.fullname,
	ROUND(AVG(g.grade), 2) as average_grade
from students s
join grades g on s.id = g.student_id
group by s.id
order by average_grade desc
limit 5;