-- Q5. Flight Hopping

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel, public;
DROP TABLE IF EXISTS q5 CASCADE;

CREATE TABLE q5 (
	destination CHAR(3),
	num_flights INT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error
-- the first time this file is imported.
DROP VIEW IF EXISTS intermediate_step CASCADE;
DROP VIEW IF EXISTS day CASCADE;
DROP VIEW IF EXISTS n CASCADE;

CREATE VIEW day AS
SELECT day::date as day FROM q5_parameters;
-- can get the given date using: (SELECT day from day)

CREATE VIEW n AS
SELECT n FROM q5_parameters;
-- can get the given number of flights using: (SELECT n from n)

-- HINT: You can answer the question by writing one recursive query below, without any more views.
-- Your query that answers the question goes below the "insert into" line:

INSERT INTO q5 (
	with recursive answer as (
		(select a.outbound, a.inbound, s_arv, 1 as num
			from flight a
			join day on (a.s_dep- day.day) < interval '1 day' and
				(a.s_dep- day.day) >= interval '0'
			where a.outbound='YYZ')
		union all
		(select a.outbound, a.inbound, a.s_arv, num + 1 as num
			from flight a
			join answer on (a.s_dep- answer.s_arv) < interval '1 day' and
				(a.s_dep- answer.s_arv) >= interval '0'
			where answer.inbound=a.outbound and
				num + 1 <= (select n from n))
	)
	select inbound as destination, num as num_flights from answer
)
















