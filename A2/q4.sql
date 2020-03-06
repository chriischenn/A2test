-- Q4. Plane Capacity Histogram

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel, public;
DROP TABLE IF EXISTS q4 CASCADE;

CREATE TABLE q4 (
	airline CHAR(2),
	tail_number CHAR(5),
	very_low INT,
	low INT,
	fair INT,
	normal INT,
	high INT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS allDepartedBooked CASCADE;
DROP VIEW IF EXISTS bookedInfo CASCADE;
DROP VIEW IF EXISTS result CASCADE;


-- Define views for your intermediate steps here:

create view allDepartedBooked as
select distinct flight.id, airline, plane
from flight, departure, booking
where flight.id=departure.flight_id
and flight.id=booking.flight_id;

create view bookedInfo as
select distinct allDepartedBooked.id, allDepartedBooked.airline, plane as tail_number,

count(pass_id) as num_pass,

(capacity_economy + capacity_business + capacity_first) as total_cap,

(100 * round(
	(count(pass_id)::decimal /
	(capacity_economy + capacity_business + capacity_first)::decimal), 4))::integer
	as per
from allDepartedBooked, plane, booking
where allDepartedBooked.plane=plane.tail_number and
booking.flight_id=allDepartedBooked.id
group by allDepartedBooked.id, allDepartedBooked.airline, plane, capacity_economy,
capacity_business, capacity_first;

create view result as
select airline, tail_number,
count(case when per>=0 and per<20 then 1 else NULL end) as very_low,
count(case when per>=20 and per<40 then 1 else NULL end) as low,
count(case when per>=40 and per<60 then 1 else NULL end) as fair,
count(case when per>=60 and per<80 then 1 else NULL end) as normal,
count(case when per>=80 then 1 else NULL end) as high
from bookedInfo
group by airline, tail_number;


-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q4 (select * from result)
