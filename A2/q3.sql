-- Q3. North and South Connections

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel, public;
DROP TABLE IF EXISTS q3 CASCADE;

CREATE TABLE q3 (
    outbound VARCHAR(30),
    inbound VARCHAR(30),
    direct INT,
    one_con INT,
    two_con INT,
    earliest timestamp
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS onThatDay CASCADE;
DROP VIEW IF EXISTS allFlightOnThatDay CASCADE;
DROP VIEW IF EXISTS allNAFlightOnThatDay CASCADE;
DROP VIEW IF EXISTS directFlight CASCADE;
DROP VIEW IF EXISTS oneCon CASCADE;
DROP VIEW IF EXISTS twoCon CASCADE;
DROP VIEW IF EXISTS allNACities CASCADE;
DROP VIEW IF EXISTS directCount CASCADE;
DROP VIEW IF EXISTS oneConCount CASCADE;
DROP VIEW IF EXISTS twoConCount CASCADE;
DROP VIEW IF EXISTS directArrival CASCADE;
DROP VIEW IF EXISTS oneConArrival CASCADE;
DROP VIEW IF EXISTS twoConArrival CASCADE;
DROP VIEW IF EXISTS finalCount CASCADE;
DROP VIEW IF EXISTS allArrival CASCADE;
DROP VIEW IF EXISTS answer CASCADE;


-- Define views for your intermediate steps here:
create view onThatDay as
select * from flight
where date_part('year', s_dep)=2020 and
date_part('month', s_dep)=4 and
date_part('day', s_dep)=30 and
date_part('year', s_arv)=2020 and
date_part('month', s_arv)=4 and
date_part('day', s_arv)=30;

-- all flights on 2020-04-30
create view allFlightOnThatDay as
select onThatDay.id,
(outboundAirport.city) as outbound,
(outboundAirport.country) as outbound_country,
(inboundAirport.city) as inbound,
(inboundAirport.country) as inbound_country,
s_dep,
s_arv
from onThatDay
JOIN airport AS inboundAirport ON onThatDay.inbound=inboundAirport.code
JOIN airport AS outboundAirport ON onThatDay.outbound=outboundAirport.code;

create view allNAFlightOnThatDay as
select id, outbound, inbound
from allFlightOnThatDay
where
(outbound_country='USA' and inbound_country='Canada') or
(outbound_country='Canada' and inbound_country='USA');

create view directFlight as
select distinct allFlightOnThatDay.id as id, allFlightOnThatDay.outbound, allFlightOnThatDay.inbound,

-- get arrival time
case when (allFlightOnThatDay.id in (select id from flight)) then
    (select s_arv from flight where allFlightOnThatDay.id=flight.id and
    date_part('month', s_arv)=4 and
    date_part('day', s_arv)=30 and
    date_part('year', s_arv)=2020)
    else NULL end
    as arv

from allFlightOnThatDay, allNAFlightOnThatDay, flight
where allNAFlightOnThatDay.outbound=allFlightOnThatDay.outbound and
allNAFlightOnThatDay.inbound=allFlightOnThatDay.inbound;

create view oneCon as
select distinct a1.id as first_leg_id,
a2.id as second_leg_id,
a1.outbound as outbound, a2.outbound as connection, a3.inbound as inbound,

-- get arrival time
case when (a3.id in (select id from flight)) then
    (select s_arv from flight where a3.id=flight.id and
    date_part('month', s_arv)=4 and
    date_part('day', s_arv)=30 and
    date_part('year', s_arv)=2020)
    else NULL end
    as arv

from allFlightOnThatDay a1, allFlightOnThatDay a2, allFlightOnThatDay a3, flight
where a1.inbound=a2.outbound and
a2.outbound=a3.outbound and
a3.id=a2.id and

-- calculate connection time
((date_part('day', a2.s_dep::timestamp - a1.s_arv::timestamp) * 24 +
date_part('hour', a2.s_dep::timestamp - a1.s_arv::timestamp)) * 60 +
date_part('minute', a2.s_dep::timestamp - a1.s_arv::timestamp)) >= 30 and

(a1.outbound_country='USA' and a3.inbound_country='Canada'
or a1.outbound_country='Canada' and a3.inbound_country='USA');

create view twoCon as
select distinct a1.id as first_leg_id,
a2.id as second_leg_id,
a3.id as third_leg_id,
a1.outbound as outbound, a2.outbound as first_con,
a3.outbound as second_con, a4.inbound as inbound,

-- get arrival time
case when (a4.id in (select id from flight)) then
    (select s_arv from flight where a4.id=flight.id and
    date_part('month', s_arv)=4 and
    date_part('day', s_arv)=30 and
    date_part('year', s_arv)=2020)
    else NULL end
    as arv

from allFlightOnThatDay a1, allFlightOnThatDay a2, allFlightOnThatDay a3, allFlightOnThatDay a4, flight
where a1.inbound=a2.outbound and
a2.inbound=a3.outbound and
a3.outbound=a4.outbound and
a4.id=a3.id and

-- calculate connection time
-- first leg
((date_part('day', a2.s_dep::timestamp - a1.s_arv::timestamp) * 24 +
date_part('hour', a2.s_dep::timestamp - a1.s_arv::timestamp)) * 60 +
date_part('minute', a2.s_dep::timestamp - a1.s_arv::timestamp)) >= 30 and

-- second leg

((date_part('day', a3.s_dep::timestamp - a2.s_arv::timestamp) * 24 +
date_part('hour', a3.s_dep::timestamp - a2.s_arv::timestamp)) * 60 +
date_part('minute', a3.s_dep::timestamp - a2.s_arv::timestamp)) >= 30 and

(a1.outbound_country='USA' and a4.inbound_country='Canada'
or a1.outbound_country='Canada' and a4.inbound_country='USA');

create view allNACities as
select distinct a1.city as outbound, a2.city as inbound
from airport a1, airport a2
where a1.country='Canada' and a2.country='USA' or
a1.country='USA' and a2.country='Canada';

create view directCount as
select a.outbound, a.inbound, count(distinct d.*) as direct
from directFlight a
join directFlight as d on
d.outbound=a.outbound and
d.inbound=a.inbound
group by a.outbound, a.inbound;

create view oneConCount as
select a.outbound, a.inbound, count(distinct o.*) as one_con
from oneCon a
join oneCon as o on
o.outbound=a.outbound and
o.inbound=a.inbound
group by a.outbound, a.inbound;

create view twoConCount as
select a.outbound, a.inbound, count(distinct t.*) as two_con
from twoCon a
join twoCon as t on
t.outbound=a.outbound and
t.inbound=a.inbound
group by a.outbound, a.inbound;

create view directArrival as
select outbound, inbound, min(arv) as arv
from directFlight
where arv is not NULL
group by outbound, inbound;

create view oneConArrival as
select outbound, inbound, min(arv) as arv
from oneCon
where arv is not NULL
group by outbound, inbound;

create view twoConArrival as
select outbound, inbound, min(arv) as arv
from twoCon
where arv is not NULL
group by outbound, inbound;

create view finalCount as
select a.outbound, a.inbound,
coalesce(t.direct, 0) as direct,
coalesce(t.one_con, 0) as one_con,
coalesce(t.two_con,0)
as two_con from
allNACities a natural left join
(select * from
directCount natural full join oneConCount natural full join twoConCount) as t;

create view allArrival as
select outbound, inbound, min(arv) as arv
from
(select t.* from ((select * from directArrival) union
(select * from oneConArrival) union
(select * from twoConArrival)) as t) as u
group by outbound, inbound;

create view answer as
select finalCount.*, allArrival.arv
from finalCount natural left join allArrival;

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q3 (select * from answer);
