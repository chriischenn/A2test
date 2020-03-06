-- Q2. Refunds!

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel, public;
DROP TABLE IF EXISTS q2 CASCADE;

CREATE TABLE q2 (
    airline CHAR(2),
    name VARCHAR(50),
    year CHAR(4),
    seat_class seat_class,
    refund REAL
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS passBooking CASCADE;
DROP VIEW IF EXISTS flightTime CASCADE;
DROP VIEW IF EXISTS delays CASCADE;
DROP VIEW IF EXISTS refunds CASCADE;
DROP VIEW IF EXISTS q2_result CASCADE;


-- Define views for your intermediate steps here:
create view passBooking as
select booking.flight_id, flight.airline, booking.seat_class, booking.pass_id,
booking.price, flight.s_dep, flight.s_arv,
(inboundAirport.country=outboundAirport.country) as isDomestic
from booking, flight
JOIN airport AS inboundAirport ON flight.inbound=inboundAirport.code
JOIN airport AS outboundAirport ON flight.outbound=outboundAirport.code
where booking.flight_id=flight.id
order by booking.flight_id DESC;

create view flightTime as
select passBooking.flight_id, passBooking.airline, airline.name,
seat_class, pass_id, price, isDomestic,
(DATE_PART('year', departure.datetime)) as year,
s_dep, s_arv, departure.datetime
as act_dep, arrival.datetime as act_arv
from passBooking, arrival, departure, airline
where passBooking.flight_id=arrival.flight_id
and passBooking.flight_id=departure.flight_id
and passBooking.airline=airline.code;

create view delays as
select flight_id, isDomestic, year, flightTime.airline, flightTime.name, seat_class, pass_id, price,
(DATE_PART('day', act_dep::timestamp - s_dep::timestamp) * 24 +
DATE_PART('hour', act_dep::timestamp - s_dep::timestamp)) as dep_delay,
(DATE_PART('day', act_arv::timestamp - s_arv::timestamp) * 24 +
DATE_PART('hour', act_arv::timestamp - s_arv::timestamp)) as arv_delay
from flightTime;

create view refunds as
select delays.*,
case when (arv_delay <= dep_delay / 2) then 0.0::numeric::real
     when (isDomestic and dep_delay >= 4 and dep_delay < 10) then (price * 0.35)::numeric::real
     when (isDomestic and dep_delay >= 10) then (price * 0.5)::numeric::real
     when (not isDomestic and dep_delay >= 7 and dep_delay < 12) then (price * 0.35)::numeric::real
     when (not isDomestic and dep_delay >= 12) then (price * 0.5)::numeric::real
     else 0.0::numeric::real end
     as refund
from delays;

create view q2_result as
select airline, name, year, seat_class, sum(refund)
from refunds
group by airline, name, year, seat_class;

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q2 (select * from q2_result)
