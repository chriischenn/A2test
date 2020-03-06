-- Q1. Airlines

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO air_travel, public;
DROP TABLE IF EXISTS q1 CASCADE;

CREATE TABLE q1 (
    pass_id INT,
    name VARCHAR(100),
    airlines INT
);

-- Do this for each of the views that define your intermediate steps.
-- (But give them better names!) The IF EXISTS avoids generating an error
-- the first time this file is imported.
DROP VIEW IF EXISTS allPass CASCADE;
DROP VIEW IF EXISTS allPassWithFlight CASCADE;
DROP VIEW IF EXISTS q1_answer CASCADE;


-- Define views for your intermediate steps here:
CREATE VIEW allPass AS SELECT passenger.id AS pass_id, (passenger.firstname || ' ' || passenger.surname) AS NAME, booking.flight_id
FROM passenger left outer join booking
ON passenger.id=booking.pass_id;

CREATE VIEW allPassWithFlight AS SELECT pass_id, name, airline
FROM allPass left outer join flight
on allPass.flight_id=flight.id;

CREATE VIEW q1_answer AS SELECT pass_id, name, count(airline) AS airlines
FROM allPassWithFlight
GROUP BY pass_id, NAME;

-- Your query that answers the question goes below the "insert into" line:
INSERT INTO q1 (select * from q1_answer);
