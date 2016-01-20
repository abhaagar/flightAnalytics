begin

declare exit handler for sqlexception 
begin
   rollback;
   GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE, 
       @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;
   SET @errorLog = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);
   insert into errorLogs(error) values(@errorLog);
end;


start transaction;

delete from temp1;
delete from temp2;
delete from temp3;
delete from counter;
insert into temp1 select id from directFlights where name like concat('%',archivalDate);
insert into temp2 select A.id from directFlightsDetails as A,temp1 as B where A.fid=B.id;
insert into temp3 select A.id from directFlightsPrices as A,temp2 as B where A.sid=B.id;

insert into archivedDirectFlights select * from directFlights where id in (select * from temp1);
insert into archivedDirectFlightsDetails select * from directFlightsDetails where id in (select * from temp2);
insert into archivedDirectFlightsPrices select * from directFlightsPrices where id in (select * from temp3);

delete from directFlightsPrices where id in (select * from temp3);
delete from directFlightsDetails where id in (select * from temp2);
delete from directFlights where id in (select * from temp1);

delete from temp1;
delete from temp2;
delete from temp3;

insert into temp1 select id from oneStopFlights where name like concat('%',archivalDate);
insert into temp2 select A.id from oneStopFlightsDetails as A,temp1 as B where A.fid=B.id;
insert into temp3 select A.id from oneStopFlightsPrices as A,temp2 as B where A.sid=B.id;

insert into archivedOneStopFlights select * from oneStopFlights where id in (select * from temp1);
insert into archivedOneStopFlightsDetails select * from oneStopFlightsDetails where id in (select * from temp2);
insert into archivedOneStopFlightsPrices select * from oneStopFlightsPrices where id in (select * from temp3);

delete from oneStopFlightsPrices where id in (select * from temp3);
delete from oneStopFlightsDetails where id in (select * from temp2);
delete from oneStopFlights where id in (select * from temp1);

delete from temp1;
delete from temp2;
delete from temp3;

insert into temp1 select id from twoStopFlights where name like concat('%',archivalDate);
insert into temp2 select A.id from twoStopFlightsDetails as A,temp1 as B where A.fid=B.id;
insert into temp3 select A.id from twoStopFlightsPrices as A,temp2 as B where A.sid=B.id;

insert into archivedTwoStopFlights select * from twoStopFlights where id in (select * from temp1);
insert into archivedTwoStopFlightsDetails select * from twoStopFlightsDetails where id in (select * from temp2);
insert into archivedTwoStopFlightsPrices select * from twoStopFlightsPrices where id in (select * from temp3);

delete from twoStopFlightsPrices where id in (select * from temp3);
delete from twoStopFlightsDetails where id in (select * from temp2);
delete from twoStopFlights where id in (select * from temp1);

delete from temp1;
delete from temp2;
delete from temp3;
commit;
end
