begin
declare timingsLength smallint;
declare cnt smallint;
declare flightId int(11);
declare stopId int;
declare pid int;
declare prc int(11) unsigned;

declare exit handler for sqlexception 
begin
   rollback;
end;

start transaction;
set prc = 0;

select id, count(*) into flightId, cnt from flights where name=flightName;

if cnt=0 then
   insert into flights(name,departureCity,arrivalCity,isAvailable)    
   values (flightName,departureCity,arrivalCity,1);
   select last_insert_id() into flightId;
else
   update flights set isAvailable=1 where name=flightName;
end if;

select length(times) into timingsLength;
if timingsLength <= 12 then
   select id into stopId from directFlightsDetails where fid=flightId and timings=times;
   if not exists (select id from directFlightsDetails where fid=flightId and timings=times) then
      insert into directFlightsDetails(timings,fid,isAvailable) values(times,flightId,1);
      select last_insert_id() into stopId;
   else 
        update directFlightsDetails set isAvailable=1 where fid=flightId and timings=times;
   end if;
     
   select id, price into pid, prc from directFlightsPrices where sid=stopId order by id desc limit 1;
   if not exists (select id from directFlightsPrices where sid=stopId) or priceOfFlight!=prc then
      insert into directFlightsPrices (price,sid) values(priceOfFlight,stopId);
   end if;

elseif timingsLength <= 25 then
   select id into stopId from oneStopFlightsDetails where fid=flightId and timings=times;
   if not exists (select id from oneStopFlightsDetails where fid=flightId and timings=times) then
      insert into oneStopFlightsDetails(timings,fid,isAvailable) values(times,flightId,1);
      select last_insert_id() into stopId;
   else 
        update oneStopFlightsDetails set isAvailable=1 where fid=flightId and timings=times;
   end if;
     
   select id, price into pid, prc from oneStopFlightsPrices where sid=stopId order by id desc limit 1;
   if not exists (select id from oneStopFlightsPrices where sid=stopId) or priceOfFlight!=prc then
      insert into oneStopFlightsPrices (price,sid) values(priceOfFlight,stopId);
   end if;

elseif timingsLength <= 36 then
   select id into stopId from twoStopFlightsDetails where fid=flightId and timings=times;
   if not exists (select id from twoStopFlightsDetails where fid=flightId and timings=times) then
      insert into twoStopFlightsDetails(timings,fid,isAvailable) values(times,flightId,1);
      select last_insert_id() into stopId;
   else 
        update twoStopFlightsDetails set isAvailable=1 where fid=flightId and timings=times;
   end if;
     
   select id, price into pid, prc from twoStopFlightsPrices where sid=stopId order by id desc limit 1;
   if not exists (select id from twoStopFlightsPrices where sid=stopId) or priceOfFlight!=prc then
      insert into twoStopFlightsPrices (price,sid) values(priceOfFlight,stopId);
   end if;
end if;
commit;
end
