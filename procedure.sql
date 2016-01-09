begin
      declare flightId int(11);
      declare pid int;
      declare cnt int;
      declare arrive char(5);
      declare departure char(5);
      declare stopId int;
      declare stopId1 int;
      declare stopId2 int;
      declare prc int(11) unsigned;
      declare pass int;
      
      declare exit handler for sqlexception 
      begin
         rollback;
      end;
      start transaction;
      set prc = 0;
                
      update flights set isAvailable=1 where name=flightName;
      select id, count(*) into flightId, cnt from flights where name=flightName;
      if cnt=0 then
         insert into flights(name,departureCity,arrivalCity,isAvailable) values (flightName,departureCity,arrivalCity,1);
         select last_insert_id() into flightId;
      end if;

      select substr(timings,1,5) into departure;
      select substr(timings,7) into timings;
      select substr(timings,1,5) into arrive;
      select substr(timings,7) into timings;

      select id into stopId from stop where departure=depart and arrival=arrive and fid=flightId;
      if not exists (select id from stop where departure=depart and arrival=arrive and fid=flightId) then
        insert into stop(depart,arrival,fid) values(departure,arrive,flightId);
        select last_insert_id() into stopId;
      end if;
     
      select id, price into pid, prc from price where sid=stopId limit 1;
      if not exists (select id from price where sid=stopId) or priceOfFlight!=prc then
        insert into price (price,sid) values(priceOfFlight,stopId);
      end if;
      
      if timings!=' ' then
         select substr(timings,1,5) into departure;
         select substr(timings,7) into timings;
         select substr(timings,1,5) into arrive;
         select substr(timings,7) into timings;

         select id into stopId1 from stop1 where depart=departure and arrival=arrive and sid=stopId;
         if not exists (select id from stop1 where depart=departure and arrival=arrive and sid=stopId) then
           insert into stop1(depart,arrival,sid) values(departure,arrive,stopId);
           select last_insert_id() into stopId1;
         end if;

         if timings!=' ' then
            select substr(timings,1,5) into departure;
            select substr(timings,7) into timings;
            select substr(timings,1,5) into arrive;
            select substr(timings,7) into timings;
            select id into stopId2 from stop2 where depart=departure and arrival=arrive and sid=stopId1;
            if not exists (select id from stop2 where depart=departure and arrival=arrive and sid=stopId1) then
               insert into stop2(depart,arrival,sid) values(departure,arrive,stopId1);
            end if;
         end if;
      end if;
      commit;
end
