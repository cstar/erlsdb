-module(sdb_test).
-export([test/1, select/1, serial/0, parallel/0]).
-define(DOMAIN,"roster").
test(Type)->
    erlsdb:start(),
    statistics(runtime),
    statistics(wall_clock),
    
    ?MODULE:Type(),
    
    {_, RTime} = statistics(runtime),
    {_, WTime} = statistics(wall_clock),
    io:format("Elapsed ~p (~p) ms ", [WTime, RTime]).

serial()->
   lists:foreach(fun(_N)->
        erlsdb:s("select * from roster")
    end, lists:seq(1,20)). 
   
parallel()->
    lists:foreach(fun(_N)->
        spawn(?MODULE, select,[ self()])
    end, lists:seq(1,20)),
    
    lists:foldl(fun(_N, Acc)->
        receive
            {_From, R} -> [R|Acc]
        end
    end,[],  lists:seq(1,20)).
    
select(From)->
    {ok, R, _} = erlsdb:s("select * from roster"),
    From ! {self(),R }.
    
   