-module(cachedsdb).

-export([start/1, get_attributes/2, get_attributes/3, put_attributes/3, replace_attributes/3]).
% cachedsdb:start("../../erlang/merle/ketama.servers").
% cachedsdb:get_attributes("users", "mulder").
% cachedsdb:put_attributes("fubar","mars",[{"io", "sympa"}, {"voila", "c'est tout"}]).
start(KetamaFile)->
    application:set_env(merle, file,KetamaFile),
    erlsdb:start(),
    merle:start().

get_attributes(Domain,ItemName) ->
    get_attributes(Domain,ItemName, []). 
        
get_attributes(Domain,ItemName, AttributeNames) -> 
    Key = key(Domain, ItemName),
    case merle:getkey(Key) of
        undefined ->
            {ok, Attrs} = erlsdb:get_attributes(Domain, ItemName),
            Json = to_json(Attrs),
            merle:set(Key, Json),
            {ok, Attrs};
        Val when AttributeNames == [] ->
            {ok, from_json(Val)};
        Val ->
            Response = lists:filter(
                fun({K, _V})->
                    lists:member(K, AttributeNames)
                end, from_json(Val)),
            {ok, Response}
    end.
    
put_attributes(Domain,ItemName, Attributes)->
    Ret = erlsdb:put_attributes(Domain,ItemName, Attributes),
    Key = key(Domain, ItemName),
    merle:delete(Key),
    Ret.
    
replace_attributes(Domain,ItemName, Attributes)->
    Ret = erlsdb:put_attributes(Domain,ItemName, Attributes),
    Key = key(Domain, ItemName),
    merle:delete(Key),
    Ret.

to_json(Attrs)->
    NAttrs = lists:map(fun({K, V})->
        {K, list_to_binary(V)}
    end, Attrs),
    rfc4627:encode({obj, NAttrs}).
    
from_json(Str)->
    {ok, {obj, Attrs}, _} = rfc4627:decode(Str),
    NAttrs = lists:map(fun({K, V})->
        {K, binary_to_list(V)}
    end, Attrs),
    NAttrs.
    
    
key(Domain, ItemName)->
    "sdb/"++Domain ++ "/" ++ ItemName.