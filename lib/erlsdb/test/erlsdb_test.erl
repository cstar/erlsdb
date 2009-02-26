%%%-------------------------------------------------------------------
%% @author Shahzad Bhatti <bhatti@plexobject.com> [http://bhatti.plexobject.com]
%% @doc Tests for this library
%%
%% == Contents ==
%%
%% {@section Introduction}<br/>
%% == Introduction ==
%%  Amazon's SimpleDB is a web service to persist string based key/value pairs.
%%  This library provides access to the web service using REST based interface.
%%  
%%  APIs:
%%  
%%
%% @copyright Shahzad Bhatti 2007
%%  
%% For license information see LICENSE.txt
%% 
%% @end
%%%-------------------------------------------------------------------
-module(erlsdb_test).
-author("Shahzad Bhatti <bhatti@plexobject.com> [http://bhatti.plexobject.com]").


%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------


%%%-------------------------------------------------------------------
%%% Test Methods
%%%-------------------------------------------------------------------
-export([test/0]).


test_domain() ->
   "TestDomain".



%%%-------------------------------------------------------------------
%%% Test Functions
%%%-------------------------------------------------------------------
test_list_domains() ->
    erlsdb:list_domains().

test_create_test_domain() ->
    erlsdb:create_domain(test_domain()),
    {ok, List, _} = erlsdb:list_domains(),
    Domain = test_domain(),
    [Domain] = lists:filter(
			fun(Elem) -> Elem == Domain end,
			List).


test_delete_test_domain() ->
    erlsdb:delete_domain(test_domain()),
    {ok, List, _} = erlsdb:list_domains(),
    Domain = test_domain(),
    [] = lists:filter(
			fun(Elem) -> Elem == Domain end,
			List).

test_replace_get_attributes() ->
    Attributes = lists:sort([
	{"StreetAddress", "705 5th Ave"},
        {"City", "Seattle"},
        {"State", "WA"},
        {"Zip", "98101"}
	]),
    erlsdb:replace_attributes(test_domain(),"TccAddress", Attributes),
    Response = erlsdb:get_attributes(test_domain(),"TccAddress"),
    case Response of
        {ok, UnsortedAttrs} ->
            Attributes = lists:sort(UnsortedAttrs);
	_ ->
            io:format("Unexpected response while getting attributes ~p~n", [Response])
     end.

test_replace_delete_attributes() ->
    Attributes = lists:sort([
	{"StreetAddress", "705 5th Ave"},
        {"City", "Seattle"},
        {"State", "WA"},
        {"Zip", "98101"}
	]),
    erlsdb:replace_attributes(test_domain(),"TccAddress", Attributes),
    erlsdb:delete_attributes(test_domain(),"TccAddress"),
    erlsdb_util:sleep(5000), %% let it sync
    Response = erlsdb:get_attributes(test_domain(),"TccAddress"),
    case Response of
        {ok, []} ->
            ok;
	_ ->
            io:format("Unexpected response while getting attributes after delete ~p~n", [Response])
     end.

test() ->
    io:format("Test Started server ~p~n", [erlsdb:start()]),
    io:format("Listing domains ~p~n",[test_list_domains()]),
    io:format("Creating test domain ~p~n",[test_create_test_domain()]),
    io:format("Adding attributes ~p~n",[test_replace_get_attributes()]),
    io:format("Removing attributes ~p~n",[test_replace_delete_attributes()]),
    io:format("Removing test domain ~p~n",[test_delete_test_domain()]),
    erlsdb_util:sleep(1500).

