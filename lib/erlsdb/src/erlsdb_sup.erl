%%%-------------------------------------------------------------------
%% @author Shahzad Bhatti <bhatti@plexobject.com> [http://bhatti.plexobject.com]
%% @doc supervisor for gen_server to access SimpleDB
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
-module(erlsdb_sup).
-author("Shahzad Bhatti <bhatti@plexobject.com> [http://bhatti.plexobject.com]").
-author("Eric Cestari <ecestari@mac.com> [http://www.cestari.info]").

-behaviour(supervisor).
%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-export([start_link/1]).
-export([init/1]).

start_link(StartArgs) ->
    supervisor:start_link(?MODULE, StartArgs).

init(StartArgs) ->
    {ok, {{simple_one_for_one, 10, 1},
          [{undefined, {erlsdb_server, start_link, StartArgs},
            temporary, brutal_kill, worker, [erlsdb_server]}]}}.

