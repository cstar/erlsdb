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

-behaviour(supervisor).
%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% External exports
%%--------------------------------------------------------------------
-export([
	 start_link/1
        ]).

%%--------------------------------------------------------------------
%% Internal exports
%%--------------------------------------------------------------------
-export([
	 init/1
        ]).

%%--------------------------------------------------------------------
%% Macros
%%--------------------------------------------------------------------
-define(SERVER, ?MODULE).

%%--------------------------------------------------------------------
%% Records
%%--------------------------------------------------------------------

%%====================================================================
%% External functions
%%====================================================================
%%--------------------------------------------------------------------
%% @doc Starts the supervisor.
%% @spec start_link(StartArgs) -> {ok, pid()} | Error
%% @end
%%--------------------------------------------------------------------
start_link(StartArgs) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, StartArgs).

%%====================================================================
%% Server functions
%%====================================================================
%%--------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok,  {SupFlags,  [ChildSpec]}} |
%%          ignore                          |
%%          {error, Reason}   
%%--------------------------------------------------------------------
init(StartArgs) ->
    RestartStrategy    = one_for_one,
    MaxRestarts        = 1000,
    MaxTimeBetRestarts = 3600,
    
    SupFlags = {RestartStrategy, MaxRestarts, MaxTimeBetRestarts},
    
    ChildSpecs =
	[
	 {erlsdb_server,
	  {erlsdb_server, start_link, StartArgs},
	  permanent,
	  1000,		% period for graceful shutdown
	  worker,
	  [erlsdb_server]}
	 ],
    {ok,{SupFlags, ChildSpecs}}.
%%====================================================================
%% Internal functions
%%====================================================================
