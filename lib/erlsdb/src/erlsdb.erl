%%%-------------------------------------------------------------------
%% @author Shahzad Bhatti <bhatti@plexobject.com> [http://bhatti.plexobject.com]
%% @doc Business delegate to access SimpleDB
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
-module(erlsdb).
-author("Shahzad Bhatti <bhatti@plexobject.com> [http://bhatti.plexobject.com]").
-author("Eric Cestari <ecestari@mac.com> [http://www.cestari.info]").

-behaviour(application).
%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include("../include/erlsdb.hrl").

%%--------------------------------------------------------------------
%% External exports
%%--------------------------------------------------------------------
-export([
	 start/2,
	 shutdown/0,
	 stop/1
	 ]).

%%--------------------------------------------------------------------
%% External exports for APIs
%%--------------------------------------------------------------------
-export([
	create_domain/1, 
	list_domains/0, 
    list_domains/1, 
	list_domains/2, 
	delete_domain/1, 
	put_attributes/3, 
	put_attributes/4, 
	replace_attributes/3,
	get_attributes/2, 
	get_attributes/3, 
	delete_item/2, 
	delete_attributes/2, 
	delete_attributes/3,
	domain_metadata/1,
	q/4,
	qwa/5,
    select/2
	]).


%%--------------------------------------------------------------------
%% Macros
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Records
%%--------------------------------------------------------------------

%%====================================================================
%% External functions
%%====================================================================
%%--------------------------------------------------------------------
%% @doc The starting point for an erlang application.
%% @spec start(Type, StartArgs) -> {ok, Pid} | {ok, Pid, State} | {error, Reason}
%% @end
%%--------------------------------------------------------------------
start(_Type, _StartArgs) ->
    %case erlsdb_sup:start_link(StartArgs) of
    ID = param(access),
    Secret = param(secret),
    SSL = param(ssl, true),
    if SSL == true -> ssl:start();
        true -> ok
    end,
    N = param(workers, 5),
    {ok,SupPid} = erlsdb_sup:start_link([ID, Secret, SSL]),
    pg2:create(erlsdb_servers),
	lists:map(
	  fun(_) ->
		  {ok, Pid} =  supervisor:start_child(SupPid,[]),
		  pg2:join(erlsdb_servers, Pid)
	  end, lists:seq(1, N)),
	 {ok,SupPid}.

%%--------------------------------------------------------------------
%% @doc Called to shudown the erlsdb application.
%% @spec shutdown() -> ok 
%% @end
%%--------------------------------------------------------------------
shutdown() ->
    pg2:delete(erlsdb_servers),
    application:stop(erlsdb).


%%--------------------------------------------------------------------
%% @doc Creates a new domain that was specified to the start parameters. 
%% Types:
%% </pre>
%% @spec create_domain() -> ok
%% @end
%%--------------------------------------------------------------------
create_domain(Domain) ->
    Pid = pg2:get_closest_pid(erlsdb_servers),
    gen_server:call(Pid, {create_domain,Domain}).

%%--------------------------------------------------------------------
%% @doc List all domains for the account
%% Types:
%% </pre>
%% @spec list_domains() -> {ok, [Domains], MoreToken} | {error, {ErrorCode, ErrorMessage}
%% @end
%%--------------------------------------------------------------------
list_domains() ->
   list_domains(nil, nil).

%%--------------------------------------------------------------------
%% @doc List all domains for the account, will continue result from MoreTokens 
%% Types:
%%  MoreToken = string from last web request or nil
%% </pre>
%% @spec list_domains(MoreToken) -> {ok, [Domains], MoreToken} | {error, {ErrorCode, ErrorMessage}
%% @end
%%--------------------------------------------------------------------
list_domains(MoreToken) ->
    list_domains(MoreToken, nil). 

%%--------------------------------------------------------------------
%% @doc List all domains for the account
%% Types:
%%  MoreToken = string from last web request or nil
%%  MaxNumberOfDomains = integer - for maximum number of domains to return.
%% </pre>
%% @spec list_domains(MoreToken, MaxNumberOfDomains) -> {ok, [Domains], MoreToken} | {error, {ErrorCode, ErrorMessage}
%% @end
%%--------------------------------------------------------------------
list_domains(MoreToken, MaxNumberOfDomains) ->
     Pid = pg2:get_closest_pid(erlsdb_servers),
    gen_server:call(Pid, {list_domains,MoreToken, MaxNumberOfDomains}). 


%%--------------------------------------------------------------------
%% @doc Deletes a domain that was specified to the start function. 
%% Types:
%% </pre>
%% @spec delete_domain() -> ok
%% @end
%%--------------------------------------------------------------------
delete_domain(Domain) ->
    Pid = pg2:get_closest_pid(erlsdb_servers),
    gen_server:call(Pid, {delete_domain,Domain}).



%%--------------------------------------------------------------------
%% @doc Adds an item (tuple) to the domain
%% Types:
%%  ItemName = string
%%  Attributes = array of key/value [[key1, value1], [key2, value2]...]
%% </pre>
%% @spec put_attributes(ItemName, Attributes) -> ok
%% @end
%%--------------------------------------------------------------------
put_attributes(Domain,ItemName, Attributes) ->
    put_attributes(Domain,ItemName, Attributes, false). 


%%--------------------------------------------------------------------
%% @doc Adds an item (tuple) to the domain
%% Types:
%%  ItemName = string
%%  Attributes = array of key/value [{key1, value1}, {key2, value2}...]
%%  Replace = boolean - if true existing attributes will be replaced, 
%%		otherwise they will be appended.
%% </pre>
%% @spec put_attributes(ItemName, Attributes, Replace) -> ok
%% @end
%%--------------------------------------------------------------------
put_attributes(Domain, ItemName, Attributes, Replace) ->
    Pid = pg2:get_closest_pid(erlsdb_servers),
    gen_server:call(Pid, {put_attributes,Domain, ItemName, Attributes, Replace}).


%%--------------------------------------------------------------------
%% @doc Replace an existing item with specified attributes
%% Types:
%%  ItemName = string
%%  Attributes = array of key/value [{key1, value1}, {key2, value2}...]
%% </pre>
%% @spec replace_attributes(ItemName, Attributes) -> ok
%% @end
%%--------------------------------------------------------------------
replace_attributes(Domain,ItemName, Attributes) ->
    put_attributes(Domain, ItemName, Attributes, true). 


%%--------------------------------------------------------------------
%% @doc Retrieves an existing item with all attributes
%% Types:
%%  ItemName = string
%% </pre>
%% @spec get_attributes(ItemName) -> {ok, [{key1, value1}, {key2, value2}, ..]} | {error, {ErrorCode, ErrorMessage}
%% @end
%%--------------------------------------------------------------------
get_attributes(Domain,ItemName) ->
    get_attributes(Domain,ItemName, nil). 


%%--------------------------------------------------------------------
%% @doc Retrieves an existing item with matching attributes
%% Types:
%%  ItemName = string
%%  Attributes = array of keys [key1, key2, ...]
%% </pre>
%% @spec get_attributes(ItemName, Attributes) -> {ok, [{key1, value1}, {key2, value2}, ..]} | {error, {ErrorCode, ErrorMessage}
%% @end
%%--------------------------------------------------------------------
get_attributes(Domain,ItemName, AttributeNames) ->    
    Pid = pg2:get_closest_pid(erlsdb_servers),
    gen_server:call(Pid, {get_attributes,Domain,ItemName,AttributeNames}).


%%--------------------------------------------------------------------
%% @doc Deletes an existing item 
%% Types:
%%  ItemName = string
%%  Attributes = array of keys [key1, key2, ...]
%% </pre>
%% @spec delete_item(ItemName) -> ok
%% @end
%%--------------------------------------------------------------------
delete_item(Domain,ItemName) ->
    Pid = pg2:get_closest_pid(erlsdb_servers),
    gen_server:call(Pid, {delete_item, Domain,ItemName}).


%%--------------------------------------------------------------------
%% @doc Deletes all attributes for given item in domain
%% Types:
%%  ItemName = string
%% </pre>
%% @spec delete_attributes(ItemName) -> ok
%% @end
%%--------------------------------------------------------------------
delete_attributes(Domain,ItemName) ->
    delete_attributes(Domain,ItemName, nil).

%%--------------------------------------------------------------------
%% @doc Deletes all matching attributes for given item in domain
%% Types:
%%  ItemName = string
%%  Attributes = array of keys [key1, key2, ...]
%% </pre>
%% @spec delete_attributes(ItemName, AttributeNames) -> ok
%% @end
%%--------------------------------------------------------------------
delete_attributes(Domain,ItemName, AttributeNames) ->
    Pid = pg2:get_closest_pid(erlsdb_servers),
    gen_server:call(Pid, {delete_attributes,Domain,ItemName,AttributeNames}).

q(Domain, Query, MaxNumber, NextToken)->
    Pid = pg2:get_closest_pid(erlsdb_servers),
    gen_server:call(Pid, {q,Domain, Query, MaxNumber, NextToken}).
qwa(Domain, Query, AttributeNames, MaxNumber, NextToken)->
    Pid = pg2:get_closest_pid(erlsdb_servers),
    gen_server:call(Pid, {qwa,Domain,Query, AttributeNames,  MaxNumber, NextToken}).

select(Query, NextToken)->
    Pid = pg2:get_closest_pid(erlsdb_servers),
    gen_server:call(Pid, {select, Query, NextToken}).

domain_metadata(Domain)->
    Pid = pg2:get_closest_pid(erlsdb_servers),
    gen_server:call(Pid, {domain_metadata,Domain}).
%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% Called upon the termintion of an application.
%%--------------------------------------------------------------------
stop(_State) ->
    ok.


param(Name, Default)->
	case application:get_env(?MODULE, Name) of
		{ok, Value} -> Value;
		_-> Default
	end.

param(Name) -> param(Name, nil).