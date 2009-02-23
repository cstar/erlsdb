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

-behaviour(application).
%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include("erlsdb.hrl").

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
	create_domain/0, 
	list_domains/0, 
	list_domains/1, 
	list_domains/2, 
	delete_domain/0, 
	put_attributes/2, 
	put_attributes/3, 
	replace_attributes/2,
	get_attributes/1, 
	get_attributes/2, 
	delete_item/1, 
	delete_attributes/1, 
	delete_attributes/2
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
start(_Type, StartArgs) ->
    %case erlsdb_sup:start_link(StartArgs) of
    case erlsdb_server:start_link(StartArgs) of
	{ok, Pid} -> 
	    {ok, Pid};
	Error ->
	    Error
    end.

%%--------------------------------------------------------------------
%% @doc Called to shudown the erlsdb application.
%% @spec shutdown() -> ok 
%% @end
%%--------------------------------------------------------------------
shutdown() ->
    application:stop(erlsdb).


%%--------------------------------------------------------------------
%% @doc Creates a new domain that was specified to the start parameters. 
%% Types:
%% </pre>
%% @spec create_domain() -> ok
%% @end
%%--------------------------------------------------------------------
create_domain() ->
    erlsdb_server:create_domain().

%%--------------------------------------------------------------------
%% @doc List all domains for the account
%% Types:
%% </pre>
%% @spec list_domains() -> {ok, [Domains], MoreToken} | {error, {ErrorCode, ErrorMessage}
%% @end
%%--------------------------------------------------------------------
list_domains() ->
    erlsdb_server:list_domains(). 

%%--------------------------------------------------------------------
%% @doc List all domains for the account, will continue result from MoreTokens 
%% Types:
%%  MoreToken = string from last web request or nil
%% </pre>
%% @spec list_domains(MoreToken) -> {ok, [Domains], MoreToken} | {error, {ErrorCode, ErrorMessage}
%% @end
%%--------------------------------------------------------------------
list_domains(MoreToken) ->
    erlsdb_server:list_domains(MoreToken). 

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
    erlsdb_server:list_domains(MoreToken, MaxNumberOfDomains). 


%%--------------------------------------------------------------------
%% @doc Deletes a domain that was specified to the start function. 
%% Types:
%% </pre>
%% @spec delete_domain() -> ok
%% @end
%%--------------------------------------------------------------------
delete_domain() ->
    erlsdb_server:delete_domain(). 



%%--------------------------------------------------------------------
%% @doc Adds an item (tuple) to the domain
%% Types:
%%  ItemName = string
%%  Attributes = array of key/value [[key1, value1], [key2, value2]...]
%% </pre>
%% @spec put_attributes(ItemName, Attributes) -> ok
%% @end
%%--------------------------------------------------------------------
put_attributes(ItemName, Attributes) ->
    erlsdb_server:put_attributes(ItemName, Attributes). 


%%--------------------------------------------------------------------
%% @doc Adds an item (tuple) to the domain
%% Types:
%%  ItemName = string
%%  Attributes = array of key/value [[key1, value1], [key2, value2]...]
%%  Replace = boolean - if true existing attributes will be replaced, 
%%		otherwise they will be appended.
%% </pre>
%% @spec put_attributes(ItemName, Attributes, Replace) -> ok
%% @end
%%--------------------------------------------------------------------
put_attributes(ItemName, Attributes, Replace) ->
    erlsdb_server:put_attributes(ItemName, Attributes, Replace). 


%%--------------------------------------------------------------------
%% @doc Replace an existing item with specified attributes
%% Types:
%%  ItemName = string
%%  Attributes = array of key/value [[key1, value1], [key2, value2]...]
%% </pre>
%% @spec replace_attributes(ItemName, Attributes) -> ok
%% @end
%%--------------------------------------------------------------------
replace_attributes(ItemName, Attributes) ->
    erlsdb_server:replace_attributes(ItemName, Attributes). 


%%--------------------------------------------------------------------
%% @doc Retrieves an existing item with all attributes
%% Types:
%%  ItemName = string
%% </pre>
%% @spec get_attributes(ItemName) -> {ok, [[key1, value1], [key2, value2], ..]} | {error, {ErrorCode, ErrorMessage}
%% @end
%%--------------------------------------------------------------------
get_attributes(ItemName) ->
    erlsdb_server:get_attributes(ItemName). 


%%--------------------------------------------------------------------
%% @doc Retrieves an existing item with matching attributes
%% Types:
%%  ItemName = string
%%  Attributes = array of keys [key1, key2, ...]
%% </pre>
%% @spec get_attributes(ItemName, Attributes) -> {ok, [[key1, value1], [key2, value2], ..]} | {error, {ErrorCode, ErrorMessage}
%% @end
%%--------------------------------------------------------------------
get_attributes(ItemName, AttributeNames) ->
    erlsdb_server:get_attributes(ItemName, AttributeNames). 


%%--------------------------------------------------------------------
%% @doc Deletes an existing item 
%% Types:
%%  ItemName = string
%%  Attributes = array of keys [key1, key2, ...]
%% </pre>
%% @spec delete_item(ItemName) -> ok
%% @end
%%--------------------------------------------------------------------
delete_item(ItemName) ->
    erlsdb_server:delete_item(ItemName).


%%--------------------------------------------------------------------
%% @doc Deletes all attributes for given item in domain
%% Types:
%%  ItemName = string
%% </pre>
%% @spec delete_attributes(ItemName) -> ok
%% @end
%%--------------------------------------------------------------------
delete_attributes(ItemName) ->
    erlsdb_server:delete_attributes(ItemName).

%%--------------------------------------------------------------------
%% @doc Deletes all matching attributes for given item in domain
%% Types:
%%  ItemName = string
%%  Attributes = array of keys [key1, key2, ...]
%% </pre>
%% @spec delete_attributes(ItemName, AttributeNames) -> ok
%% @end
%%--------------------------------------------------------------------
delete_attributes(ItemName, AttributeNames) ->
    erlsdb_server:delete_attributes(ItemName, AttributeNames). 


%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% Called upon the termintion of an application.
%%--------------------------------------------------------------------
stop(_State) ->
    ok.
