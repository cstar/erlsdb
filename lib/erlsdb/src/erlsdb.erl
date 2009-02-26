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

%%--------------------------------------------------------------------
%% External exports
%%--------------------------------------------------------------------
-export([
     start/0,
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
	q/2,
	q/4,
	qwa/2,
	qwa/5,
	s/1,
    s/2
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

start()->
    crypto:start(),
    application:start(xmerl),
    inets:start(),
    application:start(erlsdb).
    

start(_Type, _StartArgs) ->
    ID = get(access, "AMAZON_ACCESS_KEY_ID"),
    Secret = get(secret, "AMAZON_SECRET_ACCESS_KEY"),
    SSL = param(ssl, true),
    Timeout = param(timeout, nil),
    if SSL == true -> ssl:start();
        true -> ok
    end,
    if ID == error orelse Secret == error ->
            {error, "AWS credentials not set. Pass as application parameters or as env variables."};
        true ->
            N = param(workers, 5),
            erlsdb_sup:start_link([ID, Secret, SSL, Timeout], N)
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
create_domain(Domain) ->
     Pid = erlsdb_sup:get_random_pid(),
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
    Pid = erlsdb_sup:get_random_pid(),
    gen_server:call(Pid, {list_domains,MoreToken, MaxNumberOfDomains}). 


%%--------------------------------------------------------------------
%% @doc Deletes a domain that was specified to the start function. 
%% Types:
%% </pre>
%% @spec delete_domain() -> ok
%% @end
%%--------------------------------------------------------------------
delete_domain(Domain) ->
    Pid = erlsdb_sup:get_random_pid(),
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
    Pid = erlsdb_sup:get_random_pid(),
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
    get_attributes(Domain,ItemName, []). 


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
    Pid = erlsdb_sup:get_random_pid(),
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
    Pid = erlsdb_sup:get_random_pid(),
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
    Pid = erlsdb_sup:get_random_pid(),
    gen_server:call(Pid, {delete_attributes,Domain,ItemName,AttributeNames}).

q(Domain, Query, MaxNumber, NextToken)->
     Pid = erlsdb_sup:get_random_pid(),
    gen_server:call(Pid, {q,Domain, Query, MaxNumber, NextToken}).
q(Domain, NextToken)->
    q(Domain, nil, nil, NextToken).
    
qwa(Domain, NextToken)->
    qwa(Domain,nil,nil,nil, NextToken).
    
qwa(Domain, Query, AttributeNames, MaxNumber, NextToken)->
     Pid = erlsdb_sup:get_random_pid(),
    gen_server:call(Pid, {qwa,Domain,Query, AttributeNames,  MaxNumber, NextToken}).

s(Query)->
    s(Query, nil).

s(Query, NextToken)->
     Pid = erlsdb_sup:get_random_pid(),
    gen_server:call(Pid, {select, Query, NextToken}).

domain_metadata(Domain)->
     Pid = erlsdb_sup:get_random_pid(),
    gen_server:call(Pid, {domain_metadata,Domain}).
%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% Called upon the termintion of an application.
%%--------------------------------------------------------------------
stop(_State) ->
    ok.

get(Atom, Env)->
    case application:get_env(Atom) of
     {ok, Value} ->
         Value;
     undefined ->
         case os:getenv(Env) of
     	false ->
     	    error;
     	Value ->
     	    Value
         end
    end.
    
param(Name, Default)->
	case application:get_env(?MODULE, Name) of
		{ok, Value} -> Value;
		_-> Default
	end.
