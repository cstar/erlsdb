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
	batch_put_attributes/2, 
	replace_attributes/3,
	replace_attributes/4,
	get_attributes/2, 
	get_attributes/3, 
	delete_item/2, 
	delete_item/3, 
	delete_attributes/2, 
	delete_attributes/3,
	delete_attributes/4,
	domain_metadata/1,
	q/2,
	q/4,
	qwa/2,
	qwa/5,
	s/1,
  s/2,
  s_all/1,
  cs/1,
  cs/2,
  cs_all/1
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
    application:start(crypto),
    application:start(ibrowse),
    application:start(xmerl),
    application:start(erlsdb).
    

start(_Type, _StartArgs) ->
    ID = get(access, "AMAZON_ACCESS_KEY_ID"),
    Secret = get(secret, "AMAZON_SECRET_ACCESS_KEY"),
    N = param(workers, 2),
    SSL = param(ssl, false),
    Timeout = param(timeout, nil),
    random:seed(),
    Port = if SSL == true -> 
            ssl:start(),
            443;
        true -> 80
    end,
    ibrowse:set_dest("sdb.amazonaws.com", Port, [{max_sessions, 100},{max_pipeline_size, 20}]),
    if ID == error orelse Secret == error ->
            {error, "AWS credentials not set. Pass as application parameters or as env variables."};
        true ->
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
    call( {create_domain,Domain}).

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
    call( {list_domains,MoreToken, MaxNumberOfDomains}). 


%%--------------------------------------------------------------------
%% @doc Deletes a domain that was specified to the start function. 
%% Types:
%% </pre>
%% @spec delete_domain() -> ok
%% @end
%%--------------------------------------------------------------------
delete_domain(Domain) ->
    call({delete_domain,Domain}).



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
    put_attributes(Domain,ItemName, Attributes, false, []). 


%%--------------------------------------------------------------------
%% @doc Adds an item (tuple) to the domain
%% Types:
%%  ItemName = string
%%  Attributes = array of key/value [{key1, value1}, {key2, value2}...]
%%  Replace = boolean - if true existing attributes will be replaced, 
%%		otherwise they will be appended.
%% </pre>
%% @spec put_attributes(Domain, ItemName, Attributes, Replace) -> ok
%% @end
%%--------------------------------------------------------------------
put_attributes(Domain, ItemName, Attributes, Replace) when Replace =:= true orelse Replace =:= false -> 
    call({put_attributes,Domain, ItemName, Attributes, Replace, []});

put_attributes(Domain, ItemName, Attributes, Conditional) -> 
    call({put_attributes,Domain, ItemName, Attributes, false, Conditional}).
    
put_attributes(Domain, ItemName, Attributes, Replace, Conditional) ->
    call({put_attributes,Domain, ItemName, Attributes, Replace, Conditional}).

%%--------------------------------------------------------------------
%% @doc adds multiple attributes on several key in one go.
%% 
%% <pre>Types:
%%  Items = [{ItemName,Attributes,Replace}|Tail] list of items
%%  Attributes = array of key/value [{key1, value1}, {key2, value2}...]
%% erlsdb:batch_put_attributes("fubar", 
%%          [{"toto", [{"oiu", "12"}], false}, 
%%           {"lkji", [{"oiul", "eric"}], false}]).
%% </pre>
%% @spec batch_put_attributes(Domain, Items) -> ok
%% @end
%%--------------------------------------------------------------------
%%
batch_put_attributes(Domain,Items) ->
   call( {batch_put_attributes,Domain,Items}).
    
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
    put_attributes(Domain, ItemName, Attributes, true, []). 

replace_attributes(Domain,ItemName, Attributes, Conditional) ->
    put_attributes(Domain, ItemName, Attributes, true, Conditional). 

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
    call({get_attributes,Domain,ItemName,AttributeNames}).


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
  call({delete_attributes, Domain,ItemName, [], []}).

delete_item(Domain, ItemName, ConditionalValues) ->
  call({delete_attributes, Domain,ItemName, [], ConditionalValues}).
  
%%--------------------------------------------------------------------
%% @doc Deletes all attributes for given item in domain
%% Types:
%%  ItemName = string
%% </pre>
%% @spec delete_attributes(ItemName) -> ok
%% @end
%%--------------------------------------------------------------------
delete_attributes(Domain,ItemName) ->
    delete_attributes(Domain,ItemName, nil, []).

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
    call( {delete_attributes,Domain,ItemName,AttributeNames, []}).

delete_attributes(Domain,ItemName, AttributeNames, ConditionalValues) ->
    call( {delete_attributes,Domain,ItemName,AttributeNames, ConditionalValues}).

q(Domain, Query, MaxNumber, NextToken)->
    call({q,Domain, Query, MaxNumber, NextToken}).
q(Domain, NextToken)->
    q(Domain, nil, nil, NextToken).
    
qwa(Domain, NextToken)->
    qwa(Domain,nil,nil,nil, NextToken).
    
qwa(Domain, Query, AttributeNames, MaxNumber, NextToken)->
     call({qwa,Domain,Query, AttributeNames,  MaxNumber, NextToken}).

% Eventually consistent
s_all(Query)->
    case s(Query, nil) of
        {ok, R, nil} -> {ok, R};
        {ok, R, N } -> s_all1(Query, N, R, false)
    end.
    
% Consistent read    
cs_all(Query)->
  case cs(Query, nil) of
        {ok, R, nil} -> {ok, R};
        {ok, R, N } -> s_all1(Query, N, R, true)
    end.
    
s_all1(_Query, nil, R, _Consistent) -> R;
s_all1(Query, N, R, Consistent) ->
    case call({select, Query, N, Consistent}) of
        {ok, R1, N1} ->
            s_all1(Query,N1, R ++ R1, Consistent);
        Error -> Error
    end.
            
%% Consistent read select    
cs(Query) ->
   cs(Query, nil).
cs(Query, NextToken)->
  call({select, Query, NextToken, true}).

%% Eventually consistent select
s(Query)->
    s(Query, nil).

s(Query, NextToken)->
    call({select, Query, NextToken, false}).

domain_metadata(Domain)->
    call({domain_metadata,Domain}).
%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% Called upon the termintion of an application.
%%--------------------------------------------------------------------
stop(_State) ->
    ok.

call(M)->
    call(M, 0).

call(M, Retries)->
    Pid = erlsdb_sup:get_random_pid(),
    case gen_server:call(Pid, M, infinity) of
      retry -> 
          Sleep = random:uniform(trunc(math:pow(4, Retries)*10)),
          timer:sleep(Sleep),
          call(M, Retries + 1);
      R -> R
  end.

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
