%%%-------------------------------------------------------------------
%% @author Shahzad Bhatti <bhatti@plexobject.com> [http://bhatti.plexobject.com]
%% @doc gen_server to access SimpleDB
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
-module(erlsdb_server).
-author("Shahzad Bhatti <bhatti@plexobject.com> [http://bhatti.plexobject.com]").

-include_lib("xmerl/include/xmerl.hrl").
-behaviour(gen_server).
%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include("../include/erlsdb.hrl").

%%--------------------------------------------------------------------
%% External exports
%%--------------------------------------------------------------------
-export([
	start_link/1,
	stop/0,
	create_domain/1, 
	list_domains/1, 
	list_domains/0, 
	list_domains/2, 
	delete_domain/1, 
	put_attributes/3, 
	put_attributes/4, 
	replace_attributes/3,
	get_attributes/2, 
	get_attributes/3, 
	delete_item/2, 
	delete_attributes/2
	]).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
-export([
	init/1, 
	handle_call/3, 
	handle_cast/2, 
	handle_info/2, 
	terminate/2, 
	code_change/3
	]).

%%--------------------------------------------------------------------
%% record definitions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% macro definitions
%%--------------------------------------------------------------------
-define(SERVER, ?MODULE).
-define(TIMEOUT, 10000).



%%====================================================================
%% External functions
%%====================================================================
%%--------------------------------------------------------------------
%% @doc Starts the server.
%% @spec start_link() -> {ok, pid()} | {error, Reason}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
    %%?DEBUG("******* erlsdb_server:start_link/1 starting~n", [InitialState]),
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

%%--------------------------------------------------------------------
%% @doc Stops the server.
%% @spec stop() -> ok
%% @end
%%--------------------------------------------------------------------
stop() ->
    gen_server:cast(?SERVER, stop),
    init:stop().


%%--------------------------------------------------------------------
%% @doc Creates a new domain that was passed during initialization
%%	and is stored in state.
%% Types:
%% <pre>
%% </pre>
%% @spec create_domain() -> ok
%% @end
%%--------------------------------------------------------------------
create_domain(Domain) ->
    gen_server:cast(?SERVER, {create_domain,Domain}).

%%--------------------------------------------------------------------
%% @doc List all domains for the account
%% <pre>
%% Types:
%% </pre>
%% @spec list_domains() -> {ok, [Domains], MoreToken} | {error, {ErrorCode, ErrorMessage}
%% @end
%%--------------------------------------------------------------------
list_domains() ->
    list_domains(nil, nil).

%%--------------------------------------------------------------------
%% @doc List all domains for the account, will continue result from MoreTokens 
%% <pre>
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
%% <pre>
%% Types:
%%  MoreToken = string from last web request or nil
%%  MaxNumberOfDomains = integer - for maximum number of domains to return.
%% </pre>
%% @spec list_domains(MoreToken, MaxNumberOfDomains) -> {ok, [Domains], MoreToken} | {error, {ErrorCode, ErrorMessage}
%% @end
%%--------------------------------------------------------------------
list_domains(MoreToken, MaxNumberOfDomains) ->
    gen_server:call(?SERVER, {list_domains, MoreToken, MaxNumberOfDomains}, ?TIMEOUT).


%%--------------------------------------------------------------------
%% @doc Deletes a domain  that was passed during initialization
%%	and is stored in state.
%% <pre>
%% Types:
%% </pre>
%% @spec delete_domain() -> ok
%% @end
%%--------------------------------------------------------------------
delete_domain(Domain) ->
    gen_server:cast(?SERVER, {delete_domain,Domain}).



%%--------------------------------------------------------------------
%% @doc Adds an item (tuple) to the domain
%% <pre>
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
%% <pre>
%% Types:
%%  ItemName = string
%%  Attributes = array of key/value [[key1, value1], [key2, value2]...]
%%  Replace = boolean - if true existing attributes will be replaced, 
%%		otherwise they will be appended.
%% </pre>
%% @spec put_attributes(ItemName, Attributes, Replace) -> ok
%% @end
%%--------------------------------------------------------------------
put_attributes(Domain,ItemName, Attributes, Replace) ->
    gen_server:cast(?SERVER, {put_attributes, Domain, ItemName, Attributes, Replace}).


%%--------------------------------------------------------------------
%% @doc Replace an existing item with specified attributes
%% <pre>
%% Types:
%%  ItemName = string
%%  Attributes = array of key/value [[key1, value1], [key2, value2]...]
%% </pre>
%% @spec replace_attributes(ItemName, Attributes) -> ok
%% @end
%%--------------------------------------------------------------------
replace_attributes(Domain,ItemName, Attributes) ->
    put_attributes(Domain,ItemName, Attributes, true).


%%--------------------------------------------------------------------
%% @doc Retrieves an existing item with all attributes
%% <pre>
%% Types:
%%  ItemName = string
%% </pre>
%% @spec get_attributes(ItemName) -> {ok, [[key1, value1], [key2, value2], ..]} | {error, {ErrorCode, ErrorMessage}
%% @end
%%--------------------------------------------------------------------
get_attributes(Domain,ItemName) ->
    get_attributes(Domain,ItemName, nil).


%%--------------------------------------------------------------------
%% @doc Retrieves an existing item with matching attributes
%% <pre>
%% Types:
%%  ItemName = string
%%  Attributes = array of keys [key1, key2, ...]
%% </pre>
%% @spec get_attributes(ItemName, Attributes) -> {ok, [[key1, value1], [key2, value2], ..]} | {error, {ErrorCode, ErrorMessage}
%% @end
%%--------------------------------------------------------------------
get_attributes(Domain,ItemName, AttributeNames) ->
    gen_server:call(?SERVER, {get_attributes,Domain, ItemName, AttributeNames}, ?TIMEOUT).


%%--------------------------------------------------------------------
%% @doc Deletes an existing item 
%% <pre>
%% Types:
%%  ItemName = string
%%  Attributes = array of keys [key1, key2, ...]
%% </pre>
%% @spec delete_item(ItemName) -> ok
%% @end
%%--------------------------------------------------------------------
delete_item(Domain,ItemName) ->
    delete_attributes(Domain,ItemName).


%%--------------------------------------------------------------------
%% @doc Deletes all attributes for given item in domain
%% <pre>
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
%% <pre>
%% Types:
%%  ItemName = string
%%  Attributes = array of keys [key1, key2, ...]
%% </pre>
%% @spec delete_attributes(ItemName, AttributeNames) -> ok
%% @end
%%--------------------------------------------------------------------
delete_attributes(Domain,ItemName, AttributeNames) ->
    gen_server:cast(?SERVER, {delete_attributes,Domain, ItemName, AttributeNames}).


%%====================================================================
%% Server functions
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init/1
%% Description: Initiates the server
%% Returns: {ok, State}          |
%%          {ok, State, Timeout} |
%%          ignore               |
%%          {stop, Reason}
%%--------------------------------------------------------------------
init([Access, Secret]) ->
    ?DEBUG("******* erlsdb_server:init/1 starting~n", []),
    {ok, #state{access_key=Access, secret_key=Secret}}.

%%--------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_call({list_domains, MoreToken, MaxNumberOfDomains}, _From, #state{access_key=AccessKey, secret_key = SecretKey} = State) ->
    ?DEBUG("******* erlsdb_server:list_domains BEGIN~n", []),
    Base = base_parameters("ListDomains", AccessKey),
    Base1 = if MoreToken == nil -> Base; true -> Base ++ [["MoreToken", MoreToken]] end,
    Base2 = if MaxNumberOfDomains == nil -> Base1; true -> Base1 ++ [["MaxNumberOfDomains", MaxNumberOfDomains]] end,
    Response = rest_request(
	SecretKey, 
	Base2,
	fun(Xml) -> {ok, 
    	erlsdb_util:xml_values(xmerl_xpath:string("//DomainName/text()", Xml)),
    	erlsdb_util:xml_values(xmerl_xpath:string("//MoreToken/text()", Xml))} 
	end),
    {reply, Response, State};


handle_call({get_attributes, Domain, ItemName, AttributeNames}, _From, #state{access_key=AccessKey, secret_key = SecretKey} = State) ->
    ?DEBUG("******* erlsdb_server:get_attributes~n", []),
    Base = [{"DomainName", erlsdb_util:url_encode(Domain)},
	    {"ItemName", erlsdb_util:url_encode(ItemName)}|
		base_parameters("GetAttributes", AccessKey)] ++
		erlsdb_util:encode_attribute_names(AttributeNames),

    Response = rest_request(
	SecretKey, 
	Base,
	fun(Xml) -> {ok, 
        erlsdb_util:xml_names_values(xmerl_xpath:string("//Attribute", Xml))} end),
    {reply, Response, State};


%%--------------------------------------------------------------------
%% Function: handle_call/3
%% Description: Handling call messages
%% Returns: {reply, Reply, State}          |
%%          {reply, Reply, State, Timeout} |
%%          {noreply, State}               |
%%          {noreply, State, Timeout}      |
%%          {stop, Reason, Reply, State}   | (terminate/2 is called)
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_call(Request, From, State) ->
    Reply = {unknown_request, Request, From, State},
    {reply, Reply, State}.




%%--------------------------------------------------------------------
%% Function: handle_cast/2
%% Description: Handling cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_cast(stop, State) ->
    {stop, normal, State};

    
handle_cast({create_domain, Domain}, #state{access_key=AccessKey, secret_key = SecretKey } = State) -> 
    ?DEBUG("******* erlsdb_server:create_domain ~p~n", [Domain]),
    Base = [{"DomainName", erlsdb_util:url_encode(Domain)}| 
		base_parameters("CreateDomain", AccessKey)],
    rest_request(SecretKey, Base, fun(_Xml) -> nil end),
    {noreply, State};


handle_cast({delete_domain, Domain}, #state{access_key=AccessKey, secret_key = SecretKey } = State) -> 
    ?DEBUG("******* erlsdb_server:delete_domain ~p~n", [Domain]),
    Base = [{"DomainName", erlsdb_util:url_encode(Domain)}| 
		base_parameters("DeleteDomain", AccessKey)],
    rest_request(SecretKey, Base, fun(_Xml) -> nil end),
    {noreply, State};


handle_cast({put_attributes,Domain, ItemName, Attributes, Replace}, #state{access_key=AccessKey, secret_key = SecretKey } = State) -> 
    ?DEBUG("******* erlsdb_server:put_attributes ~p~n", [Domain, ItemName, Attributes]),
    Base = [{"DomainName", erlsdb_util:url_encode(Domain)},
	    {"ItemName", erlsdb_util:url_encode(ItemName)}|
		base_parameters("PutAttributes", AccessKey)] ++
		erlsdb_util:encode_attributes(Attributes),
    Base1 = if Replace == false -> Base; true -> Base ++ [["Replace", "true"]] end,
    rest_request(SecretKey, Base1, fun(_Xml) -> nil end),
    {noreply, State};

handle_cast({delete_attributes, Domain, ItemName, AttributeNames}, #state{access_key=AccessKey, secret_key = SecretKey} = State) -> 
    ?DEBUG("******* erlsdb_server:delete_attributes ~p~n", [Domain, ItemName, AttributeNames]),
    Base = [{"DomainName", erlsdb_util:url_encode(Domain)},
	    {"ItemName", erlsdb_util:url_encode(ItemName)}|
		base_parameters("DeleteAttributes", AccessKey)] ++
		erlsdb_util:encode_attribute_names(AttributeNames),
    rest_request(SecretKey, Base, fun(_Xml) -> nil end),
    {noreply, State};

handle_cast(Msg, State) ->
    ?DEBUG("******* handle_cast unexpected message ~p, state ~p ~n", [Msg, State]),
    {noreply, State}.


%%--------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate/2
%% Description: Shutdown the server
%% Returns: any (ignored by gen_server)
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change/3
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState}
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%%% Internal functions
%%====================================================================
rest_request(SecretKey, Params, XmlParserFunc) ->
    Url = uri() ++ query_string(SecretKey, Params),
    ?DEBUG("******* Connecting to ~p ~n", [Url]),
    Response = http:request(Url),
    case Response of 
        {ok, {{_HttpVersion, StatusCode, _ErrorMessage}, _Headers, Body }} ->
    	    ?DEBUG("******* URL ~p Status ~p ~n", [Url, StatusCode]),
            {Xml, _Rest} = xmerl_scan:string(Body),
    	    %%%io:format("********* Xml ~p~n", [Xml]),
            case StatusCode of
    	        200 ->
	            XmlParserFunc(Xml);
	        _ ->
    		    [#xmlText{value=ErrorCode}]    = xmerl_xpath:string("//Error/Code/text()", Xml),
    		    [#xmlText{value=ErrorMessage}] = xmerl_xpath:string("//Error/Message/text()", Xml),
    	            {error, ErrorCode, ErrorMessage}
            end;
        {error, ErrorMessage} ->
	    case ErrorMessage of 
		Error when  Error == timeout -> %Error == nxdomain orelse
    	    	    ?DEBUG("URL ~p Timedout, retrying~n", [Url]),
    	    	    erlsdb_util:sleep(1000),
		    rest_request(SecretKey, Params, XmlParserFunc);
		_ ->
    	           {error, http_error, ErrorMessage}
	    end
    end.


query_string(SecretKey, Params) ->
    Params1 = lists:sort(
	fun({A, _}, {X, _}) -> A < X end,
	Params),
    QueryStr = 
	string:join(lists:foldr(fun query_string1/2, [], Params1), "&"),
    SignatureData = "GET\nsdb.amazonaws.com\n/\n" ++ QueryStr,
    QueryStr ++ "&Signature=" ++ erlsdb_util:url_encode(signature(SecretKey, SignatureData)).


query_string1({Key, Value}, Query) ->
    [Key ++ "=" ++ erlsdb_util:url_encode(Value) | Query].


%%%
% Returns HMAC encoded access key
%%%
signature(SecretKey, Data) ->
    binary_to_list(
        base64:encode(crypto:sha_mac(SecretKey, Data))).



base_parameters(Action, AccessKey) ->
    [{"Action", Action},
     {"AWSAccessKeyId", AccessKey},
     {"Version", version()},
     {"SignatureVersion", "2"},
     {"SignatureMethod", "HmacSHA1"},
     {"Timestamp", erlsdb_util:create_timestamp()}].




%%%-------------------------------------------------------------------
%%% Configuration Functions %%%
%%%-------------------------------------------------------------------
uri() ->
   "http://sdb.amazonaws.com?".


version() ->
   %"2007-11-07".
   "2007-11-07".

