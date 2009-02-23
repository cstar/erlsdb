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
-author("Eric Cestari <ecestari@mac.com> [http://www.cestari.info]").
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
	start_link/3,
	stop/0
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
start_link(Access, Secret, SSL) ->
    %%?DEBUG("******* erlsdb_server:start_link/1 starting~n", [InitialState]),
    gen_server:start_link(?MODULE, [Access, Secret, SSL], []).

%%--------------------------------------------------------------------
%% @doc Stops the server.
%% @spec stop() -> ok
%% @end
%%--------------------------------------------------------------------
stop() ->
    gen_server:cast(?SERVER, stop).

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
init([Access, Secret, SSL]) ->
    ?DEBUG("******* erlsdb_server:init/1 starting~n", []),
    {ok, #state{ssl = SSL,access_key=Access, secret_key=Secret, pending=gb_trees:empty()}}.

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
handle_call({list_domains, MoreToken, MaxNumberOfDomains}, From,State) ->
    ?DEBUG("******* erlsdb_server:list_domains BEGIN~n", []),
    rest_request(
	From, 
	"ListDomains",
	[{"MaxNumberOfDomains", i2l(MaxNumberOfDomains)},{"NextToken", MoreToken}],
	fun(Xml) -> {ok, 
    	erlsdb_util:xml_values(xmerl_xpath:string("//DomainName/text()", Xml)),
    	parse_token(Xml)} 
	end, State);


handle_call({get_attributes, Domain, ItemName, AttributeNames}, From,  State) ->
    ?DEBUG("******* erlsdb_server:get_attributes~n", []),
    Base = [{"DomainName", Domain},
	    {"ItemName", ItemName} |
		erlsdb_util:encode_attribute_names(AttributeNames)],
    rest_request(
	From, 
	"GetAttributes",
	Base,
	fun(Xml) -> {ok, 
        erlsdb_util:xml_names_values(xmerl_xpath:string("//Attribute", Xml))} end,
    State);
    
handle_call({create_domain, Domain}, From, State) -> 
    ?DEBUG("******* erlsdb_server:create_domain ~p~n", [Domain]),
    Base = [{"DomainName", Domain}],
    rest_request(From, "CreateDomain",Base, fun(_Xml) -> ok end, State);


handle_call({delete_domain, Domain}, From,State) -> 
    ?DEBUG("******* erlsdb_server:delete_domain ~p~n", [Domain]),
    Base = [{"DomainName", Domain}],
    rest_request(From,"DeleteDomain", Base, fun(_Xml) -> ok end, State);


handle_call({put_attributes,Domain, ItemName, Attributes, Replace},From,  State) -> 
    ?DEBUG("******* erlsdb_server:put_attributes ~p~n", [Domain, ItemName, Attributes]),
    Base = [{"DomainName", Domain},
	    {"ItemName", ItemName}|
		erlsdb_util:encode_attributes(Attributes)],
    Base1 = if Replace == false -> Base; true -> Base ++ [{"Replace", "true"}] end,
    rest_request(From, "PutAttributes", Base1, fun(_Xml) -> ok end, State);

handle_call({delete_attributes, Domain, ItemName, AttributeNames},From,  State) -> 
    ?DEBUG("******* erlsdb_server:delete_attributes ~p~n", [Domain, ItemName, AttributeNames]),
    Base = [{"DomainName", Domain},
	    {"ItemName", ItemName} |
		erlsdb_util:encode_attribute_names(AttributeNames)],
    rest_request(From, "DeleteAttributes", Base, fun(_Xml) -> ok end, State);

handle_call({domain_metadata, Domain},From,  State) ->
    rest_request(From, "DomainMetadata", [{"DomainName", Domain}], 
        fun(Xml) ->
            {ok,
           [{"Timestamp", erlsdb_util:xml_int(xmerl_xpath:string("//Timestamp/text()", Xml))},
            {"ItemCount", erlsdb_util:xml_int(xmerl_xpath:string("//ItemCount/text()", Xml))},
            {"AttributeValueCount", erlsdb_util:xml_int(xmerl_xpath:string("//AttributeValueCount/text()", Xml))},
            {"AttributeNameCount", erlsdb_util:xml_int(xmerl_xpath:string("//AttributeNameCount/text()", Xml))},
            {"ItemNamesSizeBytes", erlsdb_util:xml_int(xmerl_xpath:string("//ItemNamesSizeBytes/text()", Xml))},
            {"AttributeValuesSizeBytes", erlsdb_util:xml_int(xmerl_xpath:string("//AttributeValuesSizeBytes/text()", Xml))},
            {"AttributeNamesSizeBytes", erlsdb_util:xml_int(xmerl_xpath:string("//AttributeNamesSizeBytes/text()", Xml))}]
            }
        end, State);
        
handle_call({q,Domain, Query, MaxNumber, NextToken}, From, State)->
    rest_request(From, "Query", 
            [{"DomainName", Domain}, {"QueryExpression", Query}, {"MaxNumberOfItems", i2l(MaxNumber)}, {"NextToken", NextToken}],
            fun(Xml) -> {ok, 
    	erlsdb_util:xml_values(xmerl_xpath:string("//ItemName/text()", Xml)),
    	parse_token(Xml)} 
	end, State);
handle_call({qwa ,Domain, Query,  AttributeNames, MaxNumber, NextToken}, From, State)->
    rest_request(From, "QueryWithAttributes", 
            [{"DomainName", Domain}, {"QueryExpression", Query}, 
             {"MaxNumberOfItems", i2l(MaxNumber)}, {"NextToken", NextToken}|
		     erlsdb_util:encode_attribute_names(AttributeNames)],
            fun(Xml) -> {ok, 
    	            erlsdb_util:parse_items(Xml),
    	            parse_token(Xml)} 
	end, State);
	
handle_call({select,Query, NextToken}, From, State)->
    rest_request(From, "Select", 
            [{"SelectExpression", Query}, {"NextToken", NextToken}],
            fun(Xml) -> {ok, 
    	erlsdb_util:parse_items(Xml),
    	parse_token(Xml)} 
	end, State);
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
handle_info({http,{RequestId,Response}},State = #state{pending=P}) ->
    ?DEBUG("******* Response :  ~p~n", [Response]),
	case gb_trees:lookup(RequestId,P) of
		{value,{Client,RequestOp}} -> handle_http_response(Response,RequestOp,Client, State),
						 {noreply,State#state{pending=gb_trees:delete(RequestId,P)}};
		none -> {noreply,State}
				%% the requestid isn't here, probably the request was deleted after a timeout
	end;
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
rest_request(From, Action, Params, XmlParserFunc, #state{ssl=SSL, access_key = AccessKey, secret_key = SecretKey, pending=P} = State) ->
    FullParams = Params ++ base_parameters(Action, AccessKey),
    Url = uri(SSL) ++ query_string(SecretKey, FullParams),
    ?DEBUG("******* Connecting to ~p ~n", [Url]),
    {ok,RequestId} = http:request(get , {Url, []},[{timeout, ?TIMEOUT}],[{sync,false}]),
    Pendings = gb_trees:insert(RequestId,{From, XmlParserFunc},P),
    {noreply, State#state{pending=Pendings}}.
    

handle_http_response(HttpResponse,RequestOp,Client, _State)->
    case HttpResponse of 
        {{_HttpVersion, StatusCode, _ErrorMessage}, _Headers, Body } ->
    	    ?DEBUG("******* Status ~p ~n", [StatusCode]),
            {Xml, _Rest} = xmerl_scan:string(binary_to_list(Body)),
    	    %%%io:format("********* Xml ~p~n", [Xml]),
            case StatusCode of
    	        200 ->
	            gen_server:reply(Client,RequestOp(Xml));
	        _ ->
    		    [#xmlText{value=ErrorCode}]    = xmerl_xpath:string("//Error/Code/text()", Xml),
    		    [#xmlText{value=ErrorMessage}] = xmerl_xpath:string("//Error/Message/text()", Xml),
    	            gen_server:reply(Client,{error, ErrorCode, ErrorMessage})
            end;
        {error, ErrorMessage} ->
	    case ErrorMessage of 
		    %Error when  Error == timeout -> %Error == nxdomain orelse
    	    %	    ?DEBUG("URL Timedout, retrying~n", []),
    	    %	    erlsdb_util:sleep(1000),
		    %rest_request(SecretKey, Params, XmlParserFunc);
		_ ->
    	           gen_server:reply(Client,{error, http_error, ErrorMessage})
	    end
    end.
query_string(SecretKey, Params) ->
    Params1 = lists:filter(fun({_, nil}) -> false ; (_) -> true end, Params),
    Params2 = lists:sort(
	fun({A, _}, {X, _}) -> A < X end,
	Params1),
    QueryStr = 
	string:join(lists:foldr(fun query_string1/2, [], Params2), "&"),
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


parse_token(Xml)->
    case erlsdb_util:xml_values(xmerl_xpath:string("//NextToken/text()", Xml)) of
        [] -> nil;
        R -> hd(R)
    end.

i2l(nil) -> nil;
i2l(I) when integer(I) -> integer_to_list(I);
i2l(L) when list(L)    -> L.

%%%-------------------------------------------------------------------
%%% Configuration Functions %%%
%%%-------------------------------------------------------------------
uri(false) ->
   "http://sdb.amazonaws.com/?";
uri(true) ->
   "https://sdb.amazonaws.com/?".

version() ->
   "2007-11-07".

