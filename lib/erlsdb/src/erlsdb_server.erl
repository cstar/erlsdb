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

%%--------------------------------------------------------------------
%% External exports
%%--------------------------------------------------------------------
-export([
	start_link/1,
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
%% macro definitions
%%--------------------------------------------------------------------
-define(SERVER, ?MODULE).
-define(TIMEOUT, 20000).
-define(DEBUG(Format, Args), error_logger:info_msg("D(~p:~p:~p) : "++Format++"~n",
         [self(),?MODULE,?LINE]++Args)).
%%--------------------------------------------------------------------
%% record definitions
%%--------------------------------------------------------------------
-record(state, {ssl,access_key, secret_key, pending, timeout=?TIMEOUT}).
-record(request, {pid, callback, action, params, code, headers=[], content=[]}).
%%====================================================================
%% External functions
%%====================================================================
%%--------------------------------------------------------------------
%% @doc Starts the server.
%% @spec start_link() -> {ok, pid()} | {error, Reason}
%% @end
%%--------------------------------------------------------------------
start_link([Access, Secret, SSL, Timeout]) ->
    gen_server:start_link(?MODULE, [Access, Secret, SSL, Timeout], []).

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
init([Access, Secret, SSL, nil]) ->
    {ok, #state{ssl = SSL,access_key=Access, secret_key=Secret, pending=gb_trees:empty()}};
init([Access, Secret, SSL, Timeout]) ->
    {ok, #state{ssl = SSL,access_key=Access, secret_key=Secret, timeout=Timeout, pending=gb_trees:empty()}}.



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
    rest_request(
	From, 
	"ListDomains",
	[{"MaxNumberOfDomains", i2l(MaxNumberOfDomains)},{"NextToken", MoreToken}],
	fun(Xml) -> {ok, 
    	erlsdb_util:xml_values(xmerl_xpath:string("//DomainName/text()", Xml)),
    	parse_token(Xml)} 
	end, State);


handle_call({get_attributes, Domain, ItemName, AttributeNames}, From,  State) ->
    {Encoded, _} = lists:foldl(fun(Key, {Enc, I})->
            KeyName = "AttributeName." ++ integer_to_list(I),
            {[{KeyName, Key}|Enc], I+1}
        end, {[], 0}, AttributeNames),
    Base = [{"DomainName", Domain},
	    {"ItemName", ItemName} |
		Encoded],
    rest_request(
	From, 
	"GetAttributes",
	Base,
	fun(Xml) -> {ok, 
        erlsdb_util:xml_names_values(xmerl_xpath:string("//Attribute", Xml))} end,
    State);
    
handle_call({create_domain, Domain}, From, State) -> 
    Base = [{"DomainName", Domain}],
    rest_request(From, "CreateDomain",Base, fun(_Xml) -> ok end, State);


handle_call({delete_domain, Domain}, From,State) -> 
    Base = [{"DomainName", Domain}],
    rest_request(From,"DeleteDomain", Base, fun(_Xml) -> ok end, State);


handle_call({put_attributes,Domain, ItemName, Attributes, Replace},From,  State) -> 
    Base = [{"DomainName", Domain},
	    {"ItemName", ItemName}|
		erlsdb_util:encode_attributes(Attributes)],
    Base1 = if Replace == false -> Base; 
                true -> 
                    {Encoded, _} = 
                    lists:foldl(fun(_A, {Enc, I})->
                        KeyName = "Attribute." ++ integer_to_list(I) ++ ".Replace",
                        {[{KeyName, "true"}|Enc], I+1}
                    end, {[], 0}, Attributes),
                    Base ++ Encoded
            end,
    rest_request(From, "PutAttributes", Base1, fun(_Xml) -> ok end, State);

%%Items = [{ItemName,Attributes,Replace}|Tail] list of items
handle_call({batch_put_attributes,Domain,Items},From, State) -> 
        [_Count|Base0]=lists:foldl(fun({ItemName,Attributes,Replace},[N|Acc])->
                    Base =[{"Item."++integer_to_list(N)++".ItemName", ItemName} | lists:foldl(fun({Key,Val}, TmpAcc)->
                                    TmpKeyName = "Item."++integer_to_list(N)++"."++Key,
                                    [{TmpKeyName,Val}|TmpAcc]    end,
                                 [], erlsdb_util:encode_attributes(Attributes))],
                    Base1 = if Replace == false -> Base; 
                    true -> 
                        {Encoded, _} =  lists:foldl(fun(_A, {Enc, I})->
                            KeyName = "Item."++integer_to_list(N)++".Attribute." ++ integer_to_list(I) ++ ".Replace",
                            {[{KeyName, "true"}|Enc], I+1}
                        end, {[], 0}, Attributes),
                        Base ++ Encoded
                    end,
            [N+1,Base1|Acc] end,[0],Items),
        Params=lists:append([[{"DomainName", Domain}]|Base0]),
        rest_request(From, "BatchPutAttributes", Params, fun(_Xml) -> ok end, State);

handle_call({delete_attributes, Domain, ItemName, AttributeNames},From,  State) -> 
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

handle_cast(_Msg, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% Function: handle_info/2
%% Description: Handling all non call/cast messages
%% Returns: {noreply, State}          |
%%          {noreply, State, Timeout} |
%%          {stop, Reason, State}            (terminate/2 is called)
%%--------------------------------------------------------------------

handle_info({ibrowse_async_headers,RequestId,Code,Headers },State = #state{pending=P}) ->
    %%?DEBUG("******* Response :  ~p~n", [Response]),
	case gb_trees:lookup(RequestId,P) of
		{value,#request{pid=Pid}=R} ->
		    {ICode, []} = string:to_integer(Code),
		    if ICode >= 500 ->
		        gen_server:reply(Pid,retry), 
			    {noreply,State#state{pending=gb_trees:delete(RequestId,P)}};
			true ->
			    {noreply,State#state{pending=gb_trees:enter(RequestId,R#request{code = Code, headers=Headers},P)}}
			end;
		none -> 
		    {noreply,State}
			%% the requestid isn't here, probably the request was deleted after a timeout
	end;
handle_info({ibrowse_async_response,_RequestId,{chunk_start, _N} },State) ->
    {noreply, State};
handle_info({ibrowse_async_response,_RequestId,chunk_end },State) ->
    {noreply, State};	
    
handle_info({ibrowse_async_response,RequestId,Body },State = #state{pending=P}) when is_list(Body)->
    %?DEBUG("******* Response :  ~p~n", [Response]),
	case gb_trees:lookup(RequestId,P) of
		{value,#request{content=Content}=R} -> 
			{noreply,State#state{pending=gb_trees:enter(RequestId,R#request{content=Content ++ Body}, P)}};
		none -> {noreply,State}
			%% the requestid isn't here, probably the request was deleted after a timeout
	end;
handle_info({ibrowse_async_response_end,RequestId}, State = #state{pending=P})->
    case gb_trees:lookup(RequestId,P) of
		{value,R} -> 
		    handle_http_response(R),
			{noreply,State#state{pending=gb_trees:delete(RequestId, P)}};
		none -> {noreply,State}
			%% the requestid isn't here, probably the request was deleted after a timeout
	end;
	
handle_info({ibrowse_async_response,RequestId,{error,Error}}, State = #state{pending=P})->
    case gb_trees:lookup(RequestId,P) of
		{value,#request{pid=Pid}} -> 
		    error_logger:warning_msg("Warning query failed (retrying) : ~p~n", [Error]),
		    gen_server:reply(Pid, retry),
		    {noreply,State#state{pending=gb_trees:delete(RequestId, P)}};
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
rest_request(From, Action, Params, Callback, #state{ssl=SSL, access_key = AccessKey, secret_key = SecretKey, pending=P, timeout=Timeout} = State) ->
    FullParams = Params ++ base_parameters(Action, AccessKey),
    Url = uri(SSL) ++ query_string(SecretKey, FullParams),
    %?DEBUG("******* Connecting to ~p ~n", [Url]),
    case ibrowse:send_req(Url,[], get , [],[{is_ssl, SSL},{ssl_options, []},{stream_to, self()}], Timeout) of
    {ibrowse_req_id,RequestId} ->
            Pendings = gb_trees:insert(RequestId,#request{pid=From,callback=Callback, action=Action, params=Params},P),
            {noreply, State#state{pending=Pendings}};
        {error,E} when E =:= retry_later orelse E =:= conn_failed ->
            {reply, retry, State};
        {error, E} ->
            io:format("Error : ~p, Pid : ~p~n", [E, self()]),
            {reply, {error, E, "Error Occured"}, State}
    end.
    
handle_http_response(#request{pid=From, callback=CallBack, code=Code, content=Content})
                    when Code =:= "200" orelse Code =:= "204"->
    {Xml, _Rest} = xmerl_scan:string(Content),
    gen_server:reply(From, CallBack(Xml));
handle_http_response(#request{pid=From, content=Content})->
    {Xml, _Rest} = xmerl_scan:string(Content),
    [#xmlText{value=ErrorCode}]    = xmerl_xpath:string("//Error/Code/text()", Xml),
    [#xmlText{value=ErrorMessage}] = xmerl_xpath:string("//Error/Message/text()", Xml),
    gen_server:reply(From,{error, ErrorCode, ErrorMessage}).
    

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
    [Key ++ "=" ++ erlsdb_util:url_encode(i2l(Value)) | Query].


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

