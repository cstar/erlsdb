%%%-------------------------------------------------------------------
%%% @author Shahzad Bhatti <bhatti@plexobject.com>
%%% @doc
%%%   Business delegate to access SimpleDB
%%% @end
%%%-------------------------------------------------------------------
-module(erlsdb).
-include_lib("xmerl/include/xmerl.hrl").


%%%-------------------------------------------------------------------
%%% Public APIs
%%%-------------------------------------------------------------------
-export([list_domains/0, list_domains/1, list_domains/2, delete_domain/1, get_attributes/2, get_attributes/3, create_domain/1, put_attributes/3, put_attributes/4, delete_item/2, delete_attributes/2, delete_attributes/3, replace_attributes/3]).


%%%-------------------------------------------------------------------
%%% Test Methods
%%%-------------------------------------------------------------------
-export([test/0]).


%%%-------------------------------------------------------------------
%%% Configuration Functions %%%
%%%-------------------------------------------------------------------
uri() ->
   "http://sdb.amazonaws.com?".


version() ->
   "2007-11-07".

access_key() ->
   "key".

secret_key() ->
    "secretkey".



%%%-------------------------------------------------------------------
%%% Public APIs to access SimpleDB 
%%%-------------------------------------------------------------------
start() ->
    crypto:start(),
    inets:start().

stop() ->
    init:stop().


list_domains() ->
    list_domains(nil, nil).
list_domains(MoreToken) ->
    list_domains(MoreToken, nil).

list_domains(MoreToken, MaxResults) ->
    Base = base_parameters("ListDomains"),
    Base1 = if MoreToken == nil -> Base; true -> Base ++ [["MoreToken", MoreToken]] end,
    Base2 = if MaxResults == nil -> Base1; true -> Base1 ++ [["MaxResults", MaxResults]] end,
    Xml = rest_request(Base2),
    xml_values(xmerl_xpath:string("//DomainName/text()", Xml)).


create_domain(Domain) ->
    Base = [["DomainName", url_encode(Domain)]| 
		base_parameters("CreateDomain")],
    rest_request(Base).

delete_domain(Domain) ->
    Base = [["DomainName", url_encode(Domain)]| 
		base_parameters("DeleteDomain")],
    rest_request(Base).

replace_attributes(Domain, ItemName, Attributes) ->
    put_attributes(Domain, ItemName, Attributes, true).
put_attributes(Domain, ItemName, Attributes) ->
    put_attributes(Domain, ItemName, Attributes, false).
put_attributes(Domain, ItemName, Attributes, Replace) ->
    Base = [["DomainName", url_encode(Domain)],
	    ["ItemName", url_encode(ItemName)]|
		base_parameters("PutAttributes")] ++
		encode_attributes(Attributes),
    Base1 = if Replace == false -> Base; true -> Base ++ [["Replace", "true"]] end,
    rest_request(Base1).


delete_item(Domain, ItemName) ->
    delete_attributes(Domain, ItemName).


delete_attributes(Domain, ItemName) ->
    delete_attributes(Domain, ItemName, nil).
delete_attributes(Domain, ItemName, AttributeNames) ->
    Base = [["DomainName", url_encode(Domain)],
	    ["ItemName", url_encode(ItemName)]|
		base_parameters("DeleteAttributes")] ++
		encode_attribute_names(AttributeNames),
    rest_request(Base).

get_attributes(Domain, ItemName) ->
    get_attributes(Domain, ItemName, nil).
get_attributes(Domain, ItemName, AttributeNames) ->
    Base = [["DomainName", url_encode(Domain)],
	    ["ItemName", url_encode(ItemName)]|
		base_parameters("GetAttributes")] ++
		encode_attribute_names(AttributeNames),
    Xml = rest_request(Base),
    xml_names_values(xmerl_xpath:string("//Attribute", Xml)).


%%%-------------------------------------------------------------------
%%% Private Functions
%%%-------------------------------------------------------------------

rest_request(Params) ->
    Url = uri() ++ query_string(Params),
    Response = http:request(Url),
    case Response of 
        {ok, {{_HttpVersion, StatusCode, _ErrorMessage}, _Headers, Body }} ->
            error_logger:info_msg("URL ~p Status ~p~n", [Url, StatusCode]),
            {Xml, _Rest} = xmerl_scan:string(Body),
            %%%error_logger:info_msg("Xml ~p~n", [Xml]),
            case StatusCode of
    	        200 ->
	            Xml;
	        _ ->
    	           Error = xml_values(xmerl_xpath:string("//Message/text()", Xml)),
    	           throw({Error})
            end;
        {error, Message} ->
	    case Message of 
		timeout ->
            	    io:format("URL ~p Timedout, retrying~n", [Url]),
    	    	    sleep(1000),
		    rest_request(Params);
		true ->
    	            throw({Message})
	    end
    end.


query_string(Params) ->
    Params1 = lists:sort(
	fun([Elem1, _], [Elem2, _]) -> 
	    string:to_lower(Elem1) > string:to_lower(Elem2) end,
	Params),
    {QueryStr, SignatureData} = 
	lists:foldr(fun query_string/2, {"", ""}, Params1),
    QueryStr ++ "Signature=" ++ url_encode(signature(SignatureData)).


query_string([Key, Value], {QueryStr, SignatureData}) ->
    QueryStr1 = QueryStr ++ Key ++ "=" ++ url_encode(Value) ++ "&",
    SignatureData1 = SignatureData ++ Key ++ Value,
    {QueryStr1, SignatureData1}.


encode_attributes(Attributes) when Attributes == nil ->
    [];
encode_attributes(Attributes) ->
    {Encoded, _} = lists:foldr(fun encode_attributes/2, {[], 0}, Attributes),
    Encoded.

encode_attributes([Key, Value], {Encoded, I}) ->
    KeyName = "Attribute." ++ integer_to_list(I) ++ ".Name",
    KeyValue = "Attribute." ++ integer_to_list(I) ++ ".Value",
    {[[KeyName, Key], [KeyValue, Value]|Encoded], I+1}.


encode_attribute_names(Attributes) when Attributes == nil ->
    [];
encode_attribute_names(Attributes) ->
    {Encoded, _} = lists:foldr(fun encode_attribute_names/2, {[], 0}, Attributes),
    Encoded.

encode_attribute_names(Key, {Encoded, I}) ->
    KeyName = "Attribute." ++ integer_to_list(I) ++ ".Name",
    {[[KeyName, Key]|Encoded], I+1}.




%%%
% Converts a number into 2-digit 
%%%
two_digit(X) when is_integer(X), X >= 10 ->
    integer_to_list(X);
two_digit(X) when is_integer(X), X < 10 ->
    "0" ++ integer_to_list(X).

abs_two_digit(X) when X < 0 ->
    two_digit(0-X);
abs_two_digit(X) when X >= 0 ->
    two_digit(X).

%%%
% Returns Coordinated Universal Time (Greenwich Mean Time) time zone,
%%%
timestamp() ->
    {{_, _, _}, {_LocalHour, _LocalMin, _}} = LocalDateTime = calendar:local_time(),
    [{{Year, Month, Day}, {Hour, Min, Sec}}] = 
	calendar:local_time_to_universal_time_dst(LocalDateTime),
    Z = gmt_difference(),
    integer_to_list(Year) ++ "-" ++ two_digit(Month) ++ "-" ++ two_digit(Day) 
	++ "T" ++ two_digit(Hour) ++ ":" ++ two_digit(Min) ++ ":" ++ 
	two_digit(Sec) ++ Z.


gmt_difference() ->
    "-08:00".

gmt_difference(Hour, Min, LocalHour, LocalMin) ->
    if 
	Hour < LocalHour -> 
    	    "-" ++ abs_two_digit(LocalHour-Hour) ++ ":" ++ abs_two_digit(LocalMin-Min);
	true -> 
    	    "+" ++ abs_two_digit(LocalHour-Hour) ++ ":" ++ abs_two_digit(LocalMin-Min)
    end.


%%%
% Returns HMAC encoded access key
%%%
signature(Data) ->
    hmac(secret_key(), Data).

hmac(SecretKey, Data) ->
    http_base_64:encode(
          binary_to_list(crypto:sha_mac(SecretKey, Data))).


base_parameters(Action) ->
    [["Action", Action],
     ["AWSAccessKeyId", access_key()],
     ["Version", version()],
     ["SignatureVersion", "1"],
     ["Timestamp", timestamp()]].



%%%
% This method retrieves node value from the XML records that are returned
% after scanning tags.
%%%
xml_values(List) ->
    lists:foldr(fun xml_values/2, [], List).

xml_values(#xmlText{value=Value}, List) ->
    [Value|List].

    
xml_names_values(List) ->
    lists:foldr(fun xml_names_values/2, [], List).

xml_names_values(Xml, List) ->
    [ #xmlText{value=Name} ]  = xmerl_xpath:string("//Name/text()", Xml),
    [ #xmlText{value=Value} ]  = xmerl_xpath:string("//Value/text()", Xml),
    [[Name, Value]|List].

  
sleep(T) ->
    receive
    after T ->
       true
    end.


%%%
% URL encode - borrowed from CouchDB
%%%
url_encode([H|T]) ->
    if
        H >= $a, $z >= H ->
            [H|url_encode(T)];
        H >= $A, $Z >= H ->
            [H|url_encode(T)];
        H >= $0, $9 >= H ->
            [H|url_encode(T)];
        H == $_; H == $.; H == $-; H == $: ->
            [H|url_encode(T)];
        true ->
            case lists:flatten(io_lib:format("~.16.0B", [H])) of
                [X, Y] ->
                    [$%, X, Y | url_encode(T)];
                [X] ->
                    [$%, $0, X | url_encode(T)]
            end
    end;
url_encode([]) ->
    [].


%%%-------------------------------------------------------------------
%%% Test Functions
%%%-------------------------------------------------------------------
test_list_domains() ->
    list_domains().

test_create_domain() ->
    create_domain("TestDomain"),
    ["TestDomain"] = lists:filter(
			fun(Elem) -> Elem == "TestDomain" end,
			list_domains()).


test_delete_domain() ->
    delete_domain("TestDomain"),
    [] = lists:filter(
			fun(Elem) -> Elem == "TestDomain" end,
			list_domains()).

test_put_get_attributes() ->
    create_domain("TestDomain"),
    Attributes = lists:sort([
	["StreetAddress", "705 5th Ave"],
        ["City", "Seattle"],
        ["State", "WA"],
        ["Zip", "98101"]
	]),
    put_attributes("TestDomain", "TCC", Attributes),
    Attributes = lists:sort(get_attributes("TestDomain", "TCC")).

test_put_delete_attributes() ->
    create_domain("TestDomain"),
    Attributes = lists:sort([
	["StreetAddress", "705 5th Ave"],
        ["City", "Seattle"],
        ["State", "WA"],
        ["Zip", "98101"]
	]),
    put_attributes("TestDomain", "TCC", Attributes),
    delete_attributes("TestDomain", "TCC"),
    sleep(1000),	%% Let it sync
    [] = get_attributes("TestDomain", "TCC").

test() ->
    start(),
    test_create_domain(),
    test_delete_domain(),
    test_put_get_attributes(),
    test_put_delete_attributes(),
    stop().
