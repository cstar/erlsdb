%%%-------------------------------------------------------------------
%% @author Shahzad Bhatti <bhatti@plexobject.com> [http://bhatti.plexobject.com]
%% @doc common utility functions
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
-module(erlsdb_util).
-author("Shahzad Bhatti <bhatti@plexobject.com> [http://bhatti.plexobject.com]").
-include_lib("xmerl/include/xmerl.hrl").

%%%-------------------------------------------------------------------
%%% Public APIs
%%%-------------------------------------------------------------------
-export([
	encode_attributes/1,
	encode_attributes/2,
	encode_attribute_names/1,
	encode_attribute_names/2,
	two_digit/1,
	abs_two_digit/1,
	timestamp/0,
	gmt_difference/0,
	hmac/2,
	xml_values/1,
	xml_values/2,
	xml_names_values/1,
	xml_names_values/2,
	sleep/1,
	url_encode/1
	]).


%%====================================================================
%% Utility functions
%%====================================================================


%%--------------------------------------------------------------------
%% @doc encode_attributes
%% <pre>
%% Types:
%%  Attributes = array of key/value, e.g. [[key1, value1, [key2, value2], ..]
%% </pre>
%% @spec encode_attribute(Attributes) -> [[keyname, key], [valuename, value],...]
%% @end
%%--------------------------------------------------------------------
encode_attributes(Attributes) when Attributes == nil ->
    [];
encode_attributes(Attributes) ->
    {Encoded, _} = lists:foldr(fun encode_attributes/2, {[], 0}, Attributes),
    Encoded.

encode_attributes([Key, Value], {Encoded, I}) ->
    KeyName = "Attribute." ++ integer_to_list(I) ++ ".Name",
    KeyValue = "Attribute." ++ integer_to_list(I) ++ ".Value",
    {[[KeyName, Key], [KeyValue, Value]|Encoded], I+1}.


%%--------------------------------------------------------------------
%% @doc encode_attribute_namee
%% <pre>
%% Types:
%%  Attributes = array of names, e.g. [key1, key2, ..]
%% </pre>
%% @spec encode_attribute_names(Attributes) -> [[keyname, key], ...]
%% @end
%%--------------------------------------------------------------------
encode_attribute_names(Attributes) when Attributes == nil ->
    [];
encode_attribute_names(Attributes) ->
    {Encoded, _} = lists:foldr(fun encode_attribute_names/2, {[], 0}, Attributes),
    Encoded.

encode_attribute_names(Key, {Encoded, I}) ->
    KeyName = "Attribute." ++ integer_to_list(I) ++ ".Name",
    {[[KeyName, Key]|Encoded], I+1}.




%%--------------------------------------------------------------------
%% @doc two_digit
%% <pre>
%% Types:
%%  Attributes = integer
%% </pre>
%% @spec encode_attributes(Attributes) -> ok
%% @end
%%--------------------------------------------------------------------
two_digit(X) when is_integer(X), X >= 10 ->
    integer_to_list(X);
two_digit(X) when is_integer(X), X < 10 ->
    "0" ++ integer_to_list(X).

abs_two_digit(X) when X < 0 ->
    two_digit(0-X);
abs_two_digit(X) when X >= 0 ->
    two_digit(X).

%%--------------------------------------------------------------------
%% @doc timestamp returns Coordinated Universal Time (Greenwich Mean Time) time zone,
%% <pre>
%% Types:
%%  Attributes = integer
%% </pre>
%% @spec encode_attributes(Attributes) -> ok
%% @end
%%--------------------------------------------------------------------
timestamp() ->
    {{_, _, _}, {_LocalHour, _LocalMin, _}} = LocalDateTime = calendar:local_time(),
    [{{Year, Month, Day}, {Hour, Min, Sec}}] = 
	calendar:local_time_to_universal_time_dst(LocalDateTime),
    Z = gmt_difference(),
    integer_to_list(Year) ++ "-" ++ two_digit(Month) ++ "-" ++ two_digit(Day) 
	++ "T" ++ two_digit(Hour) ++ ":" ++ two_digit(Min) ++ ":" ++ 
	two_digit(Sec) ++ Z.


%%--------------------------------------------------------------------
%% @doc gmt_difference -- TODO
%% <pre>
%% Types:
%% </pre>
%% @spec gmt_difference() -> string
%% @end
%%--------------------------------------------------------------------
gmt_difference() ->
    UTC = calendar:universal_time(),
    Local = calendar:universal_time_to_local_time(UTC),
    gmt_difference((calendar:datetime_to_gregorian_seconds(Local) -  calendar:datetime_to_gregorian_seconds(UTC)) / 3600).
gmt_difference(Diff) when Diff < 0 ->
    gmt_difference(-Diff, "-");
gmt_difference(Diff) ->
    gmt_difference(Diff, "+").

gmt_difference(Diff, Sign) when Diff < 10 ->
    gmt_difference1(float_to_list(Diff), Sign ++ "0");
gmt_difference(Diff, Sign) ->
    gmt_difference1(float_to_list(Diff), Sign).
gmt_difference1(StrDiff, SignZero) ->
    Index = string:chr(StrDiff, $.),
    SignZero ++ string:substr(StrDiff, 1, Index-1) ++ ":" ++ string:substr(StrDiff, Index+1, 2).


%%--------------------------------------------------------------------
%% @doc hmac
%% <pre>
%% Types:
%%  SecretKey = string
%%  Data = string
%% </pre>
%% @spec hmac(SecretKey, Data) -> string
%% @end
%%--------------------------------------------------------------------
hmac(SecretKey, Data) ->
    http_base_64:encode(
          binary_to_list(crypto:sha_mac(SecretKey, Data))).


%%--------------------------------------------------------------------
%% @doc xml_values retrieves node value from the XML records that are returned
%% after scanning tags.
%% <pre>
%% Types:
%%  SecretKey = string
%%  Data = string
%% </pre>
%% @spec gmt_difference() -> string
%% @end
%%--------------------------------------------------------------------
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

  
%%--------------------------------------------------------------------
%% @doc sleep 
%% <pre>
%% Types:
%%  SecretKey = string
%%  Data = string
%% </pre>
%% @spec timeout() -> true
%% @end
%%--------------------------------------------------------------------
sleep(T) ->
    receive
    after T ->
       true
    end.


%%--------------------------------------------------------------------
%% @doc url_encode - borrowed from CouchDB
%% <pre>
%% Types:
%%  String
%% </pre>
%% @spec url_encode(String) -> String
%% @end
%%--------------------------------------------------------------------
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


