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
-author("Eric Cestari <ecestari@mac.com> [http://www.cestari.info]").
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
	create_timestamp/0,
	xml_values/1,
	xml_values/2,
	xml_names_values/1,
	xml_names_values/2,
	parse_items/1,
	xml_int/1,
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

encode_attributes({Key, Value}, {Encoded, I}) ->
    KeyName = "Attribute." ++ integer_to_list(I) ++ ".Name",
    KeyValue = "Attribute." ++ integer_to_list(I) ++ ".Value",
    {[{KeyName, Key}, {KeyValue, Value}|Encoded], I+1}.


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
    {[{KeyName, Key}|Encoded], I+1}.




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
% lifted from http://code.google.com/p/erlawys/source/browse/trunk/src/aws_util.erl
create_timestamp() -> create_timestamp(calendar:now_to_universal_time(now())).
create_timestamp({{Y, M, D}, {H, Mn, S}}) ->
	to_str(Y) ++ "-" ++ to_str(M) ++ "-" ++ to_str(D) ++ "T" ++
	to_str(H) ++ ":" ++ to_str(Mn)++ ":" ++ to_str(S) ++ "Z".
add_zeros(L) -> if length(L) == 1 -> [$0|L]; true -> L end.
to_str(L) -> add_zeros(integer_to_list(L)).




%%--------------------------------------------------------------------
%% @doc xml_values retrieves node value from the XML records that are returned
%% after scanning tags.
%% <pre>
%% Types:
%%  SecretKey = string
%%  Data = string
%% </pre>
%%
%% @end
%%--------------------------------------------------------------------
xml_values(List) ->
    lists:foldr(fun xml_values/2, [], List).

xml_values(#xmlText{value=Value}, List) ->
    [Value|List].

%only use when you are sure there's only one value.
xml_int([#xmlText{value=Int}])->
    {ok,[Value],_}=io_lib:fread("~d", Int),
    Value.
    
xml_names_values(List) ->
    lists:foldr(fun xml_names_values/2, [], List).

xml_names_values(Xml, List) ->
    [ #xmlText{value=Name} ]  = xmerl_xpath:string("Name/text()", Xml),
    case xmerl_xpath:string("Value/text()", Xml) of
        [ #xmlText{value=Value} ] ->
            [{Name, Value}|List];
        [] -> [{Name, []}|List]
    end.

parse_items(XML) ->
  lists:foldr(fun(Item, Acc)->
       [ #xmlText{value=Name} ] = xmerl_xpath:string("Name/text()", Item),
       [{Name, xml_names_values(xmerl_xpath:string("Attribute", Item))}|Acc]
      end, [], xmerl_xpath:string("//Item", XML)).

    
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
%% @doc url_encode - lifted from the ever precious yaws_utils.erl    
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
        H == $_; H == $.; H == $~;H == $- -> % FIXME: more..
            [H|url_encode(T)];
        true ->
            case integer_to_hex(H) of
                [X, Y] ->
                    [$%, X, Y | url_encode(T)];
                [X] ->
                    [$%, $0, X | url_encode(T)]
            end
     end;

url_encode([]) ->
    [].
integer_to_hex(I) ->
    case catch erlang:integer_to_list(I, 16) of
        {'EXIT', _} ->
            old_integer_to_hex(I);
        Int ->
            Int
    end.

old_integer_to_hex(I) when I<10 ->
    integer_to_list(I);
old_integer_to_hex(I) when I<16 ->
    [I-10+$A];
old_integer_to_hex(I) when I>=16 ->
    N = trunc(I/16),
    old_integer_to_hex(N) ++ old_integer_to_hex(I rem 16).


