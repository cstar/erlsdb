%%%-------------------------------------------------------------------
%% @author Shahzad Bhatti <bhatti@plexobject.com> [http://bhatti.plexobject.com]
%% @doc debug helper
%%
%% == Contents ==
%%
%% {@section Introduction}<br/>
%% == Introduction ==
%%  DebugHelper adapted from debughelper from Rudolph van Graan 
%%  
%%
%% @copyright Shahzad Bhatti 2007
%% For license information see LICENSE.txt
%% 
%% @end
%%%-------------------------------------------------------------------
-module(debug_helper).

-export([start/0,
         trace/1,
         trace/2]).

start() ->
  dbg:tracer(),
  dbg:p(all,[c,sos,sol]).

trace(ModuleName) ->
  dbg:tpl(ModuleName,[{'_',[],[{message,{return_trace}}]}]).

trace(ModuleName,Function) ->
  dbg:tpl(ModuleName,Function,[{'_',[],[{message,{return_trace}}]}]).

%cprof:start(), cprof:pause(), cprof:analyze(mod), cprof:stop().
%erlc +debug_info *.erl, xref:d('.')

