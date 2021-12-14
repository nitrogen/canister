%%%-------------------------------------------------------------------
%% @doc canister public API
%% @end
%%%-------------------------------------------------------------------

-module(canister_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    canister_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
