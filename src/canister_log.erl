-module(canister_log).
-export([info/1,
         info/2,
         debug/1,
         debug/2]).

info(Msg) ->
    info(Msg, []).

info(Msg, Args) ->
    io:format("Canister: " ++ Msg ++ "~n", Args).


debug(_Msg) ->
    ok.

debug(_Msg, _Args) ->
    ok.
