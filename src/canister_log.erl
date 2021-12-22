-module(canister_log).
-export([info/1,
         info/2]).

info(Msg) ->
    info(Msg, []).

info(Msg, Args) ->
    io:format("Canister: " ++ Msg ++ "~n", Args).
