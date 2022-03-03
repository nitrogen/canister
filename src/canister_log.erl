-module(canister_log).
-export([info/1,
         info/2,
         debug/1,
         debug/2]).

info(Msg) ->
    info(Msg, []).

info(Msg, Args) ->
    Args2 = [node() | Args],
    %FileMsg = lists:flatten(io_lib:format("(Node: ~p) Canister: " ++ Msg ++ "~n", Args2)),
    %file:write_file("canister.log", FileMsg, [append]),
    logger:info("Canister: (Node: ~p) " ++ Msg ++ "~n", Args2).


debug(_Msg) ->
    ok.

debug(_Msg, _Args) ->
    ok.
