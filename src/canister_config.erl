-module(canister_config).

-export([
    summarize/0,
    session_timeout/0,
    session_timeout/1,
    grace_period/0,
    grace_period/1,
    clear_interval/0,
    clear_interval/1,
    interval_jitter/0,
    interval_jitter/1,
    remote_timeout/0,
    remote_timeout/1
]).


summarize() ->
    Fields = [
        {session_timeout, "Session Timeout", "minutes"},
        {grace_period, "Session Deletion Grace Period", "minutes"},
        {clear_interval, "Session Clear Interval", "minutes"},
        {interval_jitter, "Session Intentional Jitter", "minutes"},
        {remote_timeout, "Cluster Remote Timeout", "milliseconds"}
    ],

    Msgs = [format_config(Field) || Field <- Fields],
    Msg = [
        "Nitrogen Canister Session Manager Configuration:\n",
        Msgs
    ],
    io:format(Msg).

format_config({Field, Label, Units}) ->
    Val = ?MODULE:Field(),
    io_lib:format("*** Canister: ~s (~p): ~p ~s~n",[Label, Field, Val, Units]).


session_timeout() ->
    Apps = [canister, nitrogen_core, nitrogen],
    get_var(Apps, ?FUNCTION_NAME, 20).

session_timeout(New) ->
    set_var(?FUNCTION_NAME, New).

grace_period() ->
    get_var(?FUNCTION_NAME, 60).

grace_period(New) ->
    set_var(?FUNCTION_NAME, New).

clear_interval() ->
    get_var(?FUNCTION_NAME, 60).

clear_interval(New) ->
    set_var(?FUNCTION_NAME, New).

interval_jitter() ->
    get_var(?FUNCTION_NAME, 5).

interval_jitter(New) ->
    set_var(?FUNCTION_NAME, New).

remote_timeout() ->
    get_var(?FUNCTION_NAME, 2000). 

remote_timeout(New) ->
    set_var(?FUNCTION_NAME, New).



get_var(Field, Default) ->
    get_var([canister], Field, Default).

get_var([], _Field, Default) ->
    Default;
get_var([App|Apps], Field, Default) ->
    case application:get_env(App, Field) of
        X when is_integer(X) -> X;
        _ -> get_var(Apps, Field, Default)
    end.

set_var(Field, Value) ->
    application:set_env(canister, Field, Value).
