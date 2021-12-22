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
    remote_timeout/1,
    node_interval/0,
    node_interval/1,
    default_cluster/0
]).

summarize() ->
    Fields = [
        {session_timeout, "Session Timeout", "minutes"},
        {grace_period, "Session Deletion Grace Period", "minutes"},
        {clear_interval, "Session Clear Interval", "minutes"},
        {interval_jitter, "Session Intentional Jitter", "minutes"},
        {remote_timeout, "Cluster Remote Timeout", "milliseconds"},
        {node_interval, "Refresh Node List Interval", "seconds"}
    ],

    Msgs = [format_config(Field) || Field <- Fields],
    Nodes = default_cluster(),
    ClusterMsg = format_cluster(Nodes),
    Msg = [
        "Nitrogen Canister Session Manager Configuration:\n",
        ClusterMsg,
        Msgs
    ],
    io:format(Msg).

format_cluster([]) ->
    "*** Canister: No auto-connected cluster nodes (default_cluster) configured. You'll have to manually connect nodes (net_kernel:connect_node(Node))\n";
format_cluster(Nodes) ->
    io_lib:format("*** Canister: Default auto-connected cluster nodes (default_cluster): ~p~n",[Nodes]).


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

node_interval() ->
    get_var(?FUNCTION_NAME, 20).

node_interval(New) ->
    get_var(?FUNCTION_NAME, New).

default_cluster() ->
    get_var(?FUNCTION_NAME, []).

get_var(Field, Default) ->
    get_var([canister], Field, Default).

get_var([], _Field, Default) ->
    Default;
get_var([App|Apps], Field, Default) ->
    case application:get_env(App, Field) of
        undefined -> get_var(Apps, Field, Default);
        {ok, X} -> X
    end.

set_var(Field, Value) ->
    application:set_env(canister, Field, Value).
