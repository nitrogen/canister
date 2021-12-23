-module(canister_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    groups/0,
    init_per_group/2,
    end_per_group/2
]).

-export([
    basic_crud/1
]).


start_nodes(1) ->
    application:ensure_all_started(canister),
    [node()];
start_nodes(Num) when Num > 1 ->
    %% A bunch of this copied from https://github.com/ostinelli/ram/blob/main/test/ram_test_suite_helper.erl
    Name = list_to_atom("can_" ++ integer_to_list(Num) ++ "@127.0.0.1"),
    Node = Name,
    ct_slave:start(Name, [
        {boot_timeout, 10},
        {erl_flags, "-connect_all false -kernel dist_auto_connect never"}
    ]),

    true = net_kernel:connect_node(Name),

    CodePath = lists:filter(fun(Path) ->
        nomatch =:= string:find(Path, "rebar3")
    end, code:get_path()),
    true = rpc:call(Node, code, set_path, [CodePath]),

    {ok, _} = erpc:call(Node, application, ensure_all_started, [canister]),

    [Name | start_nodes(Num-1)].


kill_nodes([]) ->
    [];
kill_nodes([H|T]) when H=/=node() ->
    {ok, _} = ct_slave:stop(H),
    kill_nodes(T);
kill_nodes([H|T]) when H==node() ->
    kill_nodes(T).


all() ->
    [
        {group, '1'},
        {group, '2'},
        {group, '4'}
%        {group, two},
%        {group, three}
    ].


groups() ->
    [
        {'1',
            [shuffle], [basic_crud]
        },
        {'2',
            [shuffle], [basic_crud]
        },
        {'4',
            [shuffle], [basic_crud]
        }
    ].


group_to_num(Group) ->
    list_to_integer(atom_to_list(Group)).

init_per_group(Group, Config) ->
    error_logger:info_msg("Master Node: ~p",[node()]),
    NumNodes = group_to_num(Group),
    Nodes = start_nodes(NumNodes),
    error_logger:info_msg("Sleeping for about 30 few seconds to allow the newly spawned servers to normalize, resync, etc"),
    timer:sleep(30000),
    [{nodes, Nodes} | Config].

end_per_group(_Group, Config) ->
    Nodes = proplists:get_value(nodes, Config),
    kill_nodes(Nodes),
    Config.

basic_crud(Config) ->
    Sessions = rand_sessions(),
    error_logger:info_msg("Generated ~p Sessions",[length(Sessions)]),
    ok = store_sessions(Sessions),
    Nodes = proplists:get_value(nodes, Config),
    ok = check_sessions(Nodes, Sessions).


store_sessions([]) ->
    ok;
store_sessions([{ID, KV} | Rest]) ->
    lists:foreach(fun({Key, Val}) ->
        undefined = canister:put(ID, Key, Val)
    end, KV),
    store_sessions(Rest).

check_sessions(_, []) ->
    ok;
check_sessions(Nodes, [{ID, KV} | Rest]) ->
    ok = check_session(Nodes, ID, KV),
    check_sessions(Nodes, Rest).

check_session(_, _, []) ->
    ok;
check_session(Nodes, ID, [{K, V} | RestKV]) ->
    lists:foreach(fun
        (Node) when Node==node() ->
            V = canister:get(ID, K);
        (Node) ->
            V = erpc:call(Node, canister, get, [ID, K])
    end, Nodes),
    check_session(Nodes, ID, RestKV).

rand_sessions() ->
    rand_sessions(1000).

rand_sessions(Num) ->
    lists:map(fun(_) ->
        ID = crypto:strong_rand_bytes(32),
        NumKeys = rand:uniform(20),
        KV = lists:map(fun(_) ->
            RandKey = crypto:strong_rand_bytes(16),
            RandVal = rand_value(),
            {RandKey, RandVal}
        end, lists:seq(1, NumKeys)),
        KV2 = sets:to_list(sets:from_list(KV)),
        {ID, KV2}
    end, lists:seq(1, Num)).

rand_value() ->
    rand_value(rand:uniform(5)).

rand_value(1) -> %% integer
    rand:uniform(1000000000000000000000000000000);
rand_value(2) ->  %% binary
    crypto:strong_rand_bytes(rand:uniform(1000));
rand_value(3) -> %% list
    lists:seq(1, rand:uniform(1000));
rand_value(4) -> %% tuple
    list_to_tuple(lists:seq(1, rand:uniform(1000)));
rand_value(5) -> %% mixed item
    {rand_value(1), rand_value(2), rand_value(3), rand_value(4)}.
