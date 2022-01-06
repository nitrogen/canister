-module(canister_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    groups/0,
    init_per_group/2,
    end_per_group/2
]).

-export([
    basic_crud/1,
    add_one_node/1,
    add_three_nodes/1,
    no_session/1,
    no_key/1,
    netsplit/1
]).


start_nodes(NumNodes) ->
    [start_node(Num) || Num <- lists:seq(1, NumNodes)].

start_node(1) ->
    application:ensure_all_started(canister),
    node();
start_node(Num) ->
    Name = list_to_atom("can_" ++ integer_to_list(Num) ++ "@127.0.0.1"),
    Node = Name,
    ct_slave:start(Name, [
        {boot_timeout, 10}
        %{erl_flags, "-connect_all true -kernel dist_auto_connect never"}
    ]),

    true = net_kernel:connect_node(Name),

    CodePath = lists:filter(fun(Path) ->
        nomatch =:= string:find(Path, "rebar3")
    end, code:get_path()),
    true = rpc:call(Node, code, set_path, [CodePath]),

    {ok, _} = erpc:call(Node, application, ensure_all_started, [canister]),
    Node.

start_unconnected(Num) ->
    Node = start_unconnected(Num),
    erlang:disconnect_node(Node),
    Node.

kill_nodes([]) ->
    [];
kill_nodes([H|T]) when H=/=node() ->
    {ok, _} = ct_slave:stop(H),
    kill_nodes(T);
kill_nodes([H|T]) when H==node() ->
    %mnesia:delete_table(canister
    application:stop(canister),
    %stopped = mnesia:stop(),
    %mnesia:delete_schema(canister),
    kill_nodes(T).


all() ->
    [
        {group, '1'},
        {group, '2'},
        {group, '3'},
        {group, '4'}
    ].


groups() ->
    [
        {'1',
            [shuffle], [no_session, no_key, basic_crud]
        },
        {'2',
            [shuffle], [basic_crud, add_one_node, no_session, no_key]
        },
        {'3',
            [shuffle], [netsplit]
        },
        {'4',
            [shuffle], [
                %basic_crud,
                add_three_nodes]
        }
    ].


group_to_num(Group) ->
    list_to_integer(atom_to_list(Group)).

init_per_group(Group, Config) ->
    %error_logger:info_msg("Master Node: ~p",[node()]),
    NumNodes = group_to_num(Group),
    Nodes = start_nodes(NumNodes),
    sleep_and_show_sync_status(Nodes, 1, 30),
    [{nodes, Nodes} | Config].

end_per_group(_Group, Config) ->
    Nodes = nodes() ++ [node()], %proplists:get_value(nodes, Config),
    kill_nodes(Nodes),
    Config.

no_session(_Config) ->
    ID = crypto:strong_rand_bytes(50),
    Key = crypto:strong_rand_bytes(50),
    undefined = canister:get(ID, Key).

no_key(_Config) ->
    Sessions = rand_sessions(1),
    ok = store_sessions(Sessions),
    [{ID, _}] = Sessions,
    Key = crypto:strong_rand_bytes(50),
    undefined = canister:get(ID, Key).

basic_crud(Config) ->
    Sessions = rand_sessions(),
    io:format("Generated ~p Sessions~n",[length(Sessions)]),
    ok = store_sessions(Sessions),
    Nodes = proplists:get_value(nodes, Config),
    print_local_sessions(Nodes),
    ok = check_sessions_dist(Nodes, Sessions),
    ok = check_sessions_local(Nodes, Sessions).

add_one_node(Config) ->
    add_x_nodes(1, Config).

add_three_nodes(Config) ->
    add_x_nodes(3, Config).

add_x_nodes(Num, Config) ->
    Sessions = rand_sessions(),
    ok = store_sessions(Sessions),
    Nodes = proplists:get_value(nodes, Config),
    NewNodes = add_new_nodes(Num),
    Nodes2 = Nodes ++ NewNodes,
    sleep_and_show_sync_status(Nodes2, 1, 30*Num),
    ok = check_sessions_local(Nodes2, Sessions).

netsplit(Config) ->
    Sessions = rand_sessions(),
    ok = store_sessions(Sessions),
    Nodes = proplists:get_value(nodes, Config),
    [FirstSession,SecondSession|_] = Sessions,
    {ID1, KV1} = FirstSession,
    {Key1, _} = hd(KV1),
    FirstAccess1 = canister:last_access_time(ID1),
    {ID2, KV2} = SecondSession,
    {Key2, _} = hd(KV2),
    FirstUpdate2 = canister:last_update_time(ID2),

    DownNode = lists:last(Nodes),
    erlang:disconnect_node(DownNode),
    canister:get(ID1, Key1),
    NextAccess1 = canister:last_access_time(ID1),

    NewV2 = new_value,
    FirstUpdate2 = canister:last_update_time(ID2),

    canister:put(ID2, Key2, NewV2),
    NextUpdate2 = canister:last_update_time(ID2),
    
    net_kernel:connect_node(DownNode),
    sleep_and_show_sync_status(Nodes, 1, 30),


    SyncedAccessTime1 = erpc:call(DownNode, canister, last_access_time, [ID1]),

    print_times("Access Times", FirstAccess1, NextAccess1, SyncedAccessTime1),

    SyncedAccessTime1 > FirstAccess1 orelse exit(access_time_unchanged),
    SyncedAccessTime1 == NextAccess1 orelse exit({access_time_not_synced, [SyncedAccessTime1, NextAccess1]}),

    SyncedUpdateTime2 = erpc:call(DownNode, canister, last_update_time, [ID2]),

    print_times("Update Times", FirstUpdate2, NextUpdate2, SyncedUpdateTime2),

    SyncedUpdateTime2 > FirstUpdate2 orelse exit(update_time_unchanged),
    SyncedUpdateTime2 == NextUpdate2 orelse exit({update_time_not_synced, [SyncedUpdateTime2, NextUpdate2]}),

    SyncedNewV2 = erpc:call(DownNode, canister, get_local, [ID2, Key2]),

    SyncedNewV2==NewV2 orelse exit({updated_value_not_synced, [NewV2, SyncedNewV2]}).


print_times(Label, Orig, New, OnTargetNode) ->
    canister_log:info("~s:~nOrig: ~p~nNew: ~p~nPost-resynced on target node: ~p", [Label, Orig, New, OnTargetNode]).

sleep_and_show_sync_status(Nodes, Secs, Times) ->
    print_local_sessions(Nodes),
    print_running_resync(Nodes),
    lists:foreach(fun(X) ->
        canister_log:info("Sleeping ~ps (~p/~p)~n",[Secs, X, Times]),
        timer:sleep(Secs * 1000),
        print_local_sessions(Nodes),
        print_running_resync(Nodes)
    end, lists:seq(1, Times)).
        

print_local_sessions(Nodes) ->
    lists:foreach(fun(Node) ->
        N = erpc:call(Node, canister, num_local_sessions, []),
        canister_log:info("Number of Local Session on ~p: ~p",[Node, N])
    end, Nodes).

print_running_resync(Nodes) ->
    lists:foreach(fun(Node) ->
        Syncing = canister_resync:is_resyncing(Node),
        Queued = canister_resync:num_queued(Node),
        canister_log:info("Node (~p) Resyncing: ~p (~p queued)",[Node, Syncing, Queued])
    end, Nodes).

add_new_nodes(NumNodes) ->
    [start_node(Num+100) || Num <- lists:seq(1, NumNodes)].


store_sessions([]) ->
    ok;
store_sessions([{ID, KV} | Rest]) ->
    lists:foreach(fun({Key, Val}) ->
        undefined = canister:put(ID, Key, Val)
    end, KV),
    store_sessions(Rest).

check_sessions_local(Nodes, Sessions) ->
    check_sessions(get_local, Nodes, Sessions).

check_sessions_dist(Nodes, Sessions) ->
    check_sessions(get, Nodes, Sessions).

check_sessions(_Fun, _, []) ->
    ok;
check_sessions(Fun, Nodes, [{ID, KV} | Rest]) ->
    NumLeft = length(Rest),
    case NumLeft rem 100 of
        0 -> canister_log:info("~p Session Checks Remaining",[NumLeft]);
        _ -> ok
    end,
    ok = check_session(Fun, Nodes, ID, 1, KV),
    check_sessions(Fun, Nodes, Rest).

check_session(_, _, _, _, []) ->
    ok;
check_session(Fun, Nodes, ID, I, [{K, V} | RestKV]) ->
    lists:foreach(fun(Node) ->
        case get_val(Node, Fun, ID, K) of
            V -> ok;
            Other ->
                exit({failed_lookup, [
                    {node, Node},
                    {function, Fun},
                    {iteration,I},
                    {id, ID},
                    {key, K},
                    {expected_value, V},
                    {returned_value, Other}
                ]})
        end
    end, Nodes),
    check_session(Fun, Nodes, ID, I+1, RestKV).

get_val(Node, Fun, ID, K) when Node==node() ->
    canister:Fun(ID, K);
get_val(Node, Fun, ID, K) ->
    erpc:call(Node, canister, Fun, [ID, K]).


print_check(Node, Fun, ID, Key, ExpectedVal) ->
    error_logger:info_msg("Checking (~p) canister:~p(~p, ~p). Expecting: ~p",[Node, Fun, ID, Key, ExpectedVal]).


rand_sessions() ->
    rand_sessions(10000).

rand_sessions(Num) ->
    lists:map(fun(_) ->
        ID = crypto:strong_rand_bytes(16),
        NumKeys = rand:uniform(4),
        KV = lists:map(fun(_) ->
            RandKey = crypto:strong_rand_bytes(8),
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
    crypto:strong_rand_bytes(rand:uniform(20));
rand_value(3) -> %% list
    lists:seq(1, rand:uniform(20));
rand_value(4) -> %% tuple
    list_to_tuple(lists:seq(1, rand:uniform(20)));
rand_value(5) -> %% mixed item
    {rand_value(1), rand_value(2), rand_value(3), rand_value(4)}.
