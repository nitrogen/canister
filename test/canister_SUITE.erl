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

%Rework this whole module to use peer:start(#{connection=>standard_io})
%Look at this: https://max-au.com/2022/01/06/peer/

start_nodes(NumNodes) ->
    [start_node(Num) || Num <- lists:seq(1, NumNodes)].

start_node(_) ->
    Opts = #{
        wait_boot => 5000,
        name => peer:random_name(),
        host => "127.0.0.1",
        connection => standard_io
    },
    {ok, Peer, Node} = ?CT_PEER(Opts),
    unlink(Peer),
    io:format("Peer: ~p. Node: ~p~n",[Peer, Node]),

    %true = net_kernel:connect_node(Node),

    CodePath = lists:filter(fun(Path) ->
        nomatch =:= string:find(Path, "rebar3")
    end, code:get_path()),
    %peer:
    true = peer:call(Peer, code, set_path, [CodePath]),

    %{ok, _} = erpc:call(Node, application, ensure_all_started, [canister]),
    {ok, _} = peer:call(Peer, application, ensure_all_started, [canister]),
    {Peer, Node}.

%start_unconnected(Num) ->
%    Node = start_unconnected(Num),
%    erlang:disconnect_node(Node),
%    Node.

%kill_peers([undefined|T]) ->
%    application:stop(canister),
%    kill_peers(T);
kill_peers([H|T]) ->
    peer:stop(H),
    kill_peers(T);
kill_peers([]) ->
    ok.
%    if Peer == undefined, this is the  current node, just stop canister
%    [peer:stop(Peer) || Peer <- Peers].
%%    {ok, _} = ct_slave:stop(H),
%%    kill_peers(T).
%kill_peers([H|T]) when H==node() ->
%    %mnesia:delete_table(canister
%    %stopped = mnesia:stop(),
%    %mnesia:delete_schema(canister),
%    kill_peers(T).


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
    PeerNodes = start_nodes(NumNodes),
    {Peers, Nodes} = lists:unzip(PeerNodes),
    sleep_and_show_sync_status(Peers, Nodes, 1, 30),
    [{peer_nodes, PeerNodes}, {nodes, Nodes}, {peers, Peers} | Config].

end_per_group(_Group, Config) ->
    Peers = proplists:get_value(peers, Config),
    kill_peers(Peers),
    Config.

no_session(Config) ->
    ID = crypto:strong_rand_bytes(50),
    Key = crypto:strong_rand_bytes(50),
    undefined = exec_peer(Config, canister, get, [ID, Key]).
    %undefined = canister:get(ID, Key).

no_key(Config) ->
    Sessions = rand_sessions(1),
    ok = store_sessions(Config, Sessions),
    [{ID, _}] = Sessions,
    Key = crypto:strong_rand_bytes(50),
    undefined = exec_peer(Config, canister, get, [ID, Key]).
    %undefined = canister:get(ID, Key).

basic_crud(Config) ->
    Sessions = rand_sessions(),
    io:format("Generated ~p Sessions~n",[length(Sessions)]),
    ok = store_sessions(Config, Sessions),
    %Nodes = proplists:get_value(nodes, Config),
    Peers = proplists:get_value(peers, Config),
    %Peers = proplists:get_value(peers, Config),
    print_local_sessions(Peers),
    ok = check_sessions_dist(Peers, Sessions),
    ok = check_sessions_local(Peers, Sessions).

add_one_node(Config) ->
    add_x_nodes(1, Config).

add_three_nodes(Config) ->
    add_x_nodes(3, Config).

add_x_nodes(Num, Config) ->
    Sessions = rand_sessions(),
    ok = store_sessions(Config, Sessions),
    Nodes = proplists:get_value(nodes, Config),
    Peers = proplists:get_value(peers, Config),
    PeerNodes = add_new_nodes(Num),
    {NewPeers, NewNodes} = lists:unzip(PeerNodes),
    Nodes2 = Nodes ++ NewNodes,
    Peers2 = Peers ++ NewPeers,
    sleep_and_show_sync_status(Peers2, Nodes2, 1, 30*Num),
    ok = check_sessions_local(Peers2, Sessions).

netsplit(Config) ->
    Sessions = rand_sessions(),
    ok = store_sessions(Config, Sessions),
    Nodes = proplists:get_value(nodes, Config),
    Peers = proplists:get_value(peers, Config),
    [FirstSession,SecondSession|_] = Sessions,
    {ID1, KV1} = FirstSession,
    {Key1, _} = hd(KV1),

    FirstAccess1 = exec_first(Config, canister, last_access_time, [ID1]),
    %FirstAccess1 = canister:last_access_time(ID1),
    {ID2, KV2} = SecondSession,
    {Key2, _} = hd(KV2),
    FirstUpdate2 = exec_first(Config, canister, last_update_time, [ID2]),
    %FirstUpdate2 = canister:last_update_time(ID2),


    DownNode = lists:last(Nodes),
    exec_first(Config, erlang, disconnect_node, [DownNode]),
    %erlang:disconnect_node(DownNode),
    exec_first(Config, canister, get, [ID1, Key1]),
    %canister:get(ID1, Key1),
    NextAccess1 = exec_first(Config, canister, last_access_time, [ID1]),
    %NextAccess1 = canister:last_access_time(ID1),

    NewV2 = new_value,
    FirstUpdate2 = exec_first(Config, canister, last_update_time, [ID2]),
    %FirstUpdate2 = canister:last_update_time(ID2),

    %canister:put(ID2, Key2, NewV2),
    exec_first(Config, canister, put, [ID2, Key2, NewV2]),

    %NextUpdate2 = canister:last_update_time(ID2),
    NextUpdate2 = exec_first(Config, canister, last_update_time,[ID2]),

    exec_first(Config, net_kernel, connect_node, [DownNode]),
    %net_kernel:connect_node(DownNode),
    %
    sleep_and_show_sync_status(Peers, Nodes, 1, 30),


    SyncedAccessTime1 = exec_last(Config, canister, last_access_time, [ID1]),

    print_times(Config, "Access Times", FirstAccess1, NextAccess1, SyncedAccessTime1),

    SyncedAccessTime1 > FirstAccess1 orelse exit(access_time_unchanged),
    SyncedAccessTime1 == NextAccess1 orelse exit({access_time_not_synced, [SyncedAccessTime1, NextAccess1]}),

    SyncedUpdateTime2 = exec_last(Config, canister, last_update_time, [ID2]),

    print_times(Config, "Update Times", FirstUpdate2, NextUpdate2, SyncedUpdateTime2),

    SyncedUpdateTime2 > FirstUpdate2 orelse exit(update_time_unchanged),
    SyncedUpdateTime2 == NextUpdate2 orelse exit({update_time_not_synced, [SyncedUpdateTime2, NextUpdate2]}),

    SyncedNewV2 = exec_last(Config, canister, get_local, [ID2, Key2]),

    SyncedNewV2==NewV2 orelse exit({updated_value_not_synced, [NewV2, SyncedNewV2]}).


print_times(Config, Label, Orig, New, OnTargetNode) ->
    LogArgs = ["~s:~nOrig: ~p~nNew: ~p~nPost-resynced on target node: ~p", [Label, Orig, New, OnTargetNode]],
    exec_first(Config, canister_log, info, LogArgs).

sleep_and_show_sync_status(Peers, Nodes, Secs, Times) ->
    print_local_sessions(Peers),
    print_running_resync(Nodes, Peers),
    lists:foreach(fun(X) ->
        canister_log:info("Sleeping ~ps (~p/~p)~n",[Secs, X, Times]),
        timer:sleep(Secs * 1000),
        print_local_sessions(Peers),
        print_running_resync(Nodes, Peers)
    end, lists:seq(1, Times)).

print_local_sessions(Peers) ->
    lists:foreach(fun(Peer) ->
        N = peer:call(Peer, canister, num_local_sessions, []),
        %N = erpc:call(, canister, num_local_sessions, []),
        canister_log:info("Number of Local Session on ~p: ~p",[Peer, N])
    end, Peers).

print_running_resync(Nodes, [Peer|_]) ->
    lists:foreach(fun(Node) ->
        Syncing = peer:call(Peer, canister_resync, is_resyncing, [Node]),
        %Syncing = canister_resync:is_resyncing(Node),
        Queued = peer:call(Peer, canister_resync, num_queued, [Node]),
        %Queued = canister_resync:num_queued(Node),
        canister_log:info("Node (~p) Resyncing: ~p (~p queued)",[Node, Syncing, Queued])
    end, Nodes).

add_new_nodes(NumNodes) ->
    [start_node(Num+100) || Num <- lists:seq(1, NumNodes)].


store_sessions(_Config, []) ->
    ok;
store_sessions(Config, [{ID, KV} | Rest]) ->
    lists:foreach(fun({Key, Val}) ->
        undefined = exec_peer(Config, canister, put, [ID, Key, Val])
    end, KV),
    store_sessions(Config, Rest).

check_sessions_local(Peers, Sessions) ->
    check_sessions(get_local, Peers, Sessions).

check_sessions_dist(Peers, Sessions) ->
    check_sessions(get, Peers, Sessions).

check_sessions(_Fun, _, []) ->
    ok;
check_sessions(Fun, Peers, [{ID, KV} | Rest]) ->
    NumLeft = length(Rest),
    case NumLeft rem 100 of
        0 -> canister_log:info("~p Session Checks Remaining",[NumLeft]);
        _ -> ok
    end,
    ok = check_session(Fun, Peers, ID, 1, KV),
    check_sessions(Fun, Peers, Rest).

check_session(_, _, _, _, []) ->
    ok;
check_session(Fun, Peers, ID, I, [{K, V} | RestKV]) ->
    lists:foreach(fun(Peer) ->
        case get_val(Peer, Fun, ID, K) of
            V -> ok;
            Other ->
                exit({failed_lookup, [
                    {peer, Peer},
                    {function, Fun},
                    {iteration,I},
                    {id, ID},
                    {key, K},
                    {expected_value, V},
                    {returned_value, Other}
                ]})
        end
    end, Peers),
    check_session(Fun, Peers, ID, I+1, RestKV).

%get_val(Peer, Fun, ID, K) when Node==node() ->
%    canister:Fun(ID, K);
get_val(Peer, Fun, ID, K) ->
    peer:call(Peer, canister, Fun, [ID, K]).
    %erpc:call(Node, canister, Fun, [ID, K]).


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

exec_peer(Config, Mod, Fun, Args) ->
    Peers = proplists:get_value(peers, Config),
    Peer = rand_item(Peers),
    peer:call(Peer, Mod, Fun, Args).

exec_first(Config, Mod, Fun, Args) ->
    Peers = proplists:get_value(peers, Config),
    Peer = hd(Peers),
    peer:call(Peer, Mod, Fun, Args).

exec_last(Config, Mod, Fun, Args) ->
    Peers = proplists:get_value(peers, Config),
    Peer = lists:last(Peers),
    peer:call(Peer, Mod, Fun, Args).

rand_item([X]) ->
    X;
rand_item(List) ->
    Max = length(List),
    I = rand:uniform(Max),
    lists:nth(I, List).
     

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
