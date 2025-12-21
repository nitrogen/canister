-module(canister_sync).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    send_update/2,
    send_update/3,
    send_clear/2,
    send_touch/2,
    get_nodes/0,
    get_node_to_resync/0
]).

%-export([up/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(ETS_TABLE, canister_config).
-define(SERVER, ?MODULE).
-define(REMOTE_TIMEOUT, canister_config:remote_timeout()).
-define(NODE_INTERVAL, canister_config:node_interval() * 1000).

-record(state, {resync_timer}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_nodes() ->
    case ets:lookup(?ETS_TABLE, nodes) of
        [] -> [];
        [{_, Nodes}] -> Nodes
    end.

send_update(ID, Time) ->
    Data = canister:get_data(ID),
    send_update(ID, Data, Time).

send_update(ID, Data, Time) ->
    gen_server:cast(?SERVER, {cast, {update, ID, Data, Time}}).

send_clear(ID, Time) ->
    gen_server:cast(?SERVER, {cast, {clear, ID, Time}}).

send_touch(ID, Time) ->
    gen_server:cast(?SERVER, {cast, {touch, ID, Time}}).

init([]) ->
    ok = init_ets(),
    auto_connect_nodes(),
    timer:send_after(1, ?SERVER, refresh_nodes),
    timer:send_interval(?NODE_INTERVAL, ?SERVER, refresh_nodes),
    net_kernel:monitor_nodes(true),
    {ok, #state{}}.

init_ets() ->
    ETSConfig = [
        named_table,
        {read_concurrency, true},
        {write_concurrency, false},
        public
    ],
    try ets:new(?ETS_TABLE, ETSConfig) of
        _ -> ok
    catch
        error:badarg ->
            case lists:member(?ETS_TABLE, ets:all()) of
                false ->
                    {error, could_not_init_ets_for_canister};
                true ->
                    Owner = proplists:get_value(owner, ets:info(?ETS_TABLE)),
                    case Owner==self() of
                        true -> ok;
                        false -> {error, {count_not_init_ets_for_canister, table_exists_with_different_owner}}
                    end
            end
    end.

handle_call(uptime, _From, State) ->
    {Time, _} = erlang:statistics(wall_clock),
    {reply, Time, State};
handle_call(are_you_there, _From, State) ->
    {reply, yes, State};
handle_call({remote, Msg}, _From, State) ->
    Res = handle_remote(Msg),
    {reply, Res, State};
handle_call({last_access_time, ID}, _From, State) ->
    Val = canister:last_access_time(ID),
    {reply, {ok, Val}, State};
handle_call({last_update_time, ID}, _From, State) ->
    Val = canister:last_update_time(ID),
    {reply, {ok, Val}, State};
handle_call({deleted_time, ID}, _From, State) ->
    Val = canister:deleted_time(ID),
    {reply, {ok, Val}, State};
handle_call({get_local, ID, Key}, _From, State) ->
    Val = canister:get_local(ID, Key),
    {reply, Val, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({update_nodes, NewNodes}, State = #state{resync_timer=OldResyncTimer}) ->
    OldNodes = get_nodes(),
    true = ets:insert(?ETS_TABLE, {nodes, NewNodes}),
    canister_log:info("Nodes Updated: ~p => ~p",[OldNodes, NewNodes]),
    case NewNodes -- OldNodes of
        [] ->
            canister_log:info("No resync necessary. Node change was only from nodes going offline."),
            {noreply, State};
        NewlyUp ->
            canister_log:info("New node(s) were added (~p), scheduling a full resync in about 5 seconds", [NewlyUp]),
            canister_log:info("Canceling previous Resync Timer: ~p",[OldResyncTimer]),
            timer:cancel(OldResyncTimer),
            {ok, NewResyncTimer} = timer:send_after(5000 + rand:uniform(2000), full_resync),
            NewState = State#state{resync_timer=NewResyncTimer},
            {noreply, NewState}
    end;
handle_cast({cast, Msg}, State = #state{}) ->
    erlang:spawn(fun() ->
        Nodes = get_nodes(),
        cast_to_nodes(Nodes, Msg)
    end),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({nodeup, _Node}, State) ->
    handle_info(refresh_nodes, State);
handle_info({nodedown, _Node}, State) ->
    handle_info(refresh_nodes, State);
handle_info(refresh_nodes, State = #state{}) ->
    erlang:spawn(fun() ->
        Nodes = get_nodes(),
        refresh_nodes(Nodes)
    end),
    {noreply, State};
handle_info(full_resync, State = #state{}) ->
    case get_nodes() of
        [] -> do_nothing;
        Nodes ->
            erlang:spawn(fun() ->
                List = lists:sort([node() | Nodes]),
                assemble_and_requeue(List)
            end)
    end,
    NewState = State#state{resync_timer=undefined},
    {noreply, NewState};
handle_info(_, State) ->
    {noreply, State}.
    


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_remote({update, ID, Data, Time}) ->
    canister:update(ID, Data, Time),
    ok;
handle_remote({touch, ID, Time}) ->
    canister:touch_local(ID, Time),
    ok;
handle_remote({clear, ID, Time}) ->
    canister:clear(ID, Time),
    ok.

assemble_and_requeue(Nodes) ->
    case get_node_to_resync(Nodes) of
        undefined ->
            ok;
        MainNode ->
            canister_log:info("Will resync all on ~p",[MainNode]),
            IDSet = lists:foldl(fun(Node, Acc) ->
                try erpc:call(Node, canister, all_sessions, [], ?REMOTE_TIMEOUT) of
                    NewIDs ->
                        sets:union(Acc, sets:from_list(NewIDs))
                    catch _:_ ->
                        Acc
                end
            end, sets:new(), Nodes),
            IDs = sets:to_list(IDSet),
            canister_resync:add_many(MainNode, IDs)
    end.

get_node_to_resync() ->
    get_node_to_resync([node() | get_nodes()]).

get_node_to_resync(Nodes) ->
    case which_nodes_are_resyncing(Nodes) of
        [] -> hd(Nodes);
        NodeNums ->
            canister_log:info("Eligible Servers to Resync~nAll: ~p~nFound: ~p",[Nodes, NodeNums]),
            {FoundNode, _FoundNum} = lists:foldl(fun({Node, Num}, {BestNode, BestNum}) ->
                case Num > BestNum of
                    true -> {Node, Num};
                    false -> {BestNode, BestNum}
                end
            end, {node(), 0}, NodeNums),
            FoundNode
    end.


cast_to_nodes(Nodes, Msg) ->
    ec_plists:foreach(fun(Node) ->
        Server = {?SERVER, Node},
        case gen_server:call(Server, {remote, Msg}, ?REMOTE_TIMEOUT) of
            ok ->
                ok;
            Other ->
                canister_log:info("Failed to send ~p to ~p~nResult: ~p",[Msg, Node, Other])
        end
    end, Nodes).

refresh_nodes(OrigNodes) ->
    NewNodes = ec_plists:filter(fun(Node) ->
        is_node_up(Node) andalso is_node_canister_responding(Node)
    end, nodes()),
    Sorted = lists:sort(NewNodes),
    case Sorted==OrigNodes of
        true -> ok;
        false ->
            gen_server:cast(?SERVER, {update_nodes, Sorted})
    end.

which_nodes_are_resyncing(Nodes) ->
    lists:filtermap(fun(Node) ->
        case {canister_resync:is_resyncing(Node), canister_resync:num_queued(Node)} of
            {false, _} -> false;
            {true, 0} -> false;
            {true, Num} -> {true, {Node, Num}}
        end
    end, Nodes).

is_node_up(Node) ->
    net_adm:ping(Node)==pong.
        
is_node_canister_responding(Node) ->
    Server = {?SERVER, Node},
    gen_server:call(Server, are_you_there, 1000) == yes.

auto_connect_nodes() ->
    Nodes = canister_config:default_cluster(),
    auto_connect_nodes(Nodes).

auto_connect_nodes([]) ->
    [];
auto_connect_nodes([H|T]) ->
    case H==node() of
        true ->
            canister_log:info("Node (~p) is self(), so no need to connect", [H]);
        false ->
            case net_kernel:connect_node(H) of
                true ->
                    canister_log:info("Node (~p) successfully connected", [H]);
                false ->
                    canister_log:info("Node (~p) failed to connect", [H])
            end
    end,
    auto_connect_nodes(T).
