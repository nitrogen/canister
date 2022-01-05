-module(canister_resync).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    add/1,
    add_many/1,
    add_many/2,
    is_resyncing/1,
    is_resyncing/0,
    num_queued/0,
    num_queued/1,
    resync_loop/1
]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(REMOTE_TIMEOUT, canister_config:remote_timeout()).
-define(MOVE_QUEUE_INTERVAL, 1000 + rand:uniform(1000)).

-record(state, {resync_pid, queue}).

add(ID) ->
    gen_server:cast(?SERVER, {in, ID}).

num_queued(Node) ->
    try gen_server:call({?SERVER, Node}, num_queued, ?REMOTE_TIMEOUT)
    catch _:_ -> 0
    end.

num_queued() ->
    gen_server:call(?SERVER, num_queued).

is_resyncing(Node) ->
    try gen_server:call({?SERVER, Node}, is_resyncing, ?REMOTE_TIMEOUT)
    catch _:_ -> false
    end.

is_resyncing() ->
    gen_server:call(?SERVER, is_resyncing).

add_many(Node, IDs) ->
    canister_log:info("Adding ~p sessions to resync to be processed on ~p",[length(IDs), Node]),
    gen_server:call({?SERVER, Node}, {in_many, IDs}, ?REMOTE_TIMEOUT).

add_many(IDs) when is_list(IDs) ->
    canister_log:info("Adding ~p sessions to resync", [length(IDs)]),
    gen_server:call(?SERVER, {in_many, IDs}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    ResyncPid = start_resync_loop(),
    timer:send_interval(?MOVE_QUEUE_INTERVAL, maybe_move_queue),
    {ok, #state{resync_pid=ResyncPid, queue=queue:new()}}.

handle_call(num_queued, _From, State=#state{queue=Q}) ->
    Num = queue:len(Q),
    {reply, Num, State};
handle_call(is_resyncing, _From, State=#state{resync_pid=Pid}) ->
    Response = is_process_alive(Pid),
    {reply, Response, State};
handle_call({is_queued, ID}, _From, State=#state{queue=Q}) ->
    IsQueued = queue:member(ID, Q),
    {reply, IsQueued, State};
handle_call(out, _From, State = #state{resync_pid=_Pid, queue=Q}) ->
    case queue:out(Q) of
        {empty, _} ->
            {reply, empty, State};
        {{value, V}, NewQ} ->
            NewState = State#state{queue=NewQ},
            {reply, {ok, V}, NewState}
    end;
handle_call({in_many, IDs}, _From, State = #state{queue=Q}) ->
    canister_log:info("Received ~p Sessions to resync",[length(IDs)]),
    NewQ = lists:foldl(fun(ID, Acc) ->
        case queue:member(ID, Acc) of
            true -> Acc;
            false -> queue:in(ID, Acc)
        end
    end, Q, IDs),
    canister_log:info("New Queue Size: ~p",[queue:len(NewQ)]),
    NewState = State#state{queue=NewQ},
    {reply, ok, NewState}.

handle_cast({in, ID}, State = #state{queue=Q}) ->
    case queue:member(ID, Q) of
        true ->
            %% nothing to do, this item is already queued for resyncing
            {noreply, State};
        false -> 
            NewQ = queue:in(ID, Q),
            NewState = State#state{queue=NewQ},
            {noreply, NewState}
    end;
handle_cast({move_to_node, Node}, State=#state{queue=Q}) ->
    case queue:is_empty(Q) of
        true -> {noreply, State};
        false ->
            canister_log:info("Moving to Node: ~p",[Node]),
            List = queue:to_list(Q),
            NewQ = queue:new(),
            add_many(Node, List),
            NewState = State#state{queue=NewQ},
            {noreply, NewState}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

%% doing a little fancy matching with the pid variable, be aware
handle_info({'DOWN', _, _, Pid, Reason}, State = #state{resync_pid=Pid}) ->
    canister_log:info("Resync subprocess (~p) died with reason: ~p", [Pid, Reason]),
    NewPid = start_resync_loop(),
    NewState = State#state{resync_pid=NewPid},
    {noreply, NewState};
handle_info(maybe_move_queue, State = #state{queue=Q}) ->
    case queue:is_empty(Q) of
        true -> ok;
        false ->
            erlang:spawn(fun() ->
                Node = canister_sync:get_node_to_resync(),
                case Node==node() of
                    true -> ok;
                    false ->
                        gen_server:cast(?SERVER, {move_to_node, Node})
                end
            end)
    end,
    {noreply, State};
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_resync_loop() ->
    {ResyncPid, _} = erlang:spawn_monitor(fun() ->
        timer:sleep(1000),
        resync_loop(started)
    end),
    ResyncPid.


resync_loop(LastStatus) ->
    Status = case gen_server:call(?SERVER, out) of
        empty ->
            case LastStatus==empty of
                false ->
                    canister_log:info("Resync Queue Empty");
                true ->
                    ok
            end,
            timer:sleep(1000),
            empty;
        {ok, ID} ->
            canister_log:debug("Resyncing: ~p",[ID]),
            resync_worker(ID),
            running
    end,
    ?MODULE:resync_loop(Status).

resync_worker(ID) ->
    case canister_sync:get_nodes() of
        [] ->
            ok;
        Nodes ->
            {StartStatus, StartMTime, StartATime} = canister:record_status(ID),
            Me = node(),
            {FinalNode, FinalStatus, FinalMTime, FinalATime} = lists:foldl(fun(Node, {BestNode, BestStatus, BestMTime, BestATime}) ->
                {NewStatus, NewMTime, NewATime} = remote_record_status(Node, ID),
                NewBestATime = lists:max([NewATime, BestATime]),
                case NewMTime > BestMTime of
                    true -> {Node, NewStatus, NewMTime, NewBestATime};
                    false -> {BestNode, BestStatus, BestMTime, NewBestATime}
                end
            end, {Me, StartStatus, StartMTime, StartATime}, Nodes),
            case FinalStatus of
                deleted ->
                    canister_sync:send_clear(ID, FinalMTime);
                updated ->
                    case FinalNode of
                        Me ->
                            canister_sync:send_update(ID, FinalMTime);
                        _ ->
                            erpc:call(FinalNode, canister_sync, send_update, [ID, FinalMTime])
                    end,
                    canister:touch(ID, FinalATime);
                undefined ->
                    do_nothing
            end
    end.

remote_record_status(Node, ID) ->
    %% This needs to be optimized to call gen_server:call(something)
    try erpc:call(Node, canister, record_status, [ID], ?REMOTE_TIMEOUT)
    catch _:_ -> {undefined, 0, 0}
    end.
