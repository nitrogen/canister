-module(canister_sync).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    send_update/2,
    send_update/3,
    send_clear/2,
    send_touch/2,
    get_nodes/0
]).

%-export([up/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(REMOTE_TIMEOUT, canister_config:remote_timeout()).
-define(NODE_INTERVAL, canister_config:node_interval() * 1000).

-record(state, {nodes=[], main_resync_node}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_nodes() ->
    {ok, Nodes} = gen_server:call(?SERVER, get_nodes),
    Nodes.

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
    auto_connect_nodes(),
    timer:send_after(1, ?SERVER, refresh_nodes),
    timer:send_interval(?NODE_INTERVAL, ?SERVER, refresh_nodes),
    net_kernel:monitor_nodes(true),
    {ok, #state{nodes=[]}}.

handle_call(uptime, _From, State) ->
    {Time, _} = erlang:statistics(wall_clock),
    {reply, Time, State};
handle_call(get_nodes, _From, State=#state{nodes=Nodes}) ->
    {reply, {ok, Nodes}, State};
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

handle_cast({update_nodes, NewNodes}, State = #state{nodes=OldNodes}) ->
    State2 = State#state{nodes=NewNodes},
    canister_log:info("Nodes Updated: ~p => ~p",[OldNodes, NewNodes]),
    case NewNodes -- OldNodes of
        [] ->
            canister_log:info("No resync necessary. Node change was only from nodes going offline.");
        NewlyUp ->
            canister_log:info("New node(s) were added (~p), scheduling a full resync in about 15 seconds", [NewlyUp]),
            timer:send_after(15000, full_resync)
    end,
    {noreply, State2};
handle_cast({cast, Msg}, State = #state{nodes=Nodes}) ->
    erlang:spawn(fun() ->
        cast_to_nodes(Nodes, Msg)
    end),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({nodeup, _Node}, State) ->
    handle_info(refresh_nodes, State);
handle_info({nodedown, _Node}, State) ->
    handle_info(refresh_nodes, State);
handle_info(refresh_nodes, State = #state{nodes=Nodes}) ->
    erlang:spawn(fun() ->
        refresh_nodes(Nodes)
    end),
    {noreply, State};
handle_info(full_resync, State = #state{nodes=[]}) ->
    %% no nodes in cluster, nothing to do
    {noreply, State};
handle_info(full_resync, State = #state{nodes=Nodes}) ->
    List = lists:sort([node() | Nodes]),
    Node = hd(List),
    erlang:spawn(fun() ->
        assemble_and_requeue(List)
    end),
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
    MainNode = hd(Nodes),
    IDSet = lists:foldl(fun(Node, Acc) ->
        try erpc:call(Node, canister, all_sessions, [], ?REMOTE_TIMEOUT) of
            NewIDs ->
                sets:union(Acc, sets:from_list(NewIDs))
            catch _:_ ->
                Acc
        end
    end, sets:new(), Nodes),
    IDs = sets:to_list(IDSet),
    canister_resync:add_many(IDs).


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
