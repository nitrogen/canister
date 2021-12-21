-module(canister_sync).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
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

-record(state, {nodes=[]}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_nodes() ->
    {ok, Nodes} = gen_server:call(?SERVER, get_nodes),
    Nodes.

send_update(ID, Data, Time) ->
    gen_server:cast({cast, {update, ID, Data, Time}}).

send_clear(ID, Time) ->
    gen_server:cast({cast, {clear, ID, Time}}).

send_touch(ID, Time) ->
    gen_server:cast({cast, {touch, ID, Time}}).

init([]) ->
    timer:send_interval(?SERVER, refresh_nodes),
    {ok, #state{nodes=[]}}.

handle_call(nodes, _From, State=#state{nodes=Nodes}) ->
    {reply, {ok, Nodes}, State};
handle_call(are_you_there, _From, State) ->
    {reply, yes, State};
handle_call({remote, Msg}, _From, State) ->
    Res = handle_remote(Msg),
    {reply, Res, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({update_nodes, NewNodes}, State = #state{nodes=OldNodes}) ->
    NewState = State#state{nodes=NewNodes},
    error_logger:info_msg("Nodes Updated: ~p => ~p",[OldNodes, NewNodes]),
    {noreply, NewState};
handle_cast({cast, Msg}, State = #state{nodes=Nodes}) ->
    erlang:spawn(fun() ->
        cast_to_nodes(Nodes, Msg)
    end),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(refresh_nodes, State = #state{nodes=Nodes}) ->
    erlang:spawn(fun() ->
        refresh_nodes(Nodes)
    end),
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
    canister:touch(ID, Time),
    ok;
handle_remote({clear, ID, Time}) ->
    canister:clear(ID, Time),
    ok.

cast_to_nodes(Nodes, Msg) ->
    ec_lists:foreach(fun(Node) ->
        Server = {?SERVER, Node},
        case gen_server:call(Server, {remote, Msg}, ?REMOTE_TIMEOUT) of
            ok ->
                ok;
            Other ->
                error_logger:warning_msg("Failed to send ~p to ~p~nResult: ~p",[Msg, Node, Other])
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
        

