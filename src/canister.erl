-module(canister).
-include("canister.hrl").
-include_lib("stdlib/include/qlc.hrl").
-export([
    all_sessions/0,
    start/0,
    clear/1,
    delete/1,
    get/2,
    put/3,
    touch/1,
    touch/2,
    last_update_time/1,
    last_access_time/1,
    clear_untouched_sessions/0,
    delete_deleted_sessions/0
]).
-compile(export_all).


-define(REMOTE_TIMEOUT, canister_config:remote_timeout()).
-define(SESSION_TIMEOUT, canister_config:session_timeout()).
-define(GRACE_PERIOD, canister_config:grace_period()).

all_sessions() ->
    {atomic, Res} = mnesia:transaction(fun() ->
        mnesia:all_keys(canister_times)
    end),
    Res.
    %[integerize(S) || S <- Res].

init_tables() ->
    init_table(canister_data, record_info(fields, canister_data)),
    init_table(canister_times, record_info(fields, canister_times)).

init_table(Table, Fields) ->
    Res = mnesia:create_table(Table, [
        {disc_copies, [node()]},
        {attributes, Fields}
    ]),
    error_logger:info_msg("Initializing Canister Table: ~p: ~p",[Table, Res]),
    Res.

schema() ->
    case mnesia:create_schema([node()]) of
        ok -> ok;
        {error, {_, {already_exists, _}}} -> ok;
        Other -> exit({failed_to_init_schema, Other})
    end,
    mnesia:start().

start() ->
    schema(),
    init_tables(),
    canister_config:summarize().

maybe_wrap_transaction(Fun) ->
    case mnesia:is_transaction() of
        true ->
            Fun();
        false ->
            {atomic, Res} = mnesia:transaction(Fun),
            Res
    end.

write(Rec) ->
    case mnesia:is_transaction() of
        true -> mnesia:write(Rec);
        false -> mnesia:dirty_write(Rec)
    end.

read(Table, ID) ->
    Res = case mnesia:is_transaction() of
        true -> mnesia:read(Table, ID);
        false -> mnesia:dirty_read(Table, ID)
    end,
    case Res of
        [X] -> X;
        [] -> undefined
    end.

clear(ID) ->
    Time = os:timestamp(),
    clear(ID, Time),
    canister_sync:send_clear(ID).

clear(ID, Time) ->
    maybe_wrap_transaction(fun() ->
        case read(canister_data, ID) of
            #canister_data{} ->
                C = #canister_data{id=ID},
                write(C),
                update_delete_time(ID, Time),
                ok;
            _ ->
                ok
        end
    end).

delete(ID) ->
    maybe_wrap_transaction(fun() ->
        mnesia:delete({canister_data, ID}),
        mnesia:delete({canister_times, ID})
    end).

    

data_get(Key, Data) ->
    case maps:find(Key, Data) of
        {ok, V} -> V;
        error -> undefined
    end.

get(ID, Key) ->
    case read(canister_data, ID) of
        #canister_data{data=Data} ->
            touch(Key),
            data_get(Key, Data);
        _ ->
            undefined
    end.
    
put(ID, Key, Value) ->
    maybe_wrap_transaction(fun() ->
        Rec = case read(canister_data, ID) of
            S = #canister_data{} ->
                S;
            _ ->
                #canister_data{id=ID}
        end,
        Data = Rec#canister_data.data,
        Prev = data_get(Key, Data),
        NewData = maps:put(Key, Value, Rec#canister_data.data),
        NewRec = Rec#canister_data{data=NewData},
        write(NewRec),
        Time = update_update_time(ID),
        canister_sync:send_update(ID, NewData, Time),
        Prev
    end).

%% This is coming from remote data
update(ID, Data, Time) ->
    maybe_wrap_transaction(fun() ->
        DataRec = #canister_data{id=ID, data=Data},
        write(DataRec),
        update_update_time(ID, Time)
    end).

touch(ID) ->
    Time = update_access_time(ID),
    canister_sync:queue_touch(ID, Time).

touch(ID, Time) ->
    update_access_time(ID, Time).

queue_delete(ID, Time) ->
    canister_sync:send_delete(ID, Time).

resync(ID) ->
    ok.

update_update_time(ID) ->
    update_update_time(ID, os:timestamp()).

update_update_time(ID, Time) ->
    Rec = #canister_times{
        id=ID,
        last_access=Time,
        last_update=Time
    },
    write(Rec),
    Time.

update_delete_time(ID) ->
    update_delete_time(ID, os:timestamp()).

update_delete_time(ID, Time) ->
    Rec = #canister_times{
        id=ID,
        last_access=undefined,
        last_update=undefined,
        deleted=Time
    },
    write(Rec),
    Time.


last_access_time(ID) ->
    case read(canister_times, ID) of
        T = #canister_times{last_access=T} -> T;
        undefined -> undefined
    end.

last_update_time(ID) ->
    case read(canister_times, ID) of
        T = #canister_times{last_update=T} -> T;
        undefined -> undefined
    end.

update_access_time(ID) ->
    update_access_time(ID, os:timestamp()).

update_access_time(ID, Time) ->
    maybe_wrap_transaction(fun() ->
        case read(canister_times, ID) of
            T = #canister_times{} ->
                New = T#canister_times{last_access=Time},
                write(New),
                Time;
            undefined ->
                ok
        end
    end).
         
list_untouched_sessions() ->
    Timeout = canister_config:session_timeout(),
    EffectiveTimeout = Timeout + ?GRACE_PERIOD,
    LastAccessedToExpire = qdate:to_now(qdate:add_minutes(-EffectiveTimeout)),
    Query = fun() ->
        qlc:eval(qlc:q(
            [{Rec#canister_times.id, Rec#canister_times.last_access} || Rec <- mnesia:table(canister_times),
                                             Rec#canister_times.last_access < LastAccessedToExpire]
        ))
    end,
    {atomic, Res} = mnesia:transaction(Query),
    Res.


clear_untouched_sessions() ->
    Sessions = list_untouched_sessions(),
    SessionsToSync = lists:filtermap(fun(Sess) ->
        case clear_untouched_session(Sess) of
            ok -> false;
            {resync, _Node} -> {true, Sess#canister_times.id}
        end
    end, Sessions),
    lists:foreach(fun(ID) ->
        resync(ID)
    end, SessionsToSync).


clear_untouched_session({ID, LastAccess}) ->
    case latest_cluster_access_time(ID, LastAccess) of
        ok ->
            clear(ID);
        {ok, LastAccess, Node} ->
            {resync, Node}
    end.

latest_cluster_access_time(ID, LastAccess) ->
    case canister_sync:get_nodes() of
        [] -> ok;
        Nodes ->
            case compare_latest_node_access_time(Nodes, ID, LastAccess) of
                undefined ->
                    ok;
                {Node, NewLastAccess} ->
                    {ok, NewLastAccess, Node}
            end
    end.

compare_latest_node_access_time(Nodes, ID, LastAccess) ->
    Me = node(),
    {FinalNode, FinalAccess} = lists:foldl(fun(Node, {BestNode, BestAccess}) ->
        NodeLatest = latest_node_access_time(Node, ID),
        case NodeLatest > BestAccess of
            true -> {Node, NodeLatest};
            false -> {BestNode, BestAccess}
        end
    end, {Me, LastAccess}, Nodes),

    case FinalNode of
        Me -> ok;
        _ ->
            {ok, FinalNode, FinalAccess}
    end.

latest_node_access_time(Node, ID) ->
    try erpc:call(Node, ?MODULE, last_access_time, [ID], ?REMOTE_TIMEOUT)
    catch _:_:_ -> 0
    end.


delete_deleted_sessions() ->
    %% This just makes sure we don't delete sessions that were incorrectly deleted during a netsplit event
    BeforeTime = qdate:to_now(qdate:add_minutes(-?GRACE_PERIOD)),
    Query = fun() ->
        Sessionids = qlc:eval(qlc:q(
            [Rec#canister_times.id || Rec <- mnesia:table(canister_times),
                                             Rec#canister_times.deleted=/=undefined,
                                             Rec#canister_times.deleted < BeforeTime]
        )),
        lists:foreach(fun(Sessid) ->
            delete(Sessid)
        end, Sessionids)
    end,
    {atomic, Res} = mnesia:transaction(Query),
    Res.

integerize(Bin) when is_binary(Bin) ->
    integerize(binary_to_list(Bin));
integerize(List) when is_list(List) ->
    Rev = lists:reverse(List),
    integerize(Rev, 1).

integerize([H|T], Multiplier) ->
    H*Multiplier + integerize(T, Multiplier*256);
integerize([], _) ->
    0.

deint(X) ->
    List = deint_(X),
    list_to_binary(lists:reverse(List)).

deint_(0) ->
    [];
deint_(X) ->
    Next = X div 256,
    Rem = X rem 256,
    [Rem | deint_(Next)].
    
