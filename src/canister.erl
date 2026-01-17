-module(canister).
-include("canister.hrl").
-include_lib("stdlib/include/qlc.hrl").
-export([
    all_sessions/0,
    num_local_sessions/0,
    session_info/1,
    start/0,
    clear/1,
    delete/1,
    get/2,
    get_local/2,
    get_data/1,
    put/3,
    update/3,
    touch/1,
    touch/2,
    touch_local/1,
    touch_local/2,
    last_update_time/1,
    last_access_time/1,
    deleted_time/1,
    clear_untouched_sessions/0,
    delete_deleted_sessions/0,
    queue_delete/2,
    record_status/1
]).
-compile(nowarn_export_all).
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

num_local_sessions() ->
    length(all_sessions()).
    %-dfgdfgdfmnesia:table_info(canister_times, size).

init_tables() ->
    init_table(canister_data, record_info(fields, canister_data)),
    init_table(canister_times, record_info(fields, canister_times)).

init_table(Table, Fields) ->
    MnesiaCopies = case application:get_env(canister, mnesia_table_copies, disc) of
        disc -> disc_copies;
        ram -> ram_copies
    end,
    Res = mnesia:create_table(Table, [
        {MnesiaCopies, [node()]},
        {attributes, Fields}
    ]),
    canister_log:info("Initializing Table: ~p: ~p",[Table, Res]),
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


session_info(ID) ->
    case read(canister_data, ID) of
        #canister_data{data=Data} ->
            Times = read(canister_times, ID),
            #canister_times{
                last_access=Access,
                last_update=Update,
                deleted=Deleted
            } = Times,
            #{
                access=>format_date(Access),
                update=>format_date(Update),
                deleted=>format_date(Deleted),
                data=>Data
            };
        _ ->
            undefined
    end.

format_date(Date) ->
    try qdate:to_string("Y-m-d g:i:sa T", Date)
    catch _:_ -> undefined
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
    canister_sync:send_clear(ID, Time).

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

    
get_data(ID) ->
    case read(canister_data, ID) of
        #canister_data{data=Data} -> Data;
        _ -> #{}
    end.

data_get(Key, Data) ->
    case maps:find(Key, Data) of
        {ok, V} -> V;
        error -> undefined
    end.

get(ID, Key) ->
    case get_local(ID, Key) of
        undefined ->
            case get_remote(ID, Key) of
                undefined -> undefined;
                V ->
                    resync(ID),
                    V
            end;
        V ->
            V
    end.

resync(ID) ->
    canister_resync:add(ID).

get_local(ID, Key) ->
    case read(canister_data, ID) of
        #canister_data{data=Data} ->
            touch(ID),
            data_get(Key, Data);
        _ ->
            undefined
    end.
    
get_remote(ID, Key) ->  
    Nodes = canister_sync:get_nodes(),
    get_remote(Nodes, ID, Key).

get_remote([], _, _) ->
    undefined;
get_remote([Node|Rest], ID, Key) ->
    Val = try erpc:call(Node, canister, get_local, [ID, Key], ?REMOTE_TIMEOUT) %try gen_server:call({canister_sync, Node}, {get_local, ID, Key}, ?REMOTE_TIMEOUT)
          catch _:_ -> undefined
          end,
    case Val of
        undefined ->
            get_remote(Rest, ID, Key);
        _ ->
            Val
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
    touch(ID, os:timestamp()).

touch(ID, Time) ->
    touch_local(ID, Time),
    canister_sync:send_touch(ID, Time).

touch_local(ID) ->
    touch_local(ID, os:timestamp()).

touch_local(ID, Time) ->
    update_access_time(ID, Time).

queue_delete(ID, Time) ->
    canister_sync:send_clear(ID, Time).

record_status(ID) ->
    case deleted_time(ID) of
        undefined ->
            case last_update_time(ID) of
                undefined -> {undefined, 0, 0};
                UpdateTime ->
                    AccessTime = last_access_time(ID),
                    {updated, UpdateTime, AccessTime}
            end;
        Time ->
        {deleted, Time, 0}
    end.

update_update_time(ID) ->
    update_update_time(ID, os:timestamp()).

update_update_time(ID, Time) ->
    Rec = #canister_times{
        id=ID,
        last_access=Time,
        last_update=Time,
        deleted=undefined
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
        #canister_times{last_access=T} -> T;
        undefined -> undefined
    end.

last_update_time(ID) ->
    case read(canister_times, ID) of
        #canister_times{last_update=T} -> T;
        undefined -> undefined
    end.

deleted_time(ID) ->
    case read(canister_times, ID) of
        #canister_times{deleted=T} -> T;
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
    Timeout = ?SESSION_TIMEOUT,
    EffectiveTimeout = Timeout + ?GRACE_PERIOD,
    LastAccessedToExpire = qdate:to_now(qdate:add_minutes(-EffectiveTimeout)),
    Query = fun() ->
        qlc:eval(qlc:q(
            [{Rec#canister_times.id, Rec#canister_times.last_access} || Rec <- mnesia:table(canister_times),
                                             Rec#canister_times.deleted==undefined,
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
            {resync, _Node} ->
                {ID, _} = Sess,
                {true, ID}
        end
    end, Sessions),
    lists:foreach(fun(ID) ->
        resync(ID)
    end, SessionsToSync),
    length(Sessions) - length(SessionsToSync).


clear_untouched_session({ID, LastAccess}) ->
    case latest_cluster_access_time(ID, LastAccess) of
        ok ->
            clear(ID);
        {ok, _LastAccess, Node} ->
            {resync, Node}
    end.

latest_cluster_access_time(ID, LastAccess) ->
    latest_cluster_time(access, ID, LastAccess).

latest_cluster_update_time(ID) ->
    Time = last_update_time(ID),
    latest_cluster_update_time(ID, Time).

latest_cluster_update_time(ID, Time) ->
    latest_cluster_time(update, ID, Time).

latest_cluster_deleted_time(ID) ->
    Time = deleted_time(ID),
    latest_cluster_deleted_time(ID, Time).

latest_cluster_deleted_time(ID, Time) ->
    latest_cluster_time(delete, ID, Time).


latest_cluster_time(Type, ID, LastAccess) ->
    case canister_sync:get_nodes() of
        [] -> ok;
        Nodes ->
            case compare_latest_node_time(Nodes, Type, ID, LastAccess) of
                ok ->
                    ok;
                {ok, Node, NewLastAccess} ->
                    {ok, NewLastAccess, Node}
            end
    end.

compare_latest_node_time(Nodes, Type, ID, LastAccess) ->
    Me = node(),
    %% TODO: This needs to be reworked to just send X messages and receive X messages and compare results.
    %% As written, this will be quite slow
    {FinalNode, FinalAccess} = lists:foldl(fun(Node, {BestNode, BestAccess}) ->
        NodeLatest = latest_node_time(Node, Type, ID),
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

latest_node_time(Node, Type, ID) ->
    FunctionName = case Type of
        access -> last_access_time;
        update -> last_update_time;
        delete -> deleted_time
    end,
    try gen_server:call({canister_sync, Node}, {FunctionName, ID}, ?REMOTE_TIMEOUT) of
        {ok, undefined} -> 0;
        {ok, Val} -> Val;
        _ -> 0
    catch _:_ -> 0
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
        end, Sessionids),
        length(Sessionids)
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
    
