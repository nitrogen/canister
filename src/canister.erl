-module(canister).
-include("canister.hrl").
-include_lib("stdlib/include/qlc.hrl").
-export([
    start/0,
    clear/1,
    delete/1,
    get/2,
    put/3,
    touch/1
]).


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
    init_tables().

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
    maybe_wrap_transaction(fun() ->
        case read(canister_data, ID) of
            #canister_data{} ->
                C = #canister_data{id=ID},
                write(C),
                update_delete_time(ID),
                queue_delete(ID),
                ok;
            _ ->
                ok
        end
    end).

delete(ID) ->
    maybe_wrap_transaction(fun() ->
        mnesia:delete(canister_data, ID),
        mnesia:delete(canister_times, ID)
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
        update_update_time(ID),
        queue_update(ID),
        Prev
    end).

touch(ID) ->
    update_access_time(ID),
    queue_touch(ID).

queue_delete(ID) ->
    ok.

queue_update(ID) ->
    ok.

queue_touch(ID) ->
    ok.

update_update_time(ID) ->
    Now = os:timestamp(),
    Rec = #canister_times{
        id=ID,
        last_access=Now,
        last_update=Now
    },
    write(Rec).

update_delete_time(ID) ->
    Now = os:timestamp(),
    Rec = #canister_times{
        id=ID,
        last_access=undefined,
        last_update=undefined,
        deleted=Now
    },
    write(Rec).

update_access_time(ID) ->
    Now = os:timestamp(),
    maybe_wrap_transaction(fun() ->
        case read(canister_times, ID) of
            T = #canister_times{} ->
                New = T#canister_times{last_access=os:timestamp()},
                write(New);
            undefined ->
                ok
        end
    end).
                
        
