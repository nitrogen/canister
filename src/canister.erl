-module(canister).
-include("canister.hrl").
-include_lib("stdlib/incluide/qlc.hrl").
-compile(export_all).

init_tables() ->
    init_table(canister_data, record_info(fields, canister_data)).
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

read(Table, ID) when not(is_binary(ID)) ->
    
read(Table, ID) when is_binary(ID) ->
    Res = case mnesia:is_transaction() of
        true -> mnesia:read(Table, ID);
        false -> mnesia:dirty_read(Table, ID)
    end,
    case Res of
        [X] -> X;
        [] -> undefined
    end.

get(ID, Key) ->
    case read(session, ID) of
        #canister_data{data=Data} ->
            maps:get(Key, Data);
        _ ->
            undefined
    end.
    
put(ID, Key, Value) ->
    maybe_wrap_transaction(fun() ->
        Rec = case read(session, ID) of
            S = #canister_data{} ->
                S;
            _ ->
                #canister_data{id=ID}
        end,
        NewData = maps:put(Key, Value, Rec#canister_data.data),
        NewRec = #canister_data{data=NewData},
        write(NewRec),
        update_update_time(ID)
    end).

update_update_time(ID) ->
    Now = os:timestamp(),
    Rec = #session_times{
        id=ID,
        last_access=Now,
        last_update=Now
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
                
        
