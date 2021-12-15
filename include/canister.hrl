-record(canister_data, {
    id,
    data=#{}                    :: map()
}).

-record(canister_times, {
    id,
    last_access=os:timestamp()  :: erlang:timestamp(),
    last_update=os:timestamp()  :: erlang:timestamp(),
    deleted=undefined           :: undefined | erlang:timestamp()
}).
