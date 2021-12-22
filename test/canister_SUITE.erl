-module(canister_SUITE).

-export([
    start_nodes/1
]).


start_nodes(1) ->
    application:start(canister),
    [self()];
start_nodes(Num) when Num > 1 ->
    Name = list_to_atom("can_" ++ integer_to_list(Num)),
    {ok, Node} = ct_slave:start(Name, [
        {boot_timeout, 10}
    ]),
    net_kernel:connect_node(Node),
    erpc:call(Node, application, ensure_all_started, [canister]),
    start_nodes(Num-1).

all() ->
    [
        {group, one},
%        {group, two},
%        {group, three}
    ].
