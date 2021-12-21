-module(canister_exp).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(SIXTY_SECONDS, 60000).

-record(state, {timer_ref, last_clean=never}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, TRef} = schedule_next_cleaning(),
    {ok, #state{timer_ref=TRef}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(clean, State=#state{last_clean=LastClean}) ->
    error_logger:info_msg("Last Cleaning: ~s", [draw_last_time(LastClean)]),
    erlang:cancel_timer(State#state.timer_ref),
    do_cleaning(),
    {ok, TRef} = schedule_next_cleaning(),
    {noreply, State#state{timer_ref=TRef, last_clean=os:timestamp()}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

draw_last_time(never) ->
    "Never";
draw_last_time(LastClean) ->
    qdate:to_string("Y-m-d g:i:sa T", LastClean).

do_cleaning() ->
    %% Delete the old sessions that have been deleted
    canister:delete_deleted_sessions(),
    %% Clean sessions that haven't been touched
    canister:clear_untouched_sessions().



schedule_next_cleaning() ->
    Interval = canister_config:clear_interval(),
    Jitter = canister_config:interval_jitter(),
    Next = next_cleaning(Interval, Jitter),
    timer:send_after(Next, clean).


%% timeout and jitter are in minutes
%% return value is in milliseconds
next_cleaning(Interval, Jitter) ->
    Min0 = Interval - Jitter,
    Max0 = Interval + Jitter,
    {Min, Max} = normalize_min_max(Min0 * ?SIXTY_SECONDS, Max0 * ?SIXTY_SECONDS),
    rand(Min, Max).

normalize_min_max(Min, Max) when Min > Max ->
    normalize_min_max(Max, Min);
normalize_min_max(Min, Max) when Min < 0 ->
    {0, Max - Min};
normalize_min_max(X, X) ->
    {X, X};
normalize_min_max(Min, Max) ->
    {Min, Max}.


rand(X, X) ->
    X;
rand(Min, Max) ->
    Diff = Max - Min,
    Rand = rand:uniform(Diff),
    Rand + Min.
    


