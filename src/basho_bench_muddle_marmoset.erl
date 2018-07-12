%%%-------------------------------------------------------------------
%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2018, Russell Brown
%%% @doc
%%% It's not a chaos monkey. It's smaller, and less chaotic.  What if
%%% you made a change to riak (like always choosing to GET from the
%%% head of the preflist) that performed better under ideal conditions,
%%% but you suspect may perform badly under failure conditions. The
%%% muddle marmoset can be configured to cause some minor chaos. At a
%%% certain time in your test it can take down a node, and later bring
%%% it back up. That's it for now. But as time passes, it may get more
%%% features.
%%% @end

%%% @TODO if the mm crashes it doesn't adjust the "Elapsed" time from
%%% the start-time of the test. It should

%%% Created : 10 Jul 2018 by Russell Brown <russell@wombat.me>
%%%-------------------------------------------------------------------
-module(basho_bench_muddle_marmoset).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(operation, {
          time,
          op,
          op_cmd
         }).

-record(state, {
          start_time = os:timestamp(),
          ops = [] :: list(operation())
         }).

-type operation() :: #operation{}.

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),

    %% get config
    Config = basho_bench_config:get(muddle_marmoset),
    tick(),
    State = setup_muddle(Config, #state{}),
    lager:info("muddle marmoset started with State ~p", [State]),
    {ok, State}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    #state{start_time=StartTime, ops=Ops} = State,
    Runnable = get_runnable_ops(StartTime, Ops, []),
    State2 = run_ops(Runnable, State, []),
    tick(),
    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
setup_muddle([], State) ->
    State;
setup_muddle([{Operation, OpConfig} | Rest], State) ->
    State2 = setup_operation(Operation, OpConfig, State),
    setup_muddle(Rest, State2).

setup_operation(Name, OpConfig, State) ->
    Cmd = proplists:get_value(cmd, OpConfig),
    When = proplists:get_value(elapsed, OpConfig),
    Op = #operation{op=Name, op_cmd=Cmd, time=When},
    #state{ops=Ops} = State,
    State#state{ops=[Op | Ops]}.

get_runnable_ops(_StartTime, [], Runnable) ->
    Runnable;
get_runnable_ops(StartTime, [#operation{}=Op | Rest], Runnable) ->
    Runnable2 = case is_runnable(StartTime, Op) of
                    true ->
                        [Op | Runnable];
                    false ->
                        Runnable
                end,
    get_runnable_ops(StartTime, Rest, Runnable2).

%% Right now ops run once, and have an elapsed minutes time (i.e. the
%% op occurs after `Time' minutes have elapsed) there is no reason why
%% this can't be extended with re-occuring ops, etc etc, just why
%% bother for this simplest of possible things? NOTE: the operation
%% should be removed once it has run, or it will run again, and again.
is_runnable(StartTime, #operation{time=Time}) ->
    DiffMicros = timer:now_diff(os:timestamp(), StartTime),
    DiffMins = round(DiffMicros / 60000000),
    DiffMins >= Time.

%% runs the actual operations, if they can be run, removes them from
%% state (even if they failed, though we can add fail counters,
%% backoff, periodic commands etc if needed)
run_ops([], State, Removeable) ->
    #state{ops=Ops} = State,
    OpsSet = ordsets:from_list(Ops),
    RemSet = ordsets:from_list(Removeable),
    NewOps = ordsets:subtract(OpsSet, RemSet),
    State#state{ops=NewOps};
run_ops([Op | Rest], State, Ran) ->
    run_op(Op),
    %% TODO restart workers? Update config?
    run_ops(Rest, State, [Op | Ran]).

%% Simplest thing that could possibley work. Yes you _CAN_ "sudo rm
%% -rf /"
run_op(#operation{op=Name, op_cmd=Cmd}) ->
    lager:info("running command ~p ~p", [Name, Cmd]),
    Res = os:cmd(Cmd),
    lager:info("Result of ~p was ~p", [Name, Res]).


tick() ->
    %% check every 30 seconds
    erlang:send_after(30000, self(), tick).
