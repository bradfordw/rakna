-module(rakna_node).

-behaviour(gen_server).
-include_lib("eunit/include/eunit.hrl").

-export([start_link/0, start_link/2,
	get_counter/2,
	get_counter/3,
	increment/1,
	increment/2,
	a_increment/1,
	a_increment/2,
	decrement/1,
	decrement/2,
	a_decrement/1,
	a_decrement/2,
	eget/2,
	eput/3,
	options_handler/3,
	get_options/1,
	leveldb_stats/0,
	receive_batch/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(rkn_state, {ref, options=[]}).

%% API
start_link(LevelDbPath, Options) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [LevelDbPath, Options], []).

start_link() ->
  {ok, LevelDbPath} = application:get_env(rakna_leveldb_path),
  {ok, RaknaOptions} = application:get_env(rakna_options),
	gen_server:start_link({local, ?MODULE}, ?MODULE, [LevelDbPath, RaknaOptions], []).

%% synchronous API
receive_batch([]) ->
  ok;
receive_batch(Batch) when is_list(Batch) ->
  gen_server:call(?MODULE, {receive_batch, Batch}).

leveldb_stats() ->
  gen_server:call(?MODULE, leveldb_stats).

get_options(Type) ->
  gen_server:call(?MODULE, {get_options, Type}).

get_counter(Interval, Key) ->
  gen_server:call(?MODULE, {get, {Interval, Key}}).
get_counter(Interval, Key, []) ->
  gen_server:call(?MODULE, {get, {Interval, Key}});
get_counter(Interval, Key, Aggregates) ->
  gen_server:call(?MODULE, {get, {Interval, Key}, Aggregates}).
increment(Key) when is_binary(Key) ->
  increment({date(), Key});
increment({Interval, Key}) ->
  increment({Interval, Key}, 1.0).
increment(Key, Amount) when is_binary(Key), is_number(Amount) ->
  increment({date(), Key}, Amount);
increment({Interval, Key}, Amount) when is_number(Amount) ->
	gen_server:call(?MODULE, {incr, {Interval, Key}, Amount}).

% decr
decrement(Key) when is_binary(Key) ->
  decrement({date(), Key});
decrement({Interval, Key}) ->
  decrement({Interval, Key}, 1.0).
decrement(Key, Amount) when is_binary(Key), is_number(Amount) ->
  decrement({date(), Key}, Amount);
decrement({Interval, Key}, Amount) when is_number(Amount) ->
	gen_server:call(?MODULE, {decr, {Interval, Key}, Amount}).

%% asynchronous API
a_increment(Key) when is_binary(Key) ->
  a_increment({date(), Key});
a_increment({Interval, Key}) ->
  a_increment({Interval, Key}, 1.0).
a_increment(Key, Amount) when is_binary(Key), is_number(Amount) ->
  a_increment({date(), Key}, Amount);
a_increment({Interval, Key}, Amount) when is_number(Amount) ->
	gen_server:cast(?MODULE, {incr, {Interval, Key}, Amount}).

a_decrement(Key) when is_binary(Key) ->
  a_decrement({date(), Key});
a_decrement({Interval, Key}) ->
  a_decrement({Interval, Key}, 1.0).
a_decrement(Key, Amount) when is_binary(Key), is_number(Amount) ->
  a_decrement({date(), Key}, Amount);
a_decrement({Interval, Key}, Amount) when is_number(Amount) ->
	gen_server:cast(?MODULE, {decr, {Interval, Key}, Amount}).

%% gen_server exports
init([LevelDbPath, Options]) ->
	{ok, Ref} = eleveldb:open(LevelDbPath, [{create_if_missing, true}]),
	{ok, #rkn_state{ref=Ref, options=Options}}.

handle_call(leveldb_stats, _From, State) ->
	R = eleveldb:status(State#rkn_state.ref, <<"leveldb.stats">>),
  {reply, R, State};
handle_call({get_options, Type}, _From, State) ->
  O = State#rkn_state.options,
  Response = case rakna_utils:exists(Type, O) of
    true ->
      case proplists:get_value(Type, O) of
        undefined -> [];
        P -> P
      end;
    _ -> []
  end,
  {reply, {ok, Response}, State};
handle_call({get, {Interval, Key}, Ags}, _From, State) ->
  {ok, Current} = eget(State#rkn_state.ref, {Interval, Key}),
  Eget = fun(Ref, {_,_,A} = K) ->
    {ok, Value} = eget(Ref, K),
    {A, Value}
  end,
  R = [Eget(State#rkn_state.ref, {Interval, Key, A}) || A <- Ags],
  Response = R ++ [{current, Current}],
	{reply, {ok, Response}, State};
handle_call({get, {Interval, Key}}, _From, State) ->
	{ok, CurrentValue} = eget(State#rkn_state.ref, {Interval, Key}),
	{reply, {ok, [{current, CurrentValue}]}, State};
handle_call({incr, {Interval, Key}, Amount}, _From, State) ->
  {ok, Previous} = eget(State#rkn_state.ref, {Interval, Key}),
	{ok, Current} = incr({Interval, Key}, Amount, State#rkn_state.ref),
	options_handler({Interval, Key}, {Previous, Current}, State),
	{reply, {ok, Current}, State};
handle_call({decr, {Interval, Key}, Amount}, _From, State) ->
  {ok, Previous} = eget(State#rkn_state.ref, {Interval, Key}),
	{ok, Current} = decr({Interval, Key}, Amount, State#rkn_state.ref),
	options_handler({Interval, Key}, {Previous, Current}, State),
	{reply, {ok, Current}, State};
handle_call({receive_batch, Actions}, _From, State) ->
  ok = ewrite(State#rkn_state.ref, Actions),
  {reply, ok, State};
handle_call(_Request, _From, State) ->
	{noreply, ok, State}.

handle_cast({incr, {Interval, Key}, Amount}, State) ->
  {ok, Previous} = eget(State#rkn_state.ref, {Interval, Key}),
	{ok, Current} = incr({Interval, Key}, Amount, State#rkn_state.ref),
  options_handler({Interval, Key}, {Previous, Current}, State),
	{noreply, State};
handle_cast({decr, {Interval, Key}, Amount}, State) ->
  {ok, Previous} = eget(State#rkn_state.ref, {Interval, Key}),
	{ok, Current} = decr({Interval, Key}, Amount, State#rkn_state.ref),
	options_handler({Interval, Key}, {Previous, Current}, State),
	{noreply, State};
handle_cast({receive_batch, Actions}, State) ->
  ewrite(State#rkn_state.ref, Actions),
  {noreply, State};
handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% Internal Functions %%

incr({Interval, Key}, Amount, Ref) ->
	{ok, CurrentValue} = eget(Ref, {Interval, Key}),
	Value = CurrentValue + Amount,
	ok = eput(Ref, {Interval, Key}, Value),
	{ok, Value}.

decr({Interval, Key}, Amount, Ref) ->
	{ok, CurrentValue} = eget(Ref, {Interval, Key}),
	Value = CurrentValue - Amount,
	ok = eput(Ref, {Interval, Key}, Value),
	{ok, Value}.

eget(Ref, Key) ->
  BinKey = sext:encode(Key),
	case eleveldb:get(Ref, BinKey, []) of
		{ok, V} -> {ok, binary_to_term(V)};
		_ -> {ok, 0}
	end.

eput(Ref, Key, Value) ->
  BinKey = sext:encode(Key),
  eleveldb:put(Ref, BinKey, term_to_binary(Value), [{sync, true}]).

ewrite(Ref, Actions) ->
  eleveldb:write(Ref, Actions, [{sync, true}]).

%% Our poor-man's event handler for now.
options_handler({Interval, Key}, {Last, Current}, #rkn_state{ref=Ref, options=Opt}) ->
  BinCurrent = term_to_binary(Current),
  SeKey = sext:encode({Interval, Key}),
  case rakna_utils:exists(aggregates, Opt) of
    true ->
      AggVals = proplists:get_value(aggregates, Opt),
      case AggVals =/= [] of
        true ->
          WriteBatch = lists:filter(fun(E) -> 
            E =/= no_change
          end,
          [apply(rakna_aggregates, F, [{Interval, Key}, {Last, Current}, Ref]) || F <- AggVals]),
          WriteBatch1 = lists:map(fun({A, K, V}) -> {A, sext:encode(K), term_to_binary(V)} end, WriteBatch),
          Payload = WriteBatch1 ++ [{put, SeKey, BinCurrent}],
          ewrite(Ref, WriteBatch1);
        false ->
          Payload = [{put, SeKey, BinCurrent}],
          ok
      end;
    false ->
      Payload = [{put, SeKey, BinCurrent}],
      ok
  end,
  case rakna_utils:exists(nodes, Opt) of
    false -> ok;
    true ->
      case proplists:get_value(nodes, Opt) of
        [] -> ok;
        Nodes when is_list(Nodes) ->
          rpc:multicall(Nodes, rakna_node, receive_batch, [Payload], 65000)
      end
  end,
  done.

-ifdef(TEST).

start_test() ->
  os:cmd("rm -rf /tmp/rakna_test.ldb"),
  case whereis(rakna_node) of P
    when is_pid(P) -> ok;
    _ ->
      {ok, _} = ?MODULE:start_link("/tmp/rakna_test.ldb",[]),
      ok
  end.

incr_test() ->
  {D,K} = {date(), <<"test_key">>},
  {ok, [{current, Was}]} = rakna_node:get_counter(D, K),
  {ok, _} = rakna_node:increment({D, K}),
  {ok, [{current, Is}]} = rakna_node:get_counter(D, K),
  Is = Was + 1.0,
  {ok, Is}.

decr_test() ->
  {D,K} = {date(), <<"test_key">>},
  {ok, [{current, Was}]} = rakna_node:get_counter(D, K),
  {ok, _} = rakna_node:decrement({D, K}),
  {ok, [{current, Is}]} = rakna_node:get_counter(D, K),
  Is = Was - 1.0,
  {ok, Is}.

a_incr_test() ->
  {D,K} = {date(), <<"test_key">>},
  {ok, [{current, Was}]} = rakna_node:get_counter(D, K),
  rakna_node:a_increment({D, K}),
  timer:sleep(3600),
  {ok, [{current, Is}]} = rakna_node:get_counter(D, K),
  Is = Was + 1.0,
  {ok, Is}.

a_decr_test() ->
  {D,K} = {date(), <<"test_key">>},
  {ok, [{current, Was}]} = rakna_node:get_counter(D, K),
  rakna_node:a_decrement({D, K}),
  timer:sleep(3600),
  {ok, [{current, Is}]} = rakna_node:get_counter(D, K),
  Is = Was - 1.0,
  {ok, Is}.

incrby_test() ->
  Amount = 42,
  {D,K} = {date(), <<"incrby_test_key">>},
  {ok, [{current, Was}]} = rakna_node:get_counter(D, K),
  {ok, _} = rakna_node:increment({D, K}, Amount),
  {ok, [{current, Is}]} = rakna_node:get_counter(D, K),
  Is = Was + Amount,
  {ok, Is}.

decrby_test() ->
  Amount = 27,
  {D,K} = {date(), <<"decrby_test_key">>},
  {ok, [{current, Was}]} = rakna_node:get_counter(D, K),
  {ok, _} = rakna_node:decrement({D, K}, Amount),
  {ok, [{current, Is}]} = rakna_node:get_counter(D, K),
  Is = Was - Amount,
  {ok, Is}.

a_incrby_test() ->
  Amount = 37,
  {D,K} = {date(), <<"a_incrby_test_key">>},
  {ok, [{current, Was}]} = rakna_node:get_counter(D, K),
  rakna_node:a_increment({D, K}, Amount),
  timer:sleep(3600),
  {ok, [{current, Is}]} = rakna_node:get_counter(D, K),
  Is = Was + Amount,
  {ok, Is}.

a_decrby_test() ->
  Amount = 18.4,
  {D,K} = {date(), <<"a_decrby_test_key">>},
  {ok, [{current, Was}]} = rakna_node:get_counter(D, K),
  rakna_node:a_decrement({D, K}, Amount),
  timer:sleep(3600),
  {ok, [{current, Is}]} = rakna_node:get_counter(D, K),
  Is = Was - Amount,
  {ok, Is}.

-endif.