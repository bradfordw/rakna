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
	leveldb_stats/0
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
leveldb_stats() ->
  gen_server:call(?MODULE, leveldb_stats).

get_options(Type) ->
  gen_server:call(?MODULE, {get_options, Type}).

get_counter(Date, Key) ->
  gen_server:call(?MODULE, {get, {Date, Key}}).
get_counter(Date, Key, []) ->
  gen_server:call(?MODULE, {get, {Date, Key}});
get_counter(Date, Key, Aggregates) ->
  gen_server:call(?MODULE, {get, {Date, Key}, Aggregates}).
increment(Key) when is_binary(Key) ->
  increment({date(), Key});
increment({Date, Key}) ->
  increment({Date, Key}, 1.0).
increment(Key, Amount) when is_binary(Key), is_number(Amount) ->
  increment({date(), Key}, Amount);
increment({Date, Key}, Amount) when is_number(Amount) ->
	gen_server:call(?MODULE, {incr, {Date, Key}, Amount}).

% decr
decrement(Key) when is_binary(Key) ->
  decrement({date(), Key});
decrement({Date, Key}) ->
  decrement({Date, Key}, 1.0).
decrement(Key, Amount) when is_binary(Key), is_number(Amount) ->
  decrement({date(), Key}, Amount);
decrement({Date, Key}, Amount) when is_number(Amount) ->
	gen_server:call(?MODULE, {decr, {Date, Key}, Amount}).


%% asynchronous API
a_increment(Key) when is_binary(Key) ->
  a_increment({date(), Key});
a_increment({Date, Key}) ->
  a_increment({Date, Key}, 1.0).
a_increment(Key, Amount) when is_binary(Key), is_number(Amount) ->
  a_increment({date(), Key}, Amount);
a_increment({Date, Key}, Amount) when is_number(Amount) ->
	gen_server:cast(?MODULE, {incr, {Date, Key}, Amount}).

a_decrement(Key) when is_binary(Key) ->
  a_decrement({date(), Key});
a_decrement({Date, Key}) ->
  a_decrement({Date, Key}, 1.0).
a_decrement(Key, Amount) when is_binary(Key), is_number(Amount) ->
  a_decrement({date(), Key}, Amount);
a_decrement({Date, Key}, Amount) when is_number(Amount) ->
	gen_server:cast(?MODULE, {decr, {Date, Key}, Amount}).

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
handle_call({get, {Date, Key}, Ags}, _From, State) ->
  {ok, Current} = eget(State#rkn_state.ref, {Date, Key}),
  Eget = fun(Ref, {_,_,A} = K) ->
    {ok, Value} = eget(Ref, K),
    {A, Value}
  end,
  R = [Eget(State#rkn_state.ref, {Date, Key, A}) || A <- Ags],
  Response = R ++ [{current, Current}],
	{reply, {ok, Response}, State};
handle_call({get, {Date, Key}}, _From, State) ->
	{ok, CurrentValue} = eget(State#rkn_state.ref, {Date, Key}),
	{reply, {ok, [{current, CurrentValue}]}, State};
handle_call({incr, {Date, Key}, Amount}, _From, State) ->
  {ok, Previous} = eget(State#rkn_state.ref, {Date, Key}),
	{ok, Current} = incr({Date, Key}, Amount, State#rkn_state.ref),
	options_handler({Date, Key}, {Previous, Current}, State),
	{reply, {ok, Current}, State};
handle_call({decr, {Date, Key}, Amount}, _From, State) ->
  {ok, Previous} = eget(State#rkn_state.ref, {Date, Key}),
	{ok, Current} = decr({Date, Key}, Amount, State#rkn_state.ref),
	options_handler({Date, Key}, {Previous, Current}, State),
	{reply, {ok, Current}, State};
handle_call(_Request, _From, State) ->
	{noreply, ok, State}.

handle_cast({incr, {Date, Key}, Amount}, State) ->
  {ok, Previous} = eget(State#rkn_state.ref, {Date, Key}),
	{ok, Current} = incr({Date, Key}, Amount, State#rkn_state.ref),
  options_handler({Date, Key}, {Previous, Current}, State),
	{noreply, State};
handle_cast({decr, {Date, Key}, Amount}, State) ->
  {ok, Previous} = eget(State#rkn_state.ref, {Date, Key}),
	{ok, Current} = decr({Date, Key}, Amount, State#rkn_state.ref),
	options_handler({Date, Key}, {Previous, Current}, State),
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

incr({Date, Key}, Amount, Ref) ->
	{ok, CurrentValue} = eget(Ref, {Date, Key}),
	Value = CurrentValue + Amount,
	ok = eput(Ref, {Date, Key}, Value),
	{ok, Value}.

decr({Date, Key}, Amount, Ref) ->
	{ok, CurrentValue} = eget(Ref, {Date, Key}),
	Value = CurrentValue - Amount,
	ok = eput(Ref, {Date, Key}, Value),
	{ok, Value}.

eget(Ref, Key) ->
  BinKey = sext:encode(Key),
	case eleveldb:get(Ref, BinKey, []) of
		{ok, V} -> {ok, binary_to_term(V)};
		_ -> {ok, 0}
	end.

eput(Ref, Key, Value) ->
  BinKey = sext:encode(Key),
  eleveldb:put(Ref, BinKey, term_to_binary(Value), []).

%% Our poor-man's event handler for now.
options_handler({Date, Key}, {Last, Current}, #rkn_state{ref=Ref, options=Opt}) ->
  A = aggregates,
  case rakna_utils:exists(A, Opt) of
    true ->
      {value, {A, AggVals}, _} = lists:keytake(A, 1, Opt),
      case AggVals =/= [] of
        true ->
          [apply(rakna_aggregates, F, [{Date, Key}, {Last, Current}, Ref]) || F <- AggVals];
        false -> ok
      end;
    false -> ok
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