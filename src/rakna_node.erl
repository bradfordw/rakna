-module(rakna_node).

-behaviour(gen_server).
-include_lib("eunit/include/eunit.hrl").

% synchronous exports
-export([start_link/0, start_link/1,  
	get_counter/1,
	get_counter/2,
	increment/1,
	increment/2,
	increment/3,
	decrement/1,
	decrement/2,
	decrement/3,
	get/2,
	put/4
]).

% asynchronous exports
-export([a_increment/1,
  a_increment/2,
	a_increment/3,
	a_decrement/1,
	a_decrement/2,
	a_decrement/3
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(counter_state, {level_ref}).
-define(TEST_NODE_CONNECT, fun() -> case whereis(rakna_node) of P when is_pid(P) -> ok; _ -> {ok, _} = ?MODULE:start_link("/tmp/rakna_test.ldb"), ok end end).
%% API
start_link(LevelDbPath) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [LevelDbPath], []).

start_link() ->
  {ok, LevelDbPath} = application:get_env(rakna_leveldb_path),
	gen_server:start_link({local, ?MODULE}, ?MODULE, [LevelDbPath], []).

%% synchronous API
get_counter(Key) when is_list(Key) ->
  get_counter(list_to_binary(Key));
get_counter(Key) when is_binary(Key) ->
	gen_server:call(?MODULE, {get, Key}).

get_counter(Date, Key) when is_tuple(Date) ->
	gen_server:call(?MODULE, {get, Date, Key}).

increment(Key) ->
	gen_server:call(?MODULE, {incr, Key}).

increment(Key, Amount) when is_integer(Amount) ->
	FloatAmt = (Amount * 1.0),
	increment(Key, FloatAmt);
increment(Key, Amount) when is_float(Amount) ->
	gen_server:call(?MODULE, {incrby, date(), Key, Amount});
increment(Date, Key) when is_tuple(Date) ->
	gen_server:call(?MODULE, {incr, Date, Key}).

increment(Date, Key, Amount) when is_integer(Amount) ->
	FloatAmt = (Amount * 1.0),
	increment(Date, Key, FloatAmt);
increment(Date, Key, Amount) when is_float(Amount) ->
	gen_server:call(?MODULE, {incrby, Date, Key, Amount}).

decrement(Key) ->
	gen_server:call(?MODULE, {decr, Key}).

decrement(Key, Amount) when is_integer(Amount) ->
	FloatAmt = (Amount * 1.0),
	decrement(Key, FloatAmt);
decrement(Key, Amount) when is_float(Amount) ->
	gen_server:call(?MODULE, {decrby, date(), Key, Amount});
decrement(Date, Key) ->
	gen_server:call(?MODULE, {decr, Date, Key}).

decrement(Date, Key, Amount) when is_integer(Amount) ->
	FloatAmt = (Amount * 1.0),
	decrement(Date, Key, FloatAmt);
decrement(Date, Key, Amount) when is_float(Amount) ->
	gen_server:call(?MODULE, {decrby, Date, Key, Amount}).

%% asynchronous API
a_increment(Key) ->
	gen_server:cast(?MODULE, {incr, Key}).

a_increment(Key, Amount) when is_integer(Amount) ->
	FloatAmt = (Amount * 1.0),
	a_increment(Key, FloatAmt);
a_increment(Key, Amount) when is_float(Amount) ->
	gen_server:cast(?MODULE, {incrby, date(), Key, Amount});
a_increment(Date, Key) ->
	gen_server:cast(?MODULE, {incr, Date, Key}).

a_increment(Date, Key, Amount) when is_integer(Amount) ->
	FloatAmt = (Amount * 1.0),
	a_increment(Date, Key, FloatAmt);
a_increment(Date, Key, Amount) when is_float(Amount) ->
	gen_server:cast(?MODULE, {incrby, Date, Key, Amount}).

a_decrement(Key) ->
	gen_server:cast(?MODULE, {decr, Key}).

a_decrement(Key, Amount) when is_integer(Amount) ->
	FloatAmt = (Amount * 1.0),
	a_decrement(Key, FloatAmt);
a_decrement(Key, Amount) when is_float(Amount) ->
	gen_server:cast(?MODULE, {decrby, date(), Key, Amount});
a_decrement(Date, Key) ->
	gen_server:cast(?MODULE, {decr, Date, Key}).

a_decrement(Date, Key, Amount) when is_integer(Amount) ->
	FloatAmt = (Amount * 1.0),
	a_decrement(Date, Key, FloatAmt);
a_decrement(Date, Key, Amount) when is_float(Amount) ->
	gen_server:cast(?MODULE, {decrby, Date, Key, Amount}).

%% gen_server exports
init([LevelDbPath]) ->
	{ok, Ref} = eleveldb:open(LevelDbPath, [{create_if_missing, true}]),
	{ok, #counter_state{level_ref = Ref}}.

handle_call({get, Key}, _From, State) ->
	K = term_to_binary({date(), Key}),
	{ok, CurrentValue} = ?MODULE:get(State#counter_state.level_ref, K),
	{reply, {ok, CurrentValue}, State};
handle_call({get, Date, Key}, _From, State) ->
	K = term_to_binary({Date, Key}),
	{ok, CurrentValue} = ?MODULE:get(State#counter_state.level_ref, K),
	{reply, {ok, CurrentValue}, State};
handle_call({incr, Key}, _From, State) ->
	R = incr(Key, State#counter_state.level_ref),
	{reply, R, State};
handle_call({incr, Date, Key}, _From, State) ->
	R = incr(Date, Key, State#counter_state.level_ref),
	{reply, R, State};
handle_call({incrby, Key, Amount}, _From, State) ->
	R = incr(Key, Amount, State#counter_state.level_ref),
	{reply, R, State};
handle_call({incrby, Date, Key, Amount}, _From, State) ->
	R = incr(Date, Key, Amount, State#counter_state.level_ref),
	{reply, R, State};
handle_call({decr, Key}, _From, State) ->
	R = decr(Key, State#counter_state.level_ref),
	{reply, R, State};
handle_call({decr, Date, Key}, _From, State) ->
	R = decr(Date, Key, State#counter_state.level_ref),
	{reply, R, State};
handle_call({decrby, Key, Amount}, _From, State) ->
	R = decr(Key, Amount, State#counter_state.level_ref),
	{reply, R, State};
handle_call({decrby, Date, Key, Amount}, _From, State) ->
	R = decr(Date, Key, Amount, State#counter_state.level_ref),
	{reply, R, State};
handle_call(_Request, _From, State) ->
	{noreply, ok, State}.

handle_cast({incr, Key}, State) ->
  incr(Key, State#counter_state.level_ref),
	{noreply, State};
handle_cast({incr, Date, Key}, State) ->
	incr(Date, Key, State#counter_state.level_ref),
	{noreply, State};
handle_cast({incrby, Key, Amount}, State) ->
	incr(Key, Amount, State#counter_state.level_ref),
	{noreply, State};
handle_cast({incrby, Date, Key, Amount}, State) ->
	incr(Date, Key, Amount, State#counter_state.level_ref),
	{noreply, State};
handle_cast({decr, Key}, State) ->
	decr(Key, State#counter_state.level_ref),
	{noreply, State};
handle_cast({decr, Date, Key}, State) ->
	decr(Date, Key, State#counter_state.level_ref),
	{noreply, State};
handle_cast({decrby, Key, Amount}, State) ->
	decr(Key, Amount, State#counter_state.level_ref),
	{noreply, State};
handle_cast({decrby, Date, Key, Amount}, State) ->
	decr(Date, Key, Amount, State#counter_state.level_ref),
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

incr(Key, Ref) ->
	incr(date(), Key, 1.0, Ref).

incr(Date, Key, Ref) when is_tuple(Date) ->
  incr(Date, Key, 1.0, Ref);
incr(Key, Amount, Ref) ->
	incr(date(), Key, Amount, Ref).

incr(Date, Key, Amount, Ref) ->
	BinKey = term_to_binary({Date, Key}),
	{ok, CurrentValue} = ?MODULE:get(Ref, BinKey),
	NV = float_to_list(CurrentValue + Amount),
	NewValue = list_to_binary(NV),
	ok = ?MODULE:put(Ref, BinKey, NewValue, []).

decr(Key, Ref) ->
	decr(date(), Key, 1.0, Ref).

decr(Key, Amount, Ref) ->
	decr(date(), Key, Amount, Ref).

decr(Date, Key, Amount, Ref) ->
	BinKey = term_to_binary({Date, Key}),
	{ok, CurrentValue} = ?MODULE:get(Ref, BinKey),
	NV = float_to_list(CurrentValue - Amount),
	NewValue = list_to_binary(NV),
	ok = ?MODULE:put(Ref, BinKey, NewValue, []).

get(Ref, Key) ->
	BinKey = case Key of
		K when is_binary(K) -> K;
		K when is_tuple(K) -> sext:encode(Key)
	end,
	case eleveldb:get(Ref, BinKey, []) of
		{ok, V} ->
			{ok, list_to_float(binary_to_list(V))};
		_ -> {ok, 0}
	end.

put(Ref, Key, Value, []) ->
  eleveldb:put(Ref, Key, Value, []).

%% Tests
% start_test() ->
%   {ok, _} = ?MODULE:start_link("/tmp/rakna_test.ldb").

incr_test() ->
  ok = ?TEST_NODE_CONNECT(),
  K = <<"test_key">>,
  {ok, Was} = ?MODULE:get_counter(K),
  ok = ?MODULE:increment(K),
  {ok, Is} = ?MODULE:get_counter(K),
  Is = Was + 1.0.

decr_test() ->
  ok = ?TEST_NODE_CONNECT(),
  K = <<"test_key">>,
  {ok, Was} = ?MODULE:get_counter(K),
  ok = ?MODULE:decrement(K),
  {ok, Is} = ?MODULE:get_counter(K),
  Is = Was - 1.0.