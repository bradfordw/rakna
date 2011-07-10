-module(rakna_counter).

-behaviour(gen_server).

-export([start_link/0, 
	get_counter/1,
	get_counter/2,
	increment/1,
	increment/2,
	increment/3,
	decrement/1,
	decrement/2,
	decrement/3
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(counter_state, {level_ref}).

%% API
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
increment(Date, Key) ->
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

%% gen_server exports
init([]) ->
	{ok, LevelDbPath} = application:get_env(rakna_leveldb_path),
	{ok, Ref} = eleveldb:open(LevelDbPath, [{create_if_missing, true}]),
	{ok, #counter_state{level_ref = Ref}}.

handle_call({get, Key}, _From, State) ->
	K = term_to_binary({date(), Key}),
	{ok, CurrentValue} = get(State#counter_state.level_ref, K),
	{reply, CurrentValue, State};
handle_call({get, Date, Key}, _From, State) ->
	K = term_to_binary({Date, Key}),
	{ok, CurrentValue} = get(State#counter_state.level_ref, K),
	{reply, CurrentValue, State};
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

incr(Key, Amount, Ref) ->
	incr(date(), Key, Amount, Ref).

incr(Date, Key, Amount, Ref) ->
	BinKey = term_to_binary({Date, Key}),
	{ok, CurrentValue} = get(Ref, BinKey),
	NV = float_to_list(CurrentValue + Amount),
	NewValue = list_to_binary(NV),
	ok = eleveldb:put(Ref, BinKey, NewValue, []).

decr(Key, Ref) ->
	decr(date(), Key, 1.0, Ref).

decr(Key, Amount, Ref) ->
	decr(date(), Key, Amount, Ref).

decr(Date, Key, Amount, Ref) ->
	BinKey = term_to_binary({Date, Key}),
	{ok, CurrentValue} = get(Ref, BinKey),
	NV = float_to_list(CurrentValue - Amount),
	NewValue = list_to_binary(NV),
	ok = eleveldb:put(Ref, BinKey, NewValue, []).

get(Ref, Key) ->
	BinKey = case Key of
		K when is_binary(K) -> K;
		K when is_tuple(K) -> sext:encode(Key)
	end,
	case eleveldb:get(Ref, BinKey, []) of
		{ok, V} ->
			{ok, list_to_float(binary_to_list(V))};
		_ -> {ok, 0.0}
	end.