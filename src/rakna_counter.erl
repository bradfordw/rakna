-module(rakna_counter).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(counter_state, {level_ref}).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
	{ok, LevelDbPath} = application:get_env(rakna_leveldb_path),
	{ok, Ref} = open(LevelDbPath, [{create_if_missing, true}]),
	{ok, #counter_state{level_ref = Ref}}.

handle_call({incr, Key}, _From, State) ->
	incr(Key, State#counter_state.level_ref);
handle_call({incr, Date, Key}, _From, State) ->
	incr(Date, Key, State#counter_state.level_ref);
handle_call({incrby, Key, Amount}, _From, State) ->
	incrby(Key, Amount, State#counter_state.level_ref);
handle_call({incrby, Date, Key, Amount}, _From, State) ->
	incrby(Date, Key, Amount, State#counter_state.level_ref);
handle_call(_Request, _From, State) ->
	{noreply, ok, State}.

handle_cast({incr, Key}, State) ->
	incr(Key, State#counter_state.level_ref);
handle_cast({incr, Date, Key}, State) ->
	incr(Date, Key, State#counter_state.level_ref);
handle_cast({incrby, Key, Amount}, State) ->
	incrby(Key, Amount, State#counter_state.level_ref);
handle_cast({incrby, Date, Key, Amount}, State) ->
	incrby(Date, Key, Amount, State#counter_state.level_ref);
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
	incr(date(), Key, 1, Ref).

incr(Date, Key, Amount, Ref) ->
%% TODO: use sext on {Date, Key} to make eleveldb happier (sequential keys)
	BinKey = term_to_binary({Date, Key}),
	{ok, CurrentValue} = get(Ref, BinKey),
	NewValue = Current + Amount,
	ok = eleveldb:put(Ref, BinKey, NewValue, []).

decr(Key, Ref) ->
	decr(date(), Key, 1, Ref).

decr(Date, Key, Amount, Ref) ->
%% TODO: use sext on {Date, Key} ... 
	BinKey = term_to_binary({Date, Key}),
	{ok, CurrentValue} = get(Ref, BinKey),
	NewValue = Current + Amount,
	ok = eleveldb:put(Ref, BinKey, NewValue, []).

get(Ref, Key) ->
	case eleveldb:get(Ref, BinKey, []) of
		{ok, V} ->
			{ok, V};
		_ -> {ok, 0}
	end.