-module(rakna_node).

-behaviour(gen_server).
-include_lib("eunit/include/eunit.hrl").

-export([start_link/0, start_link/2,
	get_counter/2,
	increment/1,
	increment/2,
	a_increment/1,
	a_increment/2,
	decrement/1,
	decrement/2,
	a_decrement/1,
	a_decrement/2
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(rkn_state, {ref, options=[]}).

%% API
start_link(LevelDbPath, []) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [LevelDbPath,[]], []).

start_link() ->
  {ok, LevelDbPath} = application:get_env(rakna_leveldb_path),
  {ok, RaknaOptions} = application:get_env(rakna_options),
	gen_server:start_link({local, ?MODULE}, ?MODULE, [LevelDbPath, RaknaOptions], []).

%% synchronous API
get_counter(Date, Key) ->
	gen_server:call(?MODULE, {get, {Date, Key}}).

increment(Key) when is_binary(Key) ->
  increment({date(), Key});
increment({Date, Key}) ->
  increment({Date, Key}, 1.0).
increment({Date, Key}, Amount) when is_number(Amount) ->
	gen_server:call(?MODULE, {incr, {Date, Key}, Amount}).

% decr
decrement(Key) when is_binary(Key) ->
  decrement({date(), Key});
decrement({Date, Key}) ->
  decrement({Date, Key}, 1.0).
decrement({Date, Key}, Amount) when is_number(Amount) ->
	gen_server:call(?MODULE, {decr, {Date, Key}, Amount}).


%% asynchronous API
a_increment(Key) when is_binary(Key) ->
  a_increment({date(), Key});
a_increment({Date, Key}) ->
  a_increment({Date, Key}, 1.0).
a_increment({Date, Key}, Amount) when is_number(Amount) ->
	gen_server:cast(?MODULE, {incr, {Date, Key}, Amount}).

a_decrement(Key) when is_binary(Key) ->
  a_decrement({date(), Key});
a_decrement({Date, Key}) ->
  a_decrement({Date, Key}, 1.0).
a_decrement({Date, Key}, Amount) when is_number(Amount) ->
	gen_server:cast(?MODULE, {decr, {Date, Key}, Amount}).

%% gen_server exports
init([LevelDbPath, Options]) ->
	{ok, Ref} = eleveldb:open(LevelDbPath, [{create_if_missing, true}]),
	{ok, #rkn_state{ref = Ref, options = Options}}.

handle_call({get, {Date, Key}}, _From, State) ->
	{ok, CurrentValue} = eget(State#rkn_state.ref, {Date, Key}),
	{reply, {ok, CurrentValue}, State};
handle_call({incr, {Date, Key}, Amount}, _From, State) ->
	R = incr({Date, Key}, Amount, State#rkn_state.ref),
	{reply, R, State};
handle_call({decr, {Date, Key}, Amount}, _From, State) ->
	R = decr({Date, Key}, Amount, State#rkn_state.ref),
	{reply, R, State};
handle_call(_Request, _From, State) ->
	{noreply, ok, State}.

handle_cast({incr, {Date, Key}, Amount}, State) ->
	incr({Date, Key}, Amount, State#rkn_state.ref),
	{noreply, State};
handle_cast({decr, {Date, Key}, Amount}, State) ->
	decr({Date, Key}, Amount, State#rkn_state.ref),
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
	ok = eput(Ref, {Date, Key}, CurrentValue + Amount, []).

decr({Date, Key}, Amount, Ref) ->
	{ok, CurrentValue} = eget(Ref, {Date, Key}),
	ok = eput(Ref, {Date, Key}, CurrentValue - Amount, []).

eget(Ref, Key) ->
  BinKey = sext:encode(Key),
	case eleveldb:get(Ref, BinKey, []) of
		{ok, V} -> {ok, binary_to_term(V)};
		_ -> {ok, 0}
	end.

eput(Ref, Key, Value, []) ->
  BinKey = sext:encode(Key),
  eleveldb:put(Ref, BinKey, term_to_binary(Value), []).

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
  {ok, Was} = rakna_node:get_counter(D, K),
  ok = rakna_node:increment({D, K}),
  {ok, Is} = rakna_node:get_counter(D, K),
  Is = Was + 1.0,
  {ok, Is}.

decr_test() ->
  {D,K} = {date(), <<"test_key">>},
  {ok, Was} = rakna_node:get_counter(D, K),
  ok = rakna_node:decrement({D, K}),
  {ok, Is} = rakna_node:get_counter(D, K),
  Is = Was - 1.0,
  {ok, Is}.

a_incr_test() ->
  {D,K} = {date(), <<"test_key">>},
  {ok, Was} = rakna_node:get_counter(D, K),
  rakna_node:a_increment({D, K}),
  timer:sleep(3600),
  {ok, Is} = rakna_node:get_counter(D, K),
  Is = Was + 1.0,
  {ok, Is}.

a_decr_test() ->
  {D,K} = {date(), <<"test_key">>},
  {ok, Was} = rakna_node:get_counter(D, K),
  rakna_node:a_decrement({D, K}),
  timer:sleep(3600),
  {ok, Is} = rakna_node:get_counter(D, K),
  Is = Was - 1.0,
  {ok, Is}.

incrby_test() ->
  Amount = 42,
  {D,K} = {date(), <<"incrby_test_key">>},
  {ok, Was} = rakna_node:get_counter(D, K),
  ok = rakna_node:increment({D, K}, Amount),
  {ok, Is} = rakna_node:get_counter(D, K),
  Is = Was + Amount,
  {ok, Is}.

a_incrby_test() ->
  Amount = 37,
  {D,K} = {date(), <<"a_incrby_test_key">>},
  {ok, Was} = rakna_node:get_counter(D, K),
  rakna_node:a_increment({D, K}, Amount),
  timer:sleep(3600),
  {ok, Is} = rakna_node:get_counter(D, K),
  Is = Was + Amount,
  {ok, Is}.

a_decrby_test() ->
  Amount = 18.4,
  {D,K} = {date(), <<"a_decrby_test_key">>},
  {ok, Was} = rakna_node:get_counter(D, K),
  rakna_node:a_decrement({D, K}, Amount),
  timer:sleep(3600),
  {ok, Is} = rakna_node:get_counter(D, K),
  Is = Was - Amount,
  {ok, Is}.

-endif.