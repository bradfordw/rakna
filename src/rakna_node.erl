-module(rakna_node).

-behaviour(gen_server).
-include_lib("eunit/include/eunit.hrl").

% synchronous exports
-export([start_link/0, start_link/1,  
	get_counter/2,
	delete_counter/1,
	delete_counter/2,
	increment/1,
	increment/2,
	decrement/1,
	decrement/2,
	eget/2,
	eput/4,
	edelete/3,
	incr/3,
	decr/3
]).

% asynchronous exports
% -export([a_increment/1,
%   a_increment/2,
%   a_increment/3,
%   a_decrement/1,
%   a_decrement/2,
%   a_decrement/3
% ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(counter_state, {level_ref}).

%% API
start_link(LevelDbPath) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [LevelDbPath], []).

start_link() ->
  {ok, LevelDbPath} = application:get_env(rakna_leveldb_path),
	gen_server:start_link({local, ?MODULE}, ?MODULE, [LevelDbPath], []).

%% synchronous API
delete_counter(Key) ->
  delete_counter(date(), Key).
delete_counter(Date, Key) ->
  gen_server:call(?MODULE, {delete, Date, Key}).

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
% a_increment(Key) ->
%   gen_server:cast(?MODULE, {incr, Key}).
% 
% a_increment(Key, Amount) when is_integer(Amount) ->
%   FloatAmt = (Amount * 1.0),
%   a_increment(Key, FloatAmt);
% a_increment(Key, Amount) when is_float(Amount) ->
%   gen_server:cast(?MODULE, {incrby, date(), Key, Amount});
% a_increment(Date, Key) ->
%   gen_server:cast(?MODULE, {incr, Date, Key}).
% 
% a_increment(Date, Key, Amount) when is_integer(Amount) ->
%   FloatAmt = (Amount * 1.0),
%   a_increment(Date, Key, FloatAmt);
% a_increment(Date, Key, Amount) when is_float(Amount) ->
%   gen_server:cast(?MODULE, {incrby, Date, Key, Amount}).

% a_decrement(Key) ->
%   gen_server:cast(?MODULE, {decr, Key}).
% 
% a_decrement(Key, Amount) when is_integer(Amount) ->
%   FloatAmt = (Amount * 1.0),
%   a_decrement(Key, FloatAmt);
% a_decrement(Key, Amount) when is_float(Amount) ->
%   gen_server:cast(?MODULE, {decrby, date(), Key, Amount});
% a_decrement(Date, Key) ->
%   gen_server:cast(?MODULE, {decr, Date, Key}).
% 
% a_decrement(Date, Key, Amount) when is_integer(Amount) ->
%   FloatAmt = (Amount * 1.0),
%   a_decrement(Date, Key, FloatAmt);
% a_decrement(Date, Key, Amount) when is_float(Amount) ->
%   gen_server:cast(?MODULE, {decrby, Date, Key, Amount}).

%% gen_server exports
init([LevelDbPath]) ->
	{ok, Ref} = eleveldb:open(LevelDbPath, [{create_if_missing, true}]),
	{ok, #counter_state{level_ref = Ref}}.

handle_call({get, {Date, Key}}, _From, State) ->
	{ok, CurrentValue} = eget(State#counter_state.level_ref, {Date, Key}),
	{reply, {ok, CurrentValue}, State};
handle_call({incr, {Date, Key}, Amount}, _From, State) ->
	R = incr({Date, Key}, Amount, State#counter_state.level_ref),
	{reply, R, State};
handle_call({decr, {Date, Key}, Amount}, _From, State) ->
	R = decr({Date, Key}, Amount, State#counter_state.level_ref),
	{reply, R, State};
handle_call({delete, Date, Key}, _From, State) ->
  R = edelete(State#counter_state.level_ref, {Date, Key}, []),
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

edelete(Ref, Key, []) ->
  BinKey = sext:encode(Key),
  eleveldb:delete(Ref, BinKey, []).

-ifdef(TEST).
%% Tests
start_test() ->
  os:cmd("rm -rf /tmp/rakna_test.ldb"),
  case whereis(rakna_node) of P
    when is_pid(P) -> ok;
    _ ->
      {ok, _} = ?MODULE:start_link("/tmp/rakna_test.ldb"),
      ok
  end.

incr_test() ->
  % ok = ?TEST_NODE_CONNECT(),
  {D,K} = {date(), <<"test_key">>},
  {ok, Was} = rakna_node:get_counter(D, K),
  ok = rakna_node:increment({D, K}),
  {ok, Is} = rakna_node:get_counter(D, K),
  Is = Was + 1.0,
  {ok, Is}.

decr_test() ->
  % ok = ?TEST_NODE_CONNECT(),
  {D,K} = {date(), <<"test_key">>},
  {ok, Was} = rakna_node:get_counter(D, K),
  ok = rakna_node:decrement({D, K}),
  {ok, Is} = rakna_node:get_counter(D, K),
  Is = Was - 1.0,
  {ok, Is}.

-endif.