-module(rakna_node).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(node_state,{local_counter_store, remote_counter_store}).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(Args) ->
	{ok, #node_state{}}.

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