-module(rakna_aggregates).
-include_lib("eunit/include/eunit.hrl").

-export([min/3, max/3, last/3, delta/3]).

min({Date, Label}, {_, Current}, Ref) ->
  Key = rakna_options:aggregate_key_for(Date, Label, min),
  {ok, CurMin} = rakna_node:eget(Ref, Key),
  case CurMin of
    0 ->
      rakna_node:eput(Ref, Key, Current);
    N when N > Current ->
      rakna_node:eput(Ref, Key, Current);
    _ -> ok
  end.

max({Date, Label}, {_, Current}, Ref) ->
  Key = rakna_options:aggregate_key_for(Date, Label, max),
  {ok, CurMax} = rakna_node:eget(Ref, Key),
  case Current > CurMax of
    true -> rakna_node:eput(Ref, Key, Current);
    _ -> ok
  end.

last({Date, Label}, {Previous, _}, Ref) ->
  Key = rakna_options:aggregate_key_for(Date, Label, last),
  rakna_node:eput(Ref, Key, Previous).

delta({Date, Label}, {Previous, Current}, Ref) ->
  Key = rakna_options:aggregate_key_for(Date, Label, delta),
  rakna_node:eput(Ref, Key, Current - Previous).

-ifdef(TEST).
start_test() ->
  os:cmd("rm -rf /tmp/rakna_test.ldb"),
  case whereis(rakna_node) of P
    when is_pid(P) -> ok;
    _ ->
      Options = [{aggregates, [min, max, delta, last]}],
      {ok, _} = rakna_node:start_link("/tmp/rakna_test.ldb", Options),
      ok
  end.

min_test() ->
  {D, L} = {date(), <<"min_test_key">>},
  {ok, _} = rakna_node:increment(L),
  {ok, _} = rakna_node:increment(L, 12),
  {ok, PList} = rakna_node:get_counter(D, L, [min]),
  true = rakna_options:exists(min, PList),
  true = rakna_options:exists(current, PList),
  1.0 = proplists:get_value(min, PList),
  13.0 = proplists:get_value(current, PList),
  ok.

max_test() ->
  {D, L} = {date(), <<"max_test_key">>},
  {ok, _} = rakna_node:increment(L),
  {ok, _} = rakna_node:increment(L, 42),
  {ok, _} = rakna_node:decrement(L, 1),
  {ok, PList} = rakna_node:get_counter(D, L, [max]),
  true = rakna_options:exists(max, PList),
  true = rakna_options:exists(current, PList),
  43.0 = proplists:get_value(max, PList),
  42.0 = proplists:get_value(current, PList),
  ok.

last_test() ->
  {D, L} = {date(), <<"last_test_key">>},
  {ok, _} = rakna_node:increment(L),
  {ok, _} = rakna_node:increment(L, 15),
  {ok, PList} = rakna_node:get_counter(D, L, [last]),
  true = rakna_options:exists(last, PList),
  true = rakna_options:exists(current, PList),
  1.0 = proplists:get_value(last, PList),
  16.0 = proplists:get_value(current, PList),
  ok.

delta_test() ->
  {D, L} = {date(), <<"delta_test_key">>},
  {ok, _} = rakna_node:increment(L),
  {ok, _} = rakna_node:decrement(L, 15),
  {ok, PList} = rakna_node:get_counter(D, L, [delta]),
  true = rakna_options:exists(delta, PList),
  true = rakna_options:exists(current, PList),
  -15.0 = proplists:get_value(delta, PList),
  -14.0 = proplists:get_value(current, PList),
  ok.

-endif.