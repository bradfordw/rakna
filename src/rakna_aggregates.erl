-module(rakna_aggregates).
-include_lib("eunit/include/eunit.hrl").

-export([min/3, max/3, last/3, delta/3]).

min({Date, Label}, {_, Current}, Ref) ->
  Key = rakna_options:aggregate_key_for(Date, Label, min),
  {ok, CurMin} = rakna_node:eget(Ref, Key),
  case Current < CurMin of
    true -> rakna_node:eput(Ref, Key, Current);
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

-endif.