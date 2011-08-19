-module(rakna_utils).
-include_lib("eunit/include/eunit.hrl").

-export([exists/2, aggregate_key_for/3, aggregates_to_json_list/1]).

exists(Name, []) when is_atom(Name) ->
  false;
exists(Name, Options) when is_atom(Name), is_list(Options) ->
  case is_tuple(hd(Options)) of
    true ->
      lists:keymember(Name, 1, Options);
    false ->
      lists:any(fun(E) -> E =:= Name end, Options)
  end.

aggregate_key_for(Date, Label, Aggr) when is_tuple(Date), is_binary(Label), is_atom(Aggr) ->
  {Date, Label, Aggr}.

aggregates_to_json_list(PropList) ->
  AFun = fun(A) when is_atom(A) ->
    list_to_binary(atom_to_list(A))
  end,
  [{AFun(A), V} || {A, V} <- PropList].

-ifdef(TEST).
exists_test() ->
  Options = [bar, baz, quux, foo],
  true = exists(foo, Options),
  false = exists(bang, Options),
  OptionsProplist = [{aggregates, [min, max, avg, delta, last]}],
  true = exists(aggregates, OptionsProplist),
  false = exists(precision, OptionsProplist).

aggregate_key_for_test() ->
  {D, L, A} = {date(), <<"label">>, delta},
  {D, L, A} = aggregate_key_for(D, L, A).

aggregates_to_json_list_test() ->
  P = [{min, 0.0},{max, 14.3},{current, 14.3}],
  [
    {<<"min">>, 0.0},
    {<<"max">>, 14.3},
    {<<"current">>, 14.3}
  ] = aggregates_to_json_list(P).

-endif.