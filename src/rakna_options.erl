-module(rakna_options).
-include_lib("eunit/include/eunit.hrl").

-export([exists/2, aggregate_key_for/3]).

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

-endif.