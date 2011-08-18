-module(rakna_options).
-include_lib("eunit/include/eunit.hrl").

-export([exists/2, atom_to_binary/1, aggregate_key_for/3]).

exists(Name, Options) when is_atom(Name) ->
  lists:any(fun(E) -> E =:= Name end, Options).
  
atom_to_binary(Name) when is_atom(Name) ->
  list_to_binary(atom_to_list(Name)).

aggregate_key_for(Date, Label, Aggr) when is_tuple(Date), is_binary(Label), is_atom(Aggr) ->
  aggregate_key_for(Date, Label, atom_to_binary(Aggr));
aggregate_key_for(Date, Label, Aggr) when is_tuple(Date), is_binary(Label), is_binary(Aggr) ->
  term_to_binary({Date, Label, Aggr}).

-ifdef(TEST).
exists_test() ->
  Options = [bar, baz, quux, foo],
  true = exists(foo, Options).

atom_to_binary_test() ->
  Abin = <<"bin">>,
  Atom = bin,
  Abin = atom_to_binary(Atom).

aggregate_key_for_test() ->
  {D,L,A} = {date(), <<"label">>, delta},
  A2 = atom_to_binary(A),
  K = term_to_binary({D, L, A2}),
  Akf = aggregate_key_for(D,L,A),
  K = Akf.

-endif.