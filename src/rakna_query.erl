-module(rakna_query).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rakna_query.hrl").

-export([select_all/1, select/2]).

select_all(Ref) ->
  eleveldb:fold(Ref, fun({K,V},Acc) -> [{sext:decode(K), binary_to_term(V)} | Acc] end, [], []).

select(Ref, #rq_predicate{interval=Interval, label=Label, aggregate=Agg, value=Value}) ->
  {ok, P} = build_predicate_clause(Interval, Label, Agg, Value),
  eleveldb:fold(Ref, P, [], []).

build_predicate_clause(Intervals, Labels, Aggregates, Values) ->
  {ok, PredInterval} = interval_predicate(Intervals),
  {ok, PredLabel} = label_predicate(Labels),
  {ok, PredAggr} = aggregate_predicate(Aggregates),
  {ok, PredVal} = value_predicate(Values),
  PFun = fun({K, V}, Acc) ->
    Key = sext:decode(K),
    Value = binary_to_term(V),
    P = case Key of
      {Interval, Label} ->
          PredInterval(Interval) == true andalso PredLabel(Label) == true andalso PredVal(Value);
      {Interval, Label, Aggr} ->
        PredInterval(Interval) == true andalso PredLabel(Label) == true andalso PredAggr(Aggr) andalso PredVal(Value)
    end,
    case P of
      true -> [{Key, Value} | Acc];
      false -> Acc
    end
  end,
  {ok, PFun}.

interval_predicate(Intervals) ->
  {ok, case Intervals of
    I when is_list(I), I =/= [] ->
      fun(E) -> lists:member(E, I) end;
    {between, I1, I2} ->
      fun(E) -> edate:is_after(E, I1) == edate:is_before(E, I2) end;
    {less_than, I} ->
      fun(E) -> edate:is_before(E, I) end;
    {greater_than, I} ->
      fun(E) -> edate:is_after(E, I) end;
    {not_in, I} ->
      fun(E) -> lists:member(E, I) == false end;
    I when is_tuple(I), tuple_size(I) == 3 ->
      fun(E) -> E == I end;
    _ -> fun(_) -> true end
  end}.

label_predicate(Labels) ->
  {ok, case Labels of
    L when is_list(L), L =/= [] ->
      fun(E) -> lists:member(E, L) end;
    {not_in, L} ->
      fun(E) -> lists:member(E, L) == false end;
    L when is_binary(L), L =/= <<>> ->
      fun(E) -> E == L end;
    _ -> fun(_) -> true end
  end}.
  
aggregate_predicate(Aggregates) ->
  {ok, case Aggregates of
    A when is_list(A), A =/= [] ->
      fun(E) -> lists:member(E, A) end;
    all ->
      fun(_) -> true end;
    A when is_atom(A) ->
      fun(E) -> E == A end;
    [] ->
      fun(_) -> false end;
    _ ->
      fun(_) -> false end
  end}.

value_predicate(Values) ->
  {ok, case Values of
    V when is_list(V), V =/= [] ->
      fun(E) -> lists:member(E, V) end;
    {greater_than, V} ->
      fun(E) -> E > V end;
    {less_than, V} ->
      fun(E) -> E < V end;
    {greater_than_eq, V} ->
      fun(E) -> E >= V end;
    {less_than_eq, V} ->
      fun(E) -> E =< V end;
    {between, V1, V2} ->
      fun(E) -> E >= V1 andalso E =< V2 end;
    V when is_number(V) ->
      fun(E) -> E == V end;
    [] ->
      fun(_) -> true end;
    _ ->
      fun(_) -> true end
  end}.

-ifdef(TEST).

interval_predicate_test() ->
  Members = [{2011,10,10},{2011,10,11}],
  {ok, FTest1} = interval_predicate(Members),
    true = FTest1(hd(Members)),
    true = FTest1(lists:last(Members)),
    false = FTest1({2011,10,12}),
  Single = {2011,10,9},
  {ok, FTest2} = interval_predicate(Single),
    true = FTest2(Single),
    false = FTest2({2011,10,10}),
  {ok, FTest3} = interval_predicate(invalid_date),
    true = FTest3(invalid_date),
  StartDate = {2011,10,1}, EndDate = {2011,10,31},
  InRange = {2011,10,15}, OutRange = {2011,11,26},
  {ok, FTest4} = interval_predicate({between, StartDate, EndDate}),
    true = FTest4(InRange),
    false = FTest4(OutRange),
  {ok, FTest5} = interval_predicate({less_than, EndDate}),
    true = FTest5(InRange),
    false = FTest5(OutRange),
  {ok, FTest6} = interval_predicate({greater_than, EndDate}),
    true = FTest6(OutRange),
    false = FTest6(InRange),
  {ok, FTest7} = interval_predicate({not_in, Members}),
    true = FTest7({2011, 10, 14}),
    false = FTest7({2011, 10, 10}),
  ok.

label_predicate_test() ->

  ok.

aggregate_predicate_test() ->

  ok.

value_predicate_test() ->

  ok.
-endif.