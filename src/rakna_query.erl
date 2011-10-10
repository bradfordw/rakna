-module(rakna_query).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rakna_query.hrl").

-export([select_all/1, select/2]).

select_all(Ref) ->
  eleveldb:fold(Ref, fun({K,V},Acc) -> [{sext:decode(K), binary_to_term(V)} | Acc] end, [], []).

select(Ref, #rq_predicate{interval=Interval, label=Label, aggregate=Agg, value=Value}) ->
  {ok, P} = build_predicate_clause(Interval, Label, Agg, Value),
  eleveldb:fold(Ref, P, [], []).

build_predicate_clause(Intervals, Labels, _Aggregates, _Values) ->
  {ok, PredInterval} = interval_predicate(Intervals),
  {ok, PredLabel} = label_predicate(Labels),
  % PredAggr = aggregate_predicate(Aggregates),
  % PredVal = value_predicate(Values),
  PFun = fun({K, V}, Acc) ->
    Key = sext:decode(K),
    Value = binary_to_term(V),
    P = case Key of
      {Interval, Label} ->
        fun() ->
          PredInterval(Interval) == true, PredLabel(Label) == true
        end
      {Interval, Label, _Agg} ->
        PredInterval(Interval) == true, PredLabel(Label) == true
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
    {no_in, L} ->
      fun(E) -> lists:member(E, L) == false end;
    L when is_binary(L), L =/= <<>> ->
      fun(E) -> E == L end;
    _ -> fun(_) -> true end
  end}.