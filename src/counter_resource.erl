-module(counter_resource).
-export([init/1,
        service_available/2,
        allowed_methods/2,
        content_types_provided/2,
        content_types_accepted/2,
        get/2]).

-include_lib("webmachine/include/webmachine.hrl").

init([]) ->
  {ok, []}.

allowed_methods(Req, State) ->
  {['GET'], Req, State}.

content_types_provided(Req, Ctx) ->
	{[{"application/json", get}], Req, Ctx}.

content_types_accepted(Req, Ctx) ->
  {[{"application/json", get}], Req, Ctx}.

service_available(Req, Ctx) ->
  Availability = case whereis(rakna_node) of
    P when is_pid(P) -> true;
    _ -> false
  end,
	{Availability, Req, Ctx}.

get(Req, Ctx) ->
  Response = case wrq:path_info(counter, Req) of
    Label when is_list(Label) ->
      Date = get_date(wrq:get_qs_value("date", date(), Req)),
      Aggregates = get_aggregates(wrq:get_qs_value("aggregates", [], Req)),
      CounterName = list_to_binary(Label),
      query_counter(Date, CounterName, Aggregates);
    _ ->
      {halt, 404}
  end,
  {Response, Req, Ctx}.

%% Internal Functions

parse_date(Date) when is_binary(Date) ->
  case Date of
    <<Y:4/binary,_:1/binary,M:1/binary,_:1/binary,D:1/binary>> ->
      {list_to_integer(binary_to_list(Y)), list_to_integer(binary_to_list(M)), list_to_integer(binary_to_list(D))};
    <<Y:4/binary,_:1/binary,M:1/binary,_:1/binary,D:2/binary>> ->
      {list_to_integer(binary_to_list(Y)), list_to_integer(binary_to_list(M)), list_to_integer(binary_to_list(D))};
    <<Y:4/binary,_:1/binary,M:2/binary,_:1/binary,D:1/binary>> ->
      {list_to_integer(binary_to_list(Y)), list_to_integer(binary_to_list(M)), list_to_integer(binary_to_list(D))};
    <<Y:4/binary,_:1/binary,M:2/binary,_:1/binary,D:2/binary>> ->
      {list_to_integer(binary_to_list(Y)), list_to_integer(binary_to_list(M)), list_to_integer(binary_to_list(D))}
  end.

get_date(Date) ->
  case Date of
    D when is_tuple(D) -> D;
    D when is_list(D) -> parse_date(list_to_binary(D));
    D when is_binary(D) -> parse_date(D)
  end.

get_aggregates(Aggregates) ->
  case Aggregates of
    [] -> [];
    A when is_list(A) ->
      filter_aggregates(string:tokens(A,","));
    A when is_binary(A) ->
      filter_aggregates(string:tokens(binary_to_list(A),","))
  end.
  
filter_aggregates(Ags) when is_list(Ags) ->
  {ok, Opts} = rakna_node:get_options(aggregates),
  Allowed = [atom_to_list(A) || A <- Opts],
  Matched = lists:filter(fun(E) -> lists:any(fun(E1) -> E1 =:= E end, Allowed) end, Ags),
  [list_to_atom(A1) || A1 <- Matched].

query_counter(Date, Label, Aggregates) ->
  {ok, Result} = rakna_node:get_counter(Date, Label, Aggregates),
  Json = rakna_utils:aggregates_to_json_list(Result) ++ [{<<"name">>, Label}],
  mochijson2:encode({struct, Json}).
