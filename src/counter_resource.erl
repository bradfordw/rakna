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
  Availability = case whereis(rakna_counter) of
    P when is_pid(P) -> true;
    _ -> false
  end,
	{Availability, Req, Ctx}.

get(Req, Ctx) ->
  Response = case wrq:path_info(counter, Req) of
    Label when is_list(Label) ->
      Date = case wrq:get_qs_value("date", date(), Req) of
        D when is_tuple(D) -> D;
        D when is_list(D) -> parse_date(list_to_binary(D));
        D when is_binary(D) -> parse_date(D)
      end,
      CounterName = list_to_binary(Label),
      {ok, CurrentValue} = rakna_counter:get_counter(Date, CounterName),
      mochijson2:encode({struct, [
        {<<"name">>, CounterName},
        {<<"total">>, CurrentValue}
      ]});
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