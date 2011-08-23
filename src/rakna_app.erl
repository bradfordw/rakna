-module(rakna_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  {ok, RaknaOptions} = application:get_env(rakna_options),
  case rakna_utils:exists(nodes, RaknaOptions) of
    true ->
      case proplists:get_value(nodes, RaknaOptions) of
        [] -> ok;
          Nodes when is_list(Nodes) ->
        [net_adm:ping(Node) || Node <- Nodes]
      end;
    false -> ok
  end,
  rakna_sup:start_link(),
  rakna_web_sup:start_link().

stop(_State) ->
    ok.
