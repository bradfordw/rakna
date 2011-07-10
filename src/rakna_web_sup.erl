-module(rakna_web_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  {ok, Ip} = application:get_env(rakna_web_ip),
  {ok, Port} = application:get_env(rakna_web_port),
  {ok, LogDir} = application:get_env(rakna_web_log_dir),
  {ok, Dispatch} = application:get_env(rakna_web_dispatch),
  WebConfig = [
      {ip, Ip},
      {port, Port},
      {log_dir, LogDir},
      {dispatch, Dispatch}],
  Web = {webmachine_mochiweb,
      {webmachine_mochiweb, start, [WebConfig]}, permanent, 5000, worker, dynamic},
  Processes = [Web],
  {ok, {{one_for_one, 10, 10}, Processes}}.