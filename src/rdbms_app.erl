-module(rdbms_app).
-author('Manuel Rubio <manuel@altenwald.com>').

-behaviour(application).
-behaviour(supervisor).

%% Application callbacks
-export([start/2, stop/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, J), {I, {I, start_link, J}, permanent, 5000, worker, [I]}).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
    ok.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Strategy = one_for_one,
    Intensity = 5,
    Period = 10,
    {ok, {{Strategy, Intensity, Period}, []}}.
