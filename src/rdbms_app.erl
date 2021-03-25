-module(rdbms_app).
-author('manuel@altenwald.com').

-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    rdbms_sup:start_link().

stop(_Status) ->
    ok.
