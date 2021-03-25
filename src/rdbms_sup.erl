-module(rdbms_sup).
-author('manuel@altenwald.com').

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = [],

    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },

    {ok, {SupFlags, Children}}.
