-module(rdbms_table_test).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    rdbms_cluster:start_master(),
    ok.

teardown(_) ->
    rdbms_cluster:leave(rdbms),
    ok.

all_test_() ->
    {foreach,
        local,
        fun setup/0,
        fun teardown/1,
        [
            fun create_table_test/1
        ]
    }.

create_table_test(_) ->
    [
        ?_assertEqual(ok, rdbms_table:create_table(<<"users500">>, [<<"id">>, <<"name">>])),
        ?_assertEqual([<<"id">>, <<"name">>], rdbms_table:attrib_names(<<"users500">>))
    ].
