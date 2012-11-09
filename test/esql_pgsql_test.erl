-module(esql_pgsql_test).

-include_lib("esql/include/esql.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DB,  [{username, "epgsql_test_md5"}, 
              {password, "epgsql_test_md5"},
	          {database, "epgsql_test_db1"}]).

open_single_database_test() -> 
    {ok, C} = esql:open(esql_pgsql, ?DB),
    ok = esql:close(C).

open_commit_close_test() ->
    {ok, C} = esql:open(esql_pgsql, ?DB),
    ok = esql:commit(C),  %% No transaction is started, but pgsql likes it anyway
    ok = esql:close(C).

open_rollback_close_test() ->
    {ok, C} = esql:open(esql_pgsql, ?DB),
    ok = esql:rollback(C), %% No transaction is started, but pgsql likes it anyway
    ok = esql:close(C).

sql_syntax_error_test() ->
    {ok, C} = esql:open(esql_pgsql, ?DB),
    {error, esql_pgsql, Msg} = esql:run("dit is geen sql", C),
    ok.

execute_test() ->
    {ok, C} = esql:open(esql_pgsql, ?DB),
    {ok, [id, value], [{1, <<"one">>}, {2, <<"two">>}]} = esql:execute("select * from test_table1;", C),
    {ok, [id, value], [{1, <<"one">>}]} = esql:execute("select * from test_table1 where id=1;", C),
    {ok, [value], [{<<"two">>}]} = esql:execute("select value from test_table1 where id=2;", C),
    {ok, [value], []} = esql:execute("select value from test_table1 where id=4;", C),    
    ok.

tables_test() ->
    %% 
    {ok, C} = esql:open(esql_pgsql, ?DB),
    [test_table1, test_table2] = esql:tables(C),
    ok.

describe_table_test() ->
    {ok, C} = esql:open(esql_pgsql, ?DB),
    [C1, C2] = esql:describe_table(test_table1, C),
    ok.

simple_pool_test() ->
    application:start(esql),
    {ok, Pid} = esql_pool:create_pool(test_pool, 10,
                                      [{serialized, false},
                                       {driver, esql_pgsql},
                                       {args, ?DB}]),

    {ok, _Cols, _Rows} = esql_pool:execute("select * from test_table1;", [], test_pool),

    esql_pool:delete_pool(test_pool),
    ok.

    
