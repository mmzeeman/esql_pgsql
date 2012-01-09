%%
%% Exploring making a database driver for the generic gen_db interface
%%

-module(esql_pgsql).

-include_lib("esql/include/esql.hrl").

-behaviour(esql).

-export([open/1, run/3,  execute/3, close/1, start_transaction/1, commit/1, rollback/1, tables/1, describe_table/2]).

open(_Args) ->
    {ok, fake_connection}.

close(_Connection) ->
    ok.

run(_Sql, _Args, _Connection) ->
    ok.
    
execute(_Sql, _Args, _Connection) ->
    {ok, [], []}.


start_transaction(_Connection) ->    
    ok.

commit(_Connection) ->
    ok.

rollback(_Connection) ->
    ok.

tables(_Connection) ->
    {}.

describe_table(_Table, _Connection) ->
    undefined.
    

