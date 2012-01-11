%%
%% Exploring making a database driver for the generic gen_db interface
%%

-module(esql_pgsql).

-include_lib("esql/include/esql.hrl").
-include_lib("pgsql/include/pgsql.hrl").
-include_lib("zutils/include/zutils.hrl").

-behaviour(esql).

-export([open/1, run/3,  execute/3, close/1, start_transaction/1, commit/1, rollback/1, tables/1, describe_table/2]).

open(Args) ->
    Host = proplists:get_value(host, Args, "localhost"),
    Username = proplists:get_value(username, Args, []),
    Password = proplists:get_value(password, Args, []),
    {ok, C} = pgsql:connect(Host, Username, Password, Args).

close(Connection) ->
    ok = pgsql:close(Connection).

% execute the query, without returning results
run(Sql, Args, Connection) ->
    %% TODO: replace with a version which doesn't actually load the
    %% results.
    case pgsql:equery(Connection, Sql, Args) of
	{ok, _Affected, _Cols, _Rows} -> ok;
	{ok, _Cols, _Rows} -> ok;
	{ok, _Rows} -> ok;
	{error, Reason} -> {error, ?MODULE, Reason}
    end.
    
% execute the query, return the results
execute(Sql, Args, Connection) ->
    case pgsql:equery(Connection, Sql, Args) of
	{ok, _Affected, Cols, Rows} -> 
	    make_response(Cols, Rows);
	{ok, Cols, Rows} -> 
	    make_response(Cols, Rows);
	{ok, Rows} -> 
	    make_response(undefined, Rows);
	{error, Reason} -> 
	    {error, ?MODULE, Reason}
    end.

make_response(Cols, Rows) ->
    ?DEBUG(Cols),
    ECols = [z_convert:to_atom(B) || {_, B, _} <- Cols],
    ERows = [ V  || {V, _} <- Rows],
    ?DEBUG({ok, ECols, ERows}).

start_transaction(Connection) ->    
    run(<<"START TRANSACTION;">>, [], Connection).

commit(Connection) ->
    run(<<"COMMIT;">>, [], Connection).
    
rollback(Connection) ->
    run(<<"ROLLBACK;">>, [], Connection).

tables(_Connection) ->
    {}.

describe_table(_Table, _Connection) ->
    undefined.
    

