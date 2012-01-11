%% @author Maas-Maarten Zeeman <mmzeeman@xs4all.nl>
%% @copyright 2012 Maas-Maarten Zeeman

%% @doc Erlang driver for postgres databases

%% Copyright 2012 Maas-Maarten Zeeman
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% 
%%     http://www.apache.org/licenses/LICENSE-2.0
%% 
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

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
	{ok, _Affected, Cols, Rows} -> make_response(Cols, Rows);
	{ok, Cols, Rows} -> make_response(Cols, Rows);
	{ok, Rows} -> make_response([], Rows);
	{error, Reason} -> 
	    {error, ?MODULE, Reason}
    end.

make_response(Cols, Rows) ->
    ECols = [z_convert:to_atom(Col#column.name) || Col <- Cols],
    {ok, ECols, Rows}.

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
    

