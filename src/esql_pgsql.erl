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

-behaviour(esql).

-export([open/1, run/3, execute/3, execute/4, close/1, 
         start_transaction/1, commit/1, rollback/1, 
         tables/1, describe_table/2]).

open(Args) ->
    Host = proplists:get_value(host, Args, "localhost"),
    Username = proplists:get_value(username, Args, []),
    Password = proplists:get_value(password, Args, []),
    pgsql:connect(Host, Username, Password, Args).

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
            make_response([], Rows);
        {error, Reason} -> 
            {error, ?MODULE, Reason}
    end.

% 
execute(Sql, Args, Connection, Extra) ->
    % TODO
    ok.

make_response(Cols, Rows) ->
    ECols = [z_convert:to_atom(Col#column.name) || Col <- Cols],
    {ok, ECols, Rows}.

%%
%%
start_transaction(Connection) ->    
    run(<<"START TRANSACTION;">>, [], Connection).

%%
%%
commit(Connection) ->
    run(<<"COMMIT;">>, [], Connection).
    
%%
%%
rollback(Connection) ->
    run(<<"ROLLBACK;">>, [], Connection).

%%
%%
tables(Connection) ->
    {ok, [table_name], Names} = execute(<<"SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' 
                                                                             AND table_schema = current_schema();">>, [], Connection),
    [z_convert:to_atom(Name) || {Name} <- Names].

%%
%%
describe_table(Table, Connection) ->
    {ok, _, TableInfo} = execute(<<"SELECT c.column_name, data_type, c.column_default, is_nullable, t.constraint_type = 'PRIMARY KEY' AS pk
  FROM information_schema.columns c
    LEFT OUTER JOIN information_schema.key_column_usage u
      ON c.table_name = u.table_name 
      AND c.column_name = u.column_name
        LEFT OUTER JOIN information_schema.table_constraints t
        ON t.constraint_name = u.constraint_name 
        AND t.table_name = u.table_name
   WHERE c.table_name = $1 
   AND c.table_schema = current_schema()
   ORDER BY c.ordinal_position;">>, [z_convert:to_binary(Table)], Connection), 

    [ #esql_column_info{name=z_convert:to_atom(Name), 
                        type=Type, 
                        default=Default, 
                        notnull=not z_convert:to_bool(Nullable),
                        pk=Pk =:= true} 
      || {Name, Type, Default, Nullable, Pk} <- TableInfo].

    

