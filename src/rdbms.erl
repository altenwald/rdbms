-module(rdbms_table).
-author('manuel@altenwald.com').
-compile([warnings_as_errors]).

-include_lib("sqlparser/include/sql.hrl").

-export([
    table_names/0,
    table_exists/1,
    tables_exists/1,
    attrib_names/1,
    attrib_exists/2,
    attribs_exists/2,

    %% commands...
    create_table/1,
    drop_table/1,
    insert_into/1
]).

-define(TIMEOUT_WAIT_FOR_TABLES, 3000).

table_names() ->
    [ atom_to_binary(T, utf8) || T <- mnesia:system_info(tables) ].

table_exists(Table) when is_binary(Table) ->
    lists:member(unistring:to_lower(Table), table_names()).

tables_exists(Tables) when is_list(Tables) ->
    AllTables = table_names(),
    {_, NotFound} = lists:partition(fun(T) when is_binary(T) ->
        lists:member(unistring:to_lower(T), AllTables)
    end, Tables),
    case NotFound of
        [] -> ok;
        _ -> {error, NotFound}
    end.

attrib_names(Table) when is_binary(Table) ->
    attrib_names(binary_to_atom(unistring:to_lower(Table), utf8));
attrib_names(Table) when is_atom(Table) ->
    [ atom_to_binary(A, utf8) || A <- mnesia:table_info(Table, attributes) ].

attrib_exists(Table, Attr) when is_binary(Table) andalso is_binary(Attr) ->
    lists:member(unistring:to_lower(Attr), attrib_names(Table)).

attribs_exists(Table, Attrs) when is_binary(Table) andalso is_list(Attrs) ->
    AllAttrs = attrib_names(Table),
    {_, NotFound} = lists:partition(fun(A) when is_binary(A) ->
        lists:member(unistring:to_lower(A), AllAttrs)
    end, Attrs),
    case NotFound of
        [] -> ok;
        _ -> {error, NotFound}
    end.

field_to_name(#field{name = Name}) when is_binary(Name) ->
    binary_to_atom(unistring:to_lower(Name), utf8).

create_table(#create_table{table = #table{name = Name}, fields = Fields}) ->
    case table_exists(Name) of
        true ->
            {error, eduplicate};
        false ->
            FieldNames = [ field_to_name(Field) || Field <- Fields ],
            TableName = binary_to_atom(unistring:to_lower(Name), utf8),
            case mnesia:create_table(TableName, [{attributes, FieldNames}]) of
                {atomic, ok} -> ok;
                Error -> Error
            end
    end.

drop_table(#drop_table{table = #table{name = Name}}) ->
    case table_exists(Name) of
        true ->
            TableName = binary_to_atom(unistring:to_lower(Name), utf8),
            case mnesia:delete_table(TableName) of
                {atomic, ok} -> ok;
                Error -> Error
            end;
        false ->
            {error, enoexists}
    end.

insert_into(#insert{values = []}) ->
    {error, enodata};
insert_into(#insert{table = #table{name = Name}, values = [#set{}|_] = Set}) ->
    case table_exists(Name) of
        true ->
            case attribs_exists(Name, [ N || #set{key = N} <- Set ]) of
                ok ->
                    TableName = binary_to_atom(unistring:to_lower(Name), utf8),
                    Object = create_object(TableName, Set),
                    mnesia:transaction(fun() ->
                        mnesia:write(Object)
                    end);
                {error, NotFound} ->
                    {error, {enoattr, NotFound}}
            end;
        false ->
            {error, enoexists}
    end;
insert_into(#insert{table = #table{name = Name}, values = Values}) ->
    case table_exists(Name) of
        true ->
            TableName = binary_to_atom(unistring:to_lower(Name), utf8),
            Attrs = mnesia:table_info(TableName, attributes),
            case {length(Attrs), length(Values)} of
                {N, N} ->
                    Object = lists:foldl(fun(V, Tuple) ->
                        erlang:append_element(Tuple, V)
                    end, {TableName}, Values),
                    mnesia:transaction(fun() ->
                        mnesia:write(Object)
                    end);
                {N, M} when N > M ->
                    {error, enoenoughattrib};
                {N, M} when N < M ->
                    {error, etoomanyvalues}
            end;
        false ->
            {error, enoexists}
    end.

create_object(Table, Set) ->
    lists:foldl(fun(Attr, Tuple) ->
        Val = case lists:keyfind(Attr, #set.key, Set) of
            false -> undefined;
            #set{value = #value{value = V}} -> V
        end,
        erlang:append_element(Tuple, Val)
    end, {Table}, attrib_names(Table)).
