-module(rdbms_table).
-author('manuel@altenwald.com').

-include_lib("sqlparser/include/sql.hrl").

% -compile([nowarn_export_all, export_all]).

-export([
    table_names/0,
    table_exists/1,
    tables_exists/1,
    attrib_names/1,
    attrib_exists/2,
    attribs_exists/2,

    %% commands...
    create_table/1,
    create_table/2,
    drop_table/1,
    insert_into/1,
    insert_into/3,
    select/1,
    select/7
]).

-type table() :: binary() | atom().
-type attrib() :: binary() | atom().

-spec table_name(table()) -> atom().
table_name(Name) when is_binary(Name) ->
    binary_to_atom(unistring:to_lower(Name), utf8);

table_name(Name) when is_atom(Name) -> Name.

-spec table_names() -> [table()].
table_names() ->
    [ atom_to_binary(T, utf8) || T <- mnesia:system_info(tables) ].

-spec table_exists(table()) -> boolean().
table_exists(Table) when is_atom(Table) ->
    table_exists(atom_to_binary(Table, utf8));

table_exists(Table) when is_binary(Table) ->
    lists:member(unistring:to_lower(Table), table_names()).

-spec tables_exists([table()]) -> ok | {error, [table()]}.
tables_exists(Tables) when is_list(Tables) ->
    AllTables = table_names(),
    {_, NotFound} = lists:partition(fun(T) when is_binary(T) ->
        lists:member(unistring:to_lower(T), AllTables)
    end, Tables),
    case NotFound of
        [] -> ok;
        _ -> {error, NotFound}
    end.

-spec attrib_names(table()) -> [attrib()] | {error, enoexists}.
attrib_names(Table) when is_binary(Table) orelse is_atom(Table) ->
    case table_exists(Table) of
        true ->
            TableName = table_name(Table),
            [ atom_to_binary(A, utf8) || A <- mnesia:table_info(TableName, attributes) ];
        false ->
            {error, enoexists}
    end.

-spec attrib_exists(table(), attrib()) -> boolean().
attrib_exists(Table, Attr) when is_binary(Table) andalso is_binary(Attr) ->
    case attrib_names(Table) of
        {error, enoexists} -> false;
        Fields -> lists:member(unistring:to_lower(Attr), Fields)
    end.

-spec attribs_exists(table(), [attrib()]) -> ok | {error, [attrib()]}.
attribs_exists(Table, Attrs) when is_binary(Table) andalso is_list(Attrs) ->
    AllAttrs = attrib_names(Table),
    {_, NotFound} = lists:partition(fun(A) when is_binary(A) ->
        lists:member(unistring:to_lower(A), AllAttrs)
    end, Attrs),
    case NotFound of
        [] -> ok;
        _ -> {error, NotFound}
    end.

-spec field_to_name(#field{}) -> atom().
field_to_name(#field{name = Name}) when is_binary(Name) ->
    binary_to_atom(unistring:to_lower(Name), utf8).

-spec create_table(#create_table{}) -> ok | {error, eduplicate | atom()}.
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

-spec name_to_field(attrib()) -> #field{}.
name_to_field(Attrib) when is_atom(Attrib) ->
    name_to_field(atom_to_binary(Attrib, utf8));

name_to_field(Attrib) when is_binary(Attrib) ->
    #field{name = Attrib}.

-spec create_table(table(), [attrib()]) -> ok | {error, eduplicate | atom()}.
create_table(Name, Attribs) when is_atom(Name) ->
    create_table(atom_to_binary(Name, utf8), Attribs);

create_table(Name, Attribs) when is_binary(Name) andalso is_list(Attribs) ->
    Fields = [ name_to_field(Attrib) || Attrib <- Attribs ],
    create_table(#create_table{table = #table{name = Name}, fields = Fields}).

-spec drop_table(#drop_table{} | table()) -> ok | {error, enoexists | atom()}.
drop_table(Name) when is_atom(Name) ->
    NameBin = atom_to_binary(Name, utf8),
    drop_table(NameBin);

drop_table(Name) when is_binary(Name) ->
    drop_table(#drop_table{table = #table{name = Name}});

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

-spec insert_into(#insert{}) -> ok | {error, term()}.
insert_into(#insert{values = []}) ->
    {error, enodata};
insert_into(#insert{table = #table{name = Name}, values = [#set{}|_] = Set}) ->
    case table_exists(Name) of
        true ->
            case attribs_exists(Name, [ N || #set{key = N} <- Set ]) of
                ok ->
                    TableName = binary_to_atom(unistring:to_lower(Name), utf8),
                    Object = create_object(TableName, Set),
                    {atomic, Result} = mnesia:transaction(fun() ->
                        mnesia:write(Object)
                    end),
                    Result;
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
                    {atomic, Result} = mnesia:transaction(fun() ->
                        mnesia:write(Object)
                    end),
                    Result;

                {N, M} when N > M ->
                    {error, enoenoughattrib};

                {N, M} when N < M ->
                    {error, etoomanyvalues}
            end;

        false ->
            {error, enoexists}
    end.

-spec insert_into(table(), [attrib()], Values :: [term()]) -> ok | {error, term()}.
insert_into(Table, Attribs, Values) when is_atom(Table) ->
    insert_into(atom_to_binary(Table, utf8), Attribs, Values);

insert_into(Table, Attribs, Values) when is_binary(Table) andalso is_list(Attribs) andalso is_list(Values) ->
    AttrVals = lists:zip(Attribs, Values),
    Vals = [ #set{key = K, value = #value{value = V}} || {K, V} <- AttrVals ],
    insert_into(#insert{table = #table{name = Table}, values = Vals}).

-spec create_object(table(), [#set{}]) -> tuple().
create_object(Table, Set) ->
    lists:foldl(fun(Attr, Tuple) ->
        Val = case lists:keyfind(Attr, #set.key, Set) of
            false -> undefined;
            #set{value = #value{value = V}} -> V
        end,
        erlang:append_element(Tuple, Val)
    end, {Table}, attrib_names(Table)).

select([Name], [#all{table = undefined}], _Params, undefined, undefined, Order, Window) ->
    {atomic, Result} =
        mnesia:transaction(fun() ->
            TableName = table_name(Name),
            [ hd(mnesia:read(TableName, X)) || X <- mnesia:all_keys(TableName) ]
        end),
    window(Window, Order(Result));

select([Name], _Selection, Params, Guard, undefined, Order, Window) ->
    MatchHead = get_selection_choices(Name, get_selection_order(Params)),
    Result = '$_',
    Sel = [
        {MatchHead, [Guard], [Result]}
    ],
    {atomic, Return} = mnesia:transaction(fun() ->
        mnesia:select(table_name(Name), Sel)
    end),
    window(Window, Order(Return)).

-spec select(#select{}) -> [tuple()] | {error, term()}.
select(#select{params = Selection,
               tables = TableNames,
               conditions = Condition,
               group = Group,
               order = OrderRecords,
               limit = Limit,
               offset = Offset}) ->
    Tables = [ table_to_name(T) || T <- TableNames ],
    Params = selection_to_params(Selection, Tables),
    OrdSel = get_selection_order(Params),
    Cond = condition_to_matchspec(Condition, OrdSel),
    Order = build_order_lambda(OrderRecords, OrdSel),
    select(Tables, Selection, Params, Cond, Group, Order, {Limit, Offset}).

window({undefined, undefined}, Data) -> Data;
window({Limit, undefined}, Data) -> lists:sublist(Data, Limit);
window({Limit, Offset}, Data) -> lists:sublist(Data, Offset, Limit).

table_to_name(#table{name = Name, alias = undefined}) -> Name;
table_to_name(#table{name = Name, alias = Name}) -> Name;
table_to_name(#table{name = Name, alias = Alias}) -> {Name, Alias}.

build_tables_map(Tables) ->
    lists:foldl(fun
        ({Name, Alias}, Map0) ->
            Map1 = maps:put(Alias, {Name, attrib_names(Name)}, Map0),
            maps:put(Name, {Name, attrib_names(Name)}, Map1);

        (Name, Map) when is_binary(Name) ->
            maps:put(Name, {Name, attrib_names(Name)}, Map)
    end, #{}, Tables).

search_field_into_map(Key, TableMap) ->
    Result = maps:fold(fun(_, {Name, Keys}, Acc) ->
        case lists:member(Key, Keys) of
            true -> [{Name, Keys}|Acc];
            false -> Acc
        end
    end, [], TableMap),
    lists:ukeysort(1, Result).

get_selection_choices(Name, OrderMap) ->
    lists:foldl(fun(Attr, Tuple) ->
        Val = case maps:get(Attr, OrderMap, error) of
            error -> '_';
            I -> list_to_atom([ $$ | integer_to_list(I) ])
        end,
        erlang:append_element(Tuple, Val)
    end, {'_'}, attrib_names(Name)).

selection_to_params(Keys, Tables) ->
    TableMap = build_tables_map(Tables),
    lists:foldr(fun
        (#all{table = undefined}, Acc) ->
            lists:flatten(lists:map(fun({Alias, {Name, Fields}}) ->
                [ {Alias, Name, Field} || Field <- Fields ]
            end, maps:to_list(TableMap))) ++ Acc;
        (#key{alias = undefined, table = undefined, name = Key}, Acc) ->
            TableSel = search_field_into_map(Key, TableMap),
            case length(TableSel) of
                0 -> throw({error, {enoexistsfield, Key}});
                1 ->
                    [{TableChosen, _}] = maps:values(TableSel),
                    [{TableChosen, TableChosen, Key} | Acc];
                _ -> throw({error, {eambiguous, Key}})
            end;
        (#key{alias = undefined, table = Name, name = Key}, Acc) ->
            case maps:get(Name, TableMap, undefined) of
                undefined -> throw({error, {enoexiststable, Name}});
                {Name, Keys} ->
                    case lists:member(Key, Keys) of
                        true -> [{Name, Name, Key} | Acc];
                        false -> throw({error, {enoexistsfield, Key}})
                    end
            end;
        (#key{alias = Alias, table = Name, name = Key}, Acc) ->
            case maps:get(Alias, TableMap, undefined) of
                undefined -> throw({error, {enoexistsalias, Alias}});
                {Name, Keys} ->
                    case lists:member(Key, Keys) of
                        true -> [{Alias, Name, Key} | Acc];
                        false -> throw({error, {enoexistsfield, Key}})
                    end
            end
    end, [], Keys).

get_selection_order(Params) ->
    get_selection_order(Params, #{}, 0).

get_selection_order([], Result, _I) -> Result;
get_selection_order([{Alias, Name, Key} | More], Acc0, I) ->
    Acc1 = maps:put(Key, I+1, Acc0),
    Acc2 = maps:put({Name, Key}, I+1, Acc1),
    Acc3 = maps:put({Alias, Key}, I+1, Acc2),
    get_selection_order(More, Acc3, I+1).

build_order_lambda(undefined, _SelOrder) ->
    fun(Data) -> Data end;

build_order_lambda(Order, SelOrder) ->
    Fn = fun(_, _) -> {cont, true} end,
    fun(Data) ->
        Func = fun(T1, T2) ->
            Func = build_order_lambda(Order, Fn, SelOrder),
            {_, Result} = Func(T1, T2),
            Result
        end,
        lists:sort(Func, Data)
    end.

build_order_lambda([], Acc, _SelOrder) -> Acc;
build_order_lambda([#order{key = Key, sort = Sort} | Rest], Acc, SelOrder) ->
    KeyPos = maps:get(Key, SelOrder) + 1,
    NewAcc = case Sort of
        asc ->
            fun(Tuple1, Tuple2) ->
                case Acc(Tuple1, Tuple2) of
                    {cont, true} ->
                        Elem1 = element(KeyPos, Tuple1),
                        Elem2 = element(KeyPos, Tuple2),
                        if
                            Elem1 =:= Elem2 -> {cont, true};
                            true -> {break, Elem1 =< Elem2}
                        end;
                    {break, Result} -> {break, Result}
                end
            end;
        desc ->
            fun(Tuple1, Tuple2) ->
                case Acc(Tuple1, Tuple2) of
                    {cont, true} ->
                        Elem1 = element(KeyPos, Tuple1),
                        Elem2 = element(KeyPos, Tuple2),
                        if
                            Elem1 =:= Elem2 -> {cont, true};
                            true -> {break, Elem1 >= Elem2}
                        end;
                    {break, Result} -> {break, Result}
                end
            end
    end,
    build_order_lambda(Rest, NewAcc, SelOrder).

condition_to_matchspec(undefined, _Params) ->
    undefined;

condition_to_matchspec(#condition{nexo = eq, op1 = Op1, op2 = Op2}, Params) ->
    {'==', condition_to_matchspec(Op1, Params), condition_to_matchspec(Op2, Params)};

condition_to_matchspec(#condition{nexo = gt, op1 = Op1, op2 = Op2}, Params) ->
    {'>', condition_to_matchspec(Op1, Params), condition_to_matchspec(Op2, Params)};

condition_to_matchspec(#condition{nexo = lt, op1 = Op1, op2 = Op2}, Params) ->
    {'<', condition_to_matchspec(Op1, Params), condition_to_matchspec(Op2, Params)};

condition_to_matchspec(#key{name = Key, table = undefined}, Params) ->
    case maps:get(Key, Params, error) of
        error -> throw({error, {enoexistsfield, Key}});
        I -> list_to_atom([ $$ | integer_to_list(I) ])
    end;

condition_to_matchspec(#key{name = Key, table = Table}, Params) ->
    case maps:get({Table, Key}, Params, error) of
        error -> throw({error, {enoexistsfield, {Table, Key}}});
        I -> list_to_atom([ $$ | integer_to_list(I) ])
    end;

condition_to_matchspec(#value{value = Val}, _Params) ->
    Val.
