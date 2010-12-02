%%% @author  <michalzajda@potter71.local>
%%% Description : Demo app for riak presentation
%%% Created :  1 Dec 2010 by Michal Zajda <michalzajda@potter71.local>

-module(demo).

%% API
-export([populate/0, relations1/0, relations2/0, query_mr/0, query_mr_link/0]).

-export([sum/2, get_balance/3, calc_balance/3]).
%% Map & Reduce


-record(account, { intrests = undefined :: float(),
                   balance = 0 :: integer()
                 }).
-record(details, { description :: string(),
                   creation_date,
                   address ::  string()
                 }).




populate() ->
    save(<<"account_balance">>, <<"Michal">>, 
         #account{intrests = 0.03,
                  balance = 123}),
    save(<<"account_balance">>, <<"Person1">>, 
         #account{intrests = 0.02,
                  balance = 500}),
    save(<<"account_balance">>, <<"Bob">>, 
         #account{intrests = 0.02,
                  balance = 45}),
    save(<<"account_balance">>, <<"Alice">>, 
         #account{intrests = 0.09,
                  balance = 600}),
    save(<<"account_details">>, <<"Alice">>, 
         #details{description = "My account",
                  creation_date = calendar:local_time(),
                  address="Wonderland 56"}),
    save(<<"account_savings">>, <<"Alice">>, 
         #account{balance = 10000}).

relations1() ->
    {ok, Client} = riak:local_client(),
    {ok, Obj} = Client:get(<<"account_details">>, <<"Alice">>,1),
    Meta = riak_object:get_metadata(Obj),
    Links = dict:fetch(<<"Links">>, Meta),
    {value, {{SecretBucket, SecretKey},_}} = lists:keysearch(<<"savings">>, 2, Links),
    {ok, Linked} = Client:get(SecretBucket, SecretKey, 1),
    io:format("~p~n linked to~n ~p~n",[riak_object:get_value(Obj), 
                                       riak_object:get_value(Linked)]).

relations2() ->
    {ok, C} = riak:local_client(),
    Related = get_value([
                         C:get(Bucket, <<"Alice">>, 1) || 
                            Bucket <- [ <<"account_details">>, 
                                        <<"account_balance">>,
                                        <<"account_savings">>
                                      ]]),
    io:format("~p~n",[Related]).

query_mr() ->
    {ok, Client} = riak:local_client(),
    Result = 
        Client:mapred_bucket(<<"account_balance">>,
                             [{map, {modfun, ?MODULE, get_balance}, none, false},
                              {reduce, {modfun, ?MODULE, sum}, none, true}]),
    
    io:format("~p~n",[Result]).

query_mr_link() ->
    {ok, Client} = riak:local_client(),
    Result = 
        Client:mapred_bucket(<<"account_details">>,
                             [{link, '_', '_', false},
                              {map, {modfun, ?MODULE, calc_balance}, none, false},
                              {reduce, {modfun, ?MODULE, sum}, none, true}]),
    
    io:format("~p~n",[Result]).

calc_balance(Doc, _KeyData, _Arg) ->
    Data = riak_object:get_value(Doc),
    Intrests = Data#account.balance*(Data#account.intrests+1),
    io:format("> ~p~n",[Intrests]),
    [Intrests].

get_balance(Doc, _KeyData, _Arg) ->
    Data = riak_object:get_value(Doc),
    [Data#account.balance].

sum(ValueList, _Arg) ->
    Fun = fun(X,Acc0) when is_integer(X); is_float(X) ->
                  Acc0+X;
             (_,Acc0) ->
                  Acc0
          end,
    [lists:foldl(Fun, 0, lists:flatten(ValueList))].
    

            
%%%===================================================================
%%% Internal functions
%%%===================================================================

%% spetial case to create link to Alice's secret account
save(Bucket = <<"account_details">>, Key = <<"Alice">>, Data) ->
    Obj = riak_object:new(Bucket, Key, Data),
    Meta = dict:store(<<"Links">>,
                      [{{<<"account_savings">>, <<"Alice">>}, <<"savings">>},
                       {{<<"account_balance">>, <<"Alice">>}, <<"regular">>}],
                      riak_object:get_metadata(Obj)),
    ObjectWithMeta = riak_object:update_metadata(Obj, Meta),
    {ok, C} = riak:local_client(),
    C:put(ObjectWithMeta, 1);

save(Bucket, Key, Data) ->
    Object = riak_object:new(Bucket, Key, Data),
    {ok, C} = riak:local_client(),
    C:put(Object, 1).

get_value(List) ->
    {_OK, ObjList} = lists:unzip(List),
    [ riak_object:get_value(Obj) || Obj <- ObjList ].
