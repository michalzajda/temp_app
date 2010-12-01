-module(utils).

-export([reload/0]).
%%
%% @spec reload() -> ok
%% @doc Hot loads recompiled code.
%%
-spec(reload/0 :: () -> ok).
reload() ->
    make:all([load]).
