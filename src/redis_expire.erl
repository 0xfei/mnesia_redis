-module(redis_expire).
-behavior(gen_server).

%% API
-export([start_link/2]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% export
-export([insert/3, remove/2, clear/2]).

-record(state, {db, time}).

-define(EXPIRE_KEY, expire).
-define(TAB, ?MODULE).

start_link(Database, Time) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Database, Time], []).

%% interface
insert(Db, Key, Value) ->
    gen_server:cast(?MODULE, {add, Db, Key, Value}).
remove(Db, Key) ->
    gen_server:cast(?MODULE, {del, Db, Key}).
clear(Db, Key) ->
    gen_server:cast(?MODULE, {cls, Db, Key}).

%% callback
init([Database, Time]) ->
    {ok, #state{db=Database, time=Time}, Time}.

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast({add, Database, Key, Value}, State=#state{time=Time}) ->
    do_insert(Database, Key, Value),
    {noreply, State, Time};
handle_cast({del, Database, Key}, State=#state{time=Time}) ->
    do_remove(Database, Key),
    {noreply, State, Time};
handle_cast({cls, Database, Key}, State=#state{time=Time}) ->
    do_clear(Database, Key),
    {noreply, State, Time};
handle_cast(_Request, State=#state{time=Time}) ->
    {noreply, State, Time}.

handle_info(timeout, State=#state{db=Database, time=Time}) ->
    regular_remove(Database),
    {noreply, State, Time};
handle_info(_Info, State=#state{time=Time}) ->
    {noreply, State, Time}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% impelment
do_insert(Database, Key, Time) ->
    Fun = fun() ->
            [{Database, ?EXPIRE_KEY, {Set, Dict}}] =
                mnesia:read({Database, ?EXPIRE_KEY}),
            mnesia:write({Database, ?EXPIRE_KEY,
                case dict:find(Key, Dict) of
                    {ok, Old} ->
                        {
                            ordsets:add_element(
                                {Time, Key},
                                ordsets:del_element({Old, Key}, Set)),
                            dict:store(Key, Time, Dict)
                        };
                    _ ->
                        {ordsets:add_element({Time, Key},Set),
                            dict:store(Key, Time, Dict)}
                end
            })
          end,
    mnesia:transaction(Fun).

do_clear(Database, Key) ->
    Fun = fun() ->
            [{Database, ?EXPIRE_KEY, {Set, Dict}}] =
                mnesia:read({Database, ?EXPIRE_KEY}),
                mnesia:write({Database, ?EXPIRE_KEY,
                    case dict:find(Key, Dict) of
                        {ok, Old} ->
                            {ordsets:del_element({Old, Key}, Set),
                                dict:erase(Key, Dict)};
                        _ ->
                            {Set, Dict}
                    end
                })
          end,
    mnesia:transaction(Fun).

do_remove(Database, Key) ->
    Fun = fun() ->
            [{Database, ?EXPIRE_KEY, {Set, Dict}}] =
                mnesia:read({Database, ?EXPIRE_KEY}),
            mnesia:delete({Database, Key}),
            mnesia:write({Database, ?EXPIRE_KEY,
                case dict:find(Key, Dict) of
                    {ok, Old} ->
                        {ordsets:del_element({Old, Key}, Set),
                            dict:erase(Key, Dict)};
                    _ ->
                        {Set, Dict}
                end
            })
          end,
    mnesia:transaction(Fun).

regular_remove(Databases) ->
    Now = erlang:system_time(1),
    loop_remove(Now, Databases).

loop_remove(_, []) ->
    ok;
loop_remove(Now, [Database|Left]) ->
    try
        [{Database, ?EXPIRE_KEY, {Set, _Dict}}] =
            mnesia:dirty_read({Database, ?EXPIRE_KEY}),
        [ do_remove(Database, K) ||
            {T, K} <- ordsets:to_list(Set), T =< Now ]
    catch
        _:_ -> ok
    end,
    loop_remove(Now, Left).
