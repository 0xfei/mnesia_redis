-module(mnesis_server).
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
-export([insert_watch/3, remove_watch/3, write_watch/2, discard_watch/1]).
-export([multi_start/1, check_watch/1]).

-record(state, {db, time}).

-define(WATCH_TABLE, redis_watch).
-define(EXPIRE_KEY, expire).

start_link(Database, Time) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Database, Time], []).

%% expire
insert(Db, Key, Value) ->
    gen_server:cast(?MODULE, {add_expire_key, Db, Key, Value}).
remove(Db, Key) ->
    gen_server:cast(?MODULE, {del_expire_key, Db, Key}).
clear(Db, Key) ->
    gen_server:cast(?MODULE, {cls_expire_key, Db, Key}).

%% watch
insert_watch(Pid, Database, Key) ->
    gen_server:cast(?MODULE, {insert_watch, Pid, Database, Key}).

remove_watch(Pid, Database, Key) ->
    gen_server:cast(?MODULE, {remove_watch, Pid, Database, Key}).

write_watch(Database, Key) ->
    gen_server:cast(?MODULE, {write_watch, Database, Key}).

discard_watch(Pid) ->
    gen_server:cast(?MODULE, {discard_watch, Pid}).

%% used in mnesia:transaction
-spec check_watch(P::pid()) -> dirty | clean.
check_watch(Pid) ->
    case mnesia:read({?WATCH_TABLE, Pid}) of
        [{?WATCH_TABLE, _, 1}] ->
            mnesia:delete({?WATCH_TABLE, Pid}),
            dirty;
        _ ->
            clean
    end.

-spec multi_start(P::pid()) -> {ok, atomic}.
multi_start(Pid) ->
    mnesia:transaction(
        fun() -> mnesia:delete({?WATCH_TABLE, Pid}) end
    ).


%% callback
init([Database, Time]) ->
    {ok, #state{db=Database, time=Time}, Time}.

handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast({add_expire_key, Database, Key, Value}, State=#state{time=Time}) ->
    do_insert(Database, Key, Value),
    {noreply, State, Time};
handle_cast({del_expire_key, Database, Key}, State=#state{time=Time}) ->
    do_remove(Database, Key),
    {noreply, State, Time};
handle_cast({cls_expire_key, Database, Key}, State=#state{time=Time}) ->
    do_clear(Database, Key),
    {noreply, State, Time};

%% watch
handle_cast({write_watch, Database, Key}, State=#state{time=Time}) ->
    do_write_watch(Database, Key),
    {noreply, State, Time};
handle_cast({insert_watch, Pid, Database, Key}, State=#state{time=Time}) ->
    do_insert_watch(Pid, Database, Key),
    {noreply, State, Time};
handle_cast({remove_watch, Pid, Database, Key}, State=#state{time=Time}) ->
    do_remove_watch(Pid, Database, Key),
    {noreply, State, Time};
handle_cast({discard_watch, Pid}, State=#state{time=Time}) ->
    do_discard_watch(Pid),
    {noreply, State, Time};

handle_cast(_Request, State=#state{time=Time}) ->
    {noreply, State, Time}.

handle_info(timeout, State=#state{db=Database, time=Time}) ->
    regular_remove(Database),
    regular_clean_watch(),
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

%% watch
do_write_watch(Database, Key) ->
    Fun = fun() ->
            case mnesia:read({?WATCH_TABLE, {Database, Key}}) of
                [{?WATCH_TABLE, _, Set}] ->
                    Pids = sets:to_list(Set),
                    mnesia:delete({?WATCH_TABLE, {Database, Key}}),
                    [mnesia:write({?WATCH_TABLE, Pid, 1}) || Pid <- Pids];
                _ ->
                    ok
            end
          end,
    mnesia:transaction(Fun).

do_insert_watch(Pid, Database, Key) ->
    Fun = fun() ->
            case mnesia:read({?WATCH_TABLE, {Database, Key}}) of
                [{?WATCH_TABLE, _, Set}] ->
                    mnesia:write({?WATCH_TABLE, {Database, Key},
                        sets:add_element(Pid, Set)});
                _ ->
                    ok
            end
          end,
    mnesia:transaction(Fun).

do_remove_watch(Pid, Database, Key) ->
    Fun = fun() ->
            case mnesia:read({?WATCH_TABLE, {Database, Key}}) of
                [{?WATCH_TABLE, _, Set}] ->
                    mnesia:write({?WATCH_TABLE, {Database, Key},
                        sets:del_element(Pid, Set)});
                _ ->
                    ok
            end
          end,
    mnesia:transaction(Fun).

do_discard_watch(Pid) ->
    Fun = fun() ->
            [ case mnesia:read({?WATCH_TABLE, {Db, Key}}) of
                  [{?WATCH_TABLE, _, Set}] ->
                      mnesia:write({?WATCH_TABLE, {Db, Key}, sets:del_element(Pid, Set)});
                  _ ->
                      ok
              end || {Db, Key} <-mnesia:all_keys(?WATCH_TABLE)],
            mnesia:delete({?WATCH_TABLE, Pid})
          end,
    mnesia:transaction(Fun).

regular_clean_watch() ->
    Fun = fun() ->
            [do_discard_watch(Pid) || Pid <-mnesia:all_keys(?WATCH_TABLE),
                is_pid(Pid), erlang:is_process_alive(Pid) == false]
          end,
    mnesia:transaction(Fun).
