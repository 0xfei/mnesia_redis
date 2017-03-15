-module(redis_operation).

-include("redis_operation.hrl").

%% API
-export([do_operation/3]).
-export([get/1]).

%% do operation
do_operation(Cmd, Param, State) ->
    try binary_to_existing_atom(Cmd, latin1) of
        Func ->
            try erlang:function_exported(redis_operation, Func, 1) of
                false ->
                    {redis_parser:reply_error(<<"ERR unknown command '", Cmd/binary,  "' ">>), State};
                true ->
                    case do_transaction(Func, Param, State) of
                        {ok, old} ->
                            {redis_operation:Func(Param), State};
                        {ok, new, Reply, NewState} ->
                            {Reply, NewState}
                    end
            catch
                _Error:_Code ->
                    {redis_parser:reply_error(<<"ERR unknown command '", Cmd/binary,  "' ">>), State}
            end
    catch
        _Error:_Code ->
            {redis_parser:reply_error(<<"ERR unknown command '", Cmd/binary,  "' ">>), State}
    end.

%% check transaction operation
%% todo:
%% 1.Check command before queued
%% 2.Check watch list (need it ? ) after finish redis_operation.erl
%% 3.Finish exec command
do_transaction(Cmd, Param, State=#state{trans=false, wlist=Wlist}) ->
    case Cmd of
        multi ->
            {ok, new, redis_parser:reply_status(<<"OK">>), State#state{trans=true}};
        watch ->
            {ok, new, redis_parser:reply_status(<<"OK">>), State#state{wlist=Param++Wlist}};
        unwatch ->
            {ok, new, redis_parser:reply_status(<<"OK">>), State#state{wlist=[]}};
        exec ->
            {ok, new, redis_parser:reply_error(<<"EXEC without MULTI">>), State#state{error=true}};
        discard ->
            {ok, new, redis_parser:reply_error(<<"DISCARD without MULTI">>), State#state{error=true}};
        _ ->
            {ok, old}
    end;
do_transaction(Cmd, Param, State=#state{trans=_, wlist=_Wlist, optlist=OptList}) ->
    case Cmd of
        multi ->
            {ok, new, redis_parser:reply_error(<<"MULTI calls can not be nested">>), State#state{error=true}};
        watch ->
            {ok, new, redis_parser:reply_error(<<"WATCH inside MULTI is not allowed">>), State#state{error=true}};
        unwatch ->
            {ok, new, redis_parser:reply_status(<<"OK">>), State#state{wlist=[]}};
        exec ->
            _Operation = lists:reverse(OptList),
            {ok, new, redis_parser:reply_status(<<"OK">>), State#state{trans=false, wlist=[], optlist=[]}};
        discard ->
            {ok, new, redis_parser:reply_status(<<"OK">>), State#state{trans=false, wlist=[], optlist=[]}};
        Cmd ->
            {ok, new, redis_parser:reply_status(<<"QUEUED">>), State#state{optlist=[{Cmd, Param}|OptList]}}
    end.

%% operation
get(_List) ->
    redis_parser:reply_single(<<"def">>).
