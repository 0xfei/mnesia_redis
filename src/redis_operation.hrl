-ifndef(REDIS_OPERATION_HRL).
-define(REDIS_OPERATION_HRL, 1).

%% socket state
-record(state, {
    socket :: inet:socket(),
    peername :: {inet:ip_address(), non_neg_integer()},
    transport :: module(),
    trans = false :: boolean(),
    error = false :: boolean(),
    dirty = false :: boolean(),
    wlist = []:: list(),
    optlist = []:: [{atom(), [binary()]}]
}).

%% operation
-define(GET, get).

-endif.
