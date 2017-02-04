-module(penguin).
-export([start/0, ping/1]).


start() ->
    spawn(fun() -> loop([]) end).

ping(Pid) -> Pid ! foo.

loop(State) ->
    % if there is some cached state, keep checking until mailbox is cleared
    % if not, wait indefinitely for a message
    Timeout =
        case State of
            [] -> infinity;
            _ -> 0
        end,
    receive
        Msg ->
            append_msg(State, Msg),
            loop(State)
    after Timeout ->
        process(State),
        loop(State)
    end.


process(State) ->
    io:format("what\'s going on~n").

append_msg(State, Msg) ->
    io:format("message received~n"). 
