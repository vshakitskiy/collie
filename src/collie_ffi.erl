-module(collie_ffi).

-export([coerce_socket_message/1, to_erl_options/1, tcp_send/2, tcp_close/1,
         tcp_shutdown/2, tcp_set_opts/2, tcp_controlling_process/2, ssl_send/2, ssl_close/1,
         ssl_shutdown/2, ssl_set_opts/2, ssl_controlling_process/2, ssl_start/0,
         custom_sni_matcher/0, validate_query/1, decode_packet/2, validate_field_value/1]).

coerce_socket_message({tcp, _Socket, Data}) ->
  {packet, Data};
coerce_socket_message({ssl, _Socket, Data}) ->
  {packet, Data};
coerce_socket_message({tcp_closed, _Socket}) ->
  close;
coerce_socket_message({ssl_closed, _Socket}) ->
  close;
coerce_socket_message({tcp_passive, _Socket}) ->
  passive;
coerce_socket_message({ssl_passive, _Socket}) ->
  passive;
coerce_socket_message({tcp_error, _Socket, Reason}) ->
  {socket_error, Reason};
coerce_socket_message({ssl_error, _Socket, Reason}) ->
  {socket_error, Reason}.

to_erl_option({active_mode, once}) ->
  {active, once};
to_erl_option({active_mode, passive}) ->
  {active, false};
to_erl_option({active_mode, active}) ->
  {active, true};
to_erl_option({active_mode, {count, N}}) ->
  {active, N};
to_erl_option({mode, binary}) ->
  binary;
to_erl_option(Other) ->
  Other.

to_erl_options(Options) ->
  lists:map(fun to_erl_option/1, Options).

tcp_send(Socket, Data) ->
  case gen_tcp:send(Socket, Data) of
    ok ->
      {ok, nil};
    {error, Reason} ->
      {error, Reason}
  end.

tcp_close(Socket) ->
  gen_tcp:close(Socket),
  {ok, nil}.

tcp_shutdown(Socket, How) ->
  case gen_tcp:shutdown(Socket, How) of
    ok ->
      {ok, nil};
    {error, Reason} ->
      {error, Reason}
  end.

tcp_set_opts(Socket, Opts) ->
  case inet:setopts(Socket, Opts) of
    ok ->
      {ok, nil};
    {error, Reason} ->
      {error, Reason}
  end.

tcp_controlling_process(Socket, Pid) ->
  case gen_tcp:controlling_process(Socket, Pid) of
    ok ->
      {ok, nil};
    {error, Reason} ->
      {error, Reason}
  end.

ssl_send(Socket, Data) ->
  case ssl:send(Socket, Data) of
    ok ->
      {ok, nil};
    {error, Reason} ->
      {error, Reason}
  end.

ssl_close(Socket) ->
  case ssl:close(Socket) of
    ok ->
      {ok, nil};
    {error, Reason} ->
      {error, Reason}
  end.

ssl_shutdown(Socket, How) ->
  case ssl:shutdown(Socket, How) of
    ok ->
      {ok, nil};
    {error, Reason} ->
      {error, Reason}
  end.

ssl_set_opts(Socket, Opts) ->
  case ssl:setopts(Socket, Opts) of
    ok ->
      {ok, nil};
    {error, Reason} ->
      {error, Reason}
  end.

ssl_controlling_process(Socket, Pid) ->
  case ssl:controlling_process(Socket, Pid) of
    ok ->
      {ok, nil};
    {error, Reason} ->
      {error, Reason}
  end.

ssl_start() ->
  case ssl:start() of
    ok ->
      {ok, nil};
    {ok, _Started} ->
      {ok, nil};
    {error, {already_started, ssl}} ->
      {ok, nil};
    {error, Reason} ->
      {error, Reason}
  end.

custom_sni_matcher() ->
  [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}].

validate_query(Query) ->
  case uri_string:dissect_query(Query) of
    {error, _, _} ->
      none;
    _Pairs ->
      {some, Query}
  end.

decode_packet(Type, Bin) ->
  case erlang:decode_packet(Type, Bin, []) of
    {ok, {http_header, Idx, _, Field, Value}, Remaining} ->
      {ok, {{http_header, Idx, Field, Value}, Remaining}};
    {ok, Packet, Remaining} ->
      {ok, {Packet, Remaining}};
    {more, Length} ->
      {error, {more, Length}};
    {error, Reason} ->
      {error, {http_error, Reason}}
  end.

validate_field_value(Value) ->
  case do_validate_field_value(Value) of
    true ->
      {ok, Value};
    false ->
      {error, nil}
  end.

% HTTP field values can contain:
% - VCHAR: 0x21-0x7E (visible ASCII characters)
% - WSP: 0x20 (space), 0x09 (tab)
% - obs-text: 0x80-0xFF (for backward compatibility)
% Invalid: control characters 0x00-0x08, 0x0A-0x1F, 0x7F
do_validate_field_value(Value) ->
  case Value of
    <<>> ->
      true;
    <<C, Rest/bitstring>>
      when C =:= 16#09
           orelse C >= 16#20 andalso C =< 16#7E
           orelse C >= 16#80 andalso C =< 16#FF ->
      do_validate_field_value(Rest);
    _ ->
      false
  end.
