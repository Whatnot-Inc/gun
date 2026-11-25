%% Copyright (c) Loïc Hoguin <essen@ninenines.eu>
%%
%% Permission to use, copy, modify, and/or distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

-module(pool_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-import(ct_helper, [doc/1]).
-import(ct_helper, [config/2]).

all() ->
	[
		hello_pool_h1,
		hello_pool_h2,
		hello_pool_ws,
		max_streams_h1,
		max_streams_h1_retry,
		max_streams_h2_size_1,
		max_streams_h2_size_1_retry,
		max_streams_h2_size_2,
		max_streams_h2_size_2_retry,
		kill_restart_h1,
		kill_restart_h2,
		reconnect_h1,
		reconnect_h2,
		stop_pool,
		degraded_configuration_error,
		metrics_idle,
		metrics_streams,
		cancel_releases_stream_slot,
		kill_conn_releases_stream_count,
		cancel_h1_survives_in_flight_response,
		max_streams_h2_no_wait
	].

init_per_suite(Config) ->
	{ok, _} = cowboy:start_clear({?MODULE, tcp}, [], do_proto_opts()),
	Port = ranch:get_port({?MODULE, tcp}),
	[{port, Port}|Config].

end_per_suite(_) ->
	%% Stop listeners started by individual test cases.
	ExtraListeners = [
		max_streams_h2_size_1,
		max_streams_h2_size_1_retry,
		max_streams_h2_size_2,
		max_streams_h2_size_2_retry,
		max_streams_h2_no_wait,
		reconnect_h1,
		metrics_streams,
		cancel_releases_stream_slot,
		kill_conn_releases_stream_count,
		cancel_h1_survives_in_flight_response
	],
	_ = [cowboy:stop_listener(Listener) || Listener <- ExtraListeners],
	ok.

do_proto_opts() ->
	Routes = [
		{"/", hello_h, []},
		{"/delay", delayed_hello_h, 3000},
		{"/empty", empty_h, []},
		{"/push", push_h, []},
		{"/ws", ws_echo_h, []}
	],
	#{
		env => #{dispatch => cowboy_router:compile([{'_', Routes}])}
	}.

%% Tests.

hello_pool_h1(Config) ->
	doc("Confirm the pool can be used for HTTP/1.1 connections."),
	Port = config(port, Config),
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => ?FUNCTION_NAME
	}),
	gun_pool:await_up(ManagerPid),
	Streams = [{async, _} = gun_pool:get("/",
		#{<<"host">> => ["localhost:", integer_to_binary(Port)]},
		#{scope => ?FUNCTION_NAME}
	) || _ <- lists:seq(1, 8)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams].

hello_pool_h2(Config) ->
	doc("Confirm the pool can be used for HTTP/2 connections."),
	Port = config(port, Config),
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		scope => ?FUNCTION_NAME
	}),
	gun_pool:await_up(ManagerPid),
	Streams = [{async, _} = gun_pool:get("/",
		#{<<"host">> => ["localhost:", integer_to_binary(Port)]},
		#{scope => ?FUNCTION_NAME}
	) || _ <- lists:seq(1, 800)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams].

hello_pool_ws(Config) ->
	doc("Confirm the pool can be used for HTTP/1.1 connections upgraded to Websocket."),
	Port = config(port, Config),
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{
			protocols => [http],
			ws_opts => #{
				default_protocol => pool_ws_handler,
				user_opts => self()
			}
		},
		scope => ?FUNCTION_NAME,
		setup_fun => {fun
			(ConnPid, {gun_up, _, http}, SetupState) ->
				_ = gun:ws_upgrade(ConnPid, "/ws"),
				{setup, SetupState};
			(_, {gun_upgrade, _, StreamRef, _, _}, _) ->
				{up, ws, #{ws => StreamRef}};
			(ConnPid, Msg, SetupState) ->
				ct:pal("Unexpected setup message for ~p: ~p", [ConnPid, Msg]),
				{setup, SetupState}
		end, undefined}
	}),
	gun_pool:await_up(ManagerPid),
	_ = [gun_pool:ws_send({text, <<"Hello world!">>}, #{
		authority => ["localhost:", integer_to_binary(Port)],
		scope => ?FUNCTION_NAME
	}) || _ <- lists:seq(1, 8)],
	%% The pool_ws_handler module sends frames back to us.
	_ = [receive
		{text, <<"Hello world!">>} ->
			ok
	end || _ <- lists:seq(1, 8)].

invalid_request_header_releases_stream(Config) ->
	doc("A request header that raises in the caller releases the reserved stream."),
	Port = config(port, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	Headers = #{
		<<"host">> => Authority,
		<<"x-bad">> => <<"a\r\nb">>
	},
	Opts = #{scope => ?FUNCTION_NAME},
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => ?FUNCTION_NAME,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	%% request/5 via get, then headers/4 via post without a body.
	try gun_pool:get("/", Headers, Opts) of
		_ -> ct:fail("expected exception")
	catch
		error:{invalid_request_header, <<"x-bad">>, _} -> ok
	end,
	try gun_pool:post("/", Headers, Opts) of
		_ -> ct:fail("expected exception")
	catch
		error:{invalid_request_header, <<"x-bad">>, _} -> ok
	end,
	{operational, #{table := Tid, conns := Conns}} = gun_pool:info(ManagerPid),
	[{ConnPid, {up, http, _}}] = maps:to_list(Conns),
	[{ConnPid, _, 0}] = ets:lookup(Tid, ConnPid),
	{async, StreamRef} = gun_pool:get("/", #{<<"host">> => Authority}, Opts),
	{response, nofin, 200, _} = gun_pool:await(StreamRef),
	{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef).

max_streams_h1(Config) ->
	doc("Confirm requests are rejected when the maximum number "
		"of streams is reached for HTTP/1.1 connections."),
	Port = config(port, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => ?FUNCTION_NAME,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	{async, _} = gun_pool:get("/delay",
		#{<<"host">> => Authority}, #{scope => ?FUNCTION_NAME}),
	timer:sleep(500),
	{error, no_connection_available, _} = gun_pool:get("/delay",
		#{<<"host">> => Authority}, #{scope => ?FUNCTION_NAME}).

max_streams_h1_retry(Config) ->
	doc("Confirm connection checkout is retried when the maximum number "
		"of streams is reached for HTTP/1.1 connections."),
	Port = config(port, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => ?FUNCTION_NAME,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	{async, _} = gun_pool:get("/delay",
		#{<<"host">> => Authority}, #{scope => ?FUNCTION_NAME}),
	timer:sleep(500),
	{error, no_connection_available, _} = gun_pool:get("/delay",
		#{<<"host">> => Authority}, #{scope => ?FUNCTION_NAME}),
	{async, _} = gun_pool:get("/delay", #{<<"host">> => Authority}, #{
		checkout_retry => [100, 500, 500, 500, 500, 500, 500],
		scope => ?FUNCTION_NAME
	}).

max_streams_h2_size_1(_Config) ->
	doc("Confirm requests are rejected when the maximum number "
		"of streams is reached for HTTP/2 connections."),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(?FUNCTION_NAME, [], ProtoOpts#{
		max_concurrent_streams => 5
	}),
	Port = ranch:get_port(?FUNCTION_NAME),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	[{async, _} = gun_pool:get("/delay", #{<<"host">> => Authority}) || _ <- lists:seq(1, 5)],
	timer:sleep(500),
	{error, no_connection_available, _} = gun_pool:get("/delay", #{<<"host">> => Authority}).

max_streams_h2_size_1_retry(_Config) ->
	doc("Confirm connection checkout is retried when the maximum number "
		"of streams is reached for HTTP/2 connections."),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(?FUNCTION_NAME, [], ProtoOpts#{
		max_concurrent_streams => 5
	}),
	Port = ranch:get_port(?FUNCTION_NAME),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	[{async, _} = gun_pool:get("/delay", #{<<"host">> => Authority}) || _ <- lists:seq(1, 5)],
	timer:sleep(500),
	{error, no_connection_available, _} = gun_pool:get("/delay", #{<<"host">> => Authority}),
	{async, _} = gun_pool:get("/delay", #{<<"host">> => Authority}, #{
		checkout_retry => [100, 500, 500, 500, 500, 500, 500]
	}).

max_streams_h2_size_2(_Config) ->
	doc("Confirm requests are rejected when the maximum number "
		"of streams is reached for HTTP/2 connections."),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(?FUNCTION_NAME, [], ProtoOpts#{
		max_concurrent_streams => 5
	}),
	Port = ranch:get_port(?FUNCTION_NAME),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		size => 2
	}),
	gun_pool:await_up(ManagerPid),
	[begin
		{async, _} = gun_pool:get("/delay", #{<<"host">> => Authority}),
		%% We need to wait a bit for the request to be sent because the
		%% request is sent and counted asynchronously.
		timer:sleep(10)
	end || _ <- lists:seq(1, 10)],
	timer:sleep(500),
	{error, no_connection_available, _} = gun_pool:get("/delay", #{<<"host">> => Authority}).

max_streams_h2_size_2_retry(_Config) ->
	doc("Confirm connection checkout is retried when the maximum number "
		"of streams is reached for HTTP/2 connections."),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(?FUNCTION_NAME, [], ProtoOpts#{
		max_concurrent_streams => 5
	}),
	Port = ranch:get_port(?FUNCTION_NAME),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		size => 2
	}),
	gun_pool:await_up(ManagerPid),
	[begin
		{async, _} = gun_pool:get("/delay", #{<<"host">> => Authority}),
		%% We need to wait a bit for the request to be sent because the
		%% request is sent and counted asynchronously.
		timer:sleep(10)
	end || _ <- lists:seq(1, 10)],
	timer:sleep(500),
	{error, no_connection_available, _} = gun_pool:get("/delay", #{<<"host">> => Authority}),
	{async, _} = gun_pool:get("/delay", #{<<"host">> => Authority}, #{
		checkout_retry => [100, 500, 500, 500, 500, 500, 500]
	}).

max_streams_h2_no_wait(_) ->
	doc("Confirm await_up waits for HTTP/2 SETTINGS, and that the pool "
		"never gives out a connection that is already at the server's "
		"maximum number of streams."),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(?FUNCTION_NAME, [], ProtoOpts#{
		max_concurrent_streams => 5
	}),
	Port = ranch:get_port(?FUNCTION_NAME),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		scope => ?FUNCTION_NAME,
		size => 2
	}),
	gun_pool:await_up(ManagerPid),
	{operational, #{conns := Conns}} = gun_pool:info(ManagerPid),
	true = lists:all(fun
		({up, http2, #{max_concurrent_streams := 5}}) -> true;
		(_) -> false
	end, maps:values(Conns)),
	%% We do not wait between requests: the streams must be
	%% accounted for as soon as a connection is given out.
	Streams = [{async, _} = gun_pool:get("/delay",
		#{<<"host">> => Authority},
		#{scope => ?FUNCTION_NAME}
	) || _ <- lists:seq(1, 10)],
	{error, no_connection_available, _} = gun_pool:get("/delay",
		#{<<"host">> => Authority}, #{scope => ?FUNCTION_NAME}),
	%% None of the requests may fail with too_many_streams.
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams].

kill_restart_h1(Config) ->
	doc("Confirm the Gun process is restarted and the pool operational "
		"after an HTTP/1.1 Gun process has crashed."),
	Port = config(port, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => ?FUNCTION_NAME
	}),
	gun_pool:await_up(ManagerPid),
	Streams1 = [{async, _} = gun_pool:get("/",
		#{<<"host">> => Authority},
		#{scope => ?FUNCTION_NAME}
	) || _ <- lists:seq(1, 8)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams1],
	%% Get a connection and kill the process.
	{operational, #{conns := Conns}} = gun_pool:info(ManagerPid),
	ConnPid = hd(maps:keys(Conns)),
	MRef = monitor(process, ConnPid),
	exit(ConnPid, {shutdown, ?FUNCTION_NAME}),
	receive {'DOWN', MRef, process, ConnPid, _} -> ok end,
	{degraded, _} = gun_pool:info(ManagerPid),
	gun_pool:await_up(ManagerPid),
	Streams2 = [{async, _} = gun_pool:get("/",
		#{<<"host">> => Authority},
		#{scope => ?FUNCTION_NAME}
	) || _ <- lists:seq(1, 8)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams2].

kill_restart_h2(Config) ->
	doc("Confirm the Gun process is restarted and the pool operational "
		"after an HTTP/2 Gun process has crashed."),
	Port = config(port, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		scope => ?FUNCTION_NAME
	}),
	gun_pool:await_up(ManagerPid),
	Streams1 = [{async, _} = gun_pool:get("/",
		#{<<"host">> => Authority},
		#{scope => ?FUNCTION_NAME}
	) || _ <- lists:seq(1, 800)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams1],
	%% Get a connection and kill the process.
	{operational, #{conns := Conns}} = gun_pool:info(ManagerPid),
	ConnPid = hd(maps:keys(Conns)),
	MRef = monitor(process, ConnPid),
	exit(ConnPid, {shutdown, ?FUNCTION_NAME}),
	receive {'DOWN', MRef, process, ConnPid, _} -> ok end,
	{degraded, _} = gun_pool:info(ManagerPid),
	gun_pool:await_up(ManagerPid),
	Streams2 = [{async, _} = gun_pool:get("/",
		#{<<"host">> => Authority},
		#{scope => ?FUNCTION_NAME}
	) || _ <- lists:seq(1, 800)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams2].

%% @todo kill_restart_ws

kill_conn_releases_stream_count(_Config) ->
	doc("Confirm a connection killed while streams are still in flight "
		"does not leave its stream count behind forever in the "
		"pool's ETS table."),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(?FUNCTION_NAME, [], ProtoOpts#{max_concurrent_streams => 5}),
	Port = ranch:get_port(?FUNCTION_NAME),
	Authority = iolist_to_binary(["localhost:", integer_to_binary(Port)]),
	Scope = ?FUNCTION_NAME,
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		scope => Scope,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	{async, {ConnPid, _}} = gun_pool:get("/delay", #{<<"host">> => Authority},
		#{scope => Scope}),
	[gun_pool:get("/delay", #{<<"host">> => Authority}, #{scope => Scope})
		|| _ <- lists:seq(1, 2)],
	ok = wait_for_metrics(Authority, Scope, #{streams => 3}),
	%% Kill the connection the way an external supervisor or a crash
	%% would.
	MRef = monitor(process, ConnPid),
	exit(ConnPid, kill),
	receive {'DOWN', MRef, process, ConnPid, _} -> ok end,
	gun_pool:await_up(ManagerPid),
	ok = wait_for_metrics(Authority, Scope, #{streams => 0}).

reconnect_h1(_Config) ->
	doc("Confirm the Gun process reconnects automatically for HTTP/1.1 connections."),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(?FUNCTION_NAME, [], ProtoOpts#{
		idle_timeout => 500
	}),
	Port = ranch:get_port(?FUNCTION_NAME),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]}
	}),
	gun_pool:await_up(ManagerPid),
	Streams1 = [{async, _} = gun_pool:get("/", #{<<"host">> => Authority}) || _ <- lists:seq(1, 8)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams1],
	%% Wait for the idle timeout to trigger.
	timer:sleep(600),
%		{degraded, _} = gun_pool:info(ManagerPid),
	gun_pool:await_up(ManagerPid),
	Streams2 = [{async, _} = gun_pool:get("/", #{<<"host">> => Authority}) || _ <- lists:seq(1, 8)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams2].

reconnect_h2(Config) ->
	doc("Confirm the Gun process reconnects automatically for HTTP/2 connections."),
	Port = config(port, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		scope => ?FUNCTION_NAME
	}),
	gun_pool:await_up(ManagerPid),
	Streams1 = [{async, _} = gun_pool:get("/",
		#{<<"host">> => Authority},
		#{scope => ?FUNCTION_NAME}
	) || _ <- lists:seq(1, 800)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams1],
	%% Wait for the idle timeout to trigger.
	timer:sleep(600),
%		{degraded, _} = gun_pool:info(ManagerPid),
	gun_pool:await_up(ManagerPid),
	Streams2 = [{async, _} = gun_pool:get("/",
		#{<<"host">> => Authority},
		#{scope => ?FUNCTION_NAME}
	) || _ <- lists:seq(1, 800)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams2].

%% @todo reconnect_ws

push_promise_no_crash(Config) ->
	doc("Confirm a server-pushed HTTP/2 stream does not crash "
		"the pooled connection or change its stream counter."),
	Port = config(port, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		scope => ?FUNCTION_NAME,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	{async, PoolRef={ConnPid, _}} = gun_pool:get("/push",
		#{<<"host">> => Authority},
		#{scope => ?FUNCTION_NAME}),
	{push, PromisedRef1, <<"GET">>, _, _} = gun_pool:await(PoolRef),
	{push, PromisedRef2, <<"GET">>, _, _} = gun_pool:await(PoolRef),
	{response, nofin, 200, _} = gun_pool:await(PoolRef),
	{ok, <<"Hello world!">>} = gun_pool:await_body(PoolRef),
	ok = do_await_pushed({ConnPid, PromisedRef1}),
	ok = do_await_pushed({ConnPid, PromisedRef2}),
	{operational, #{table := Tid, conns := Conns}} = gun_pool:info(ManagerPid),
	{up, http2, _} = maps:get(ConnPid, Conns),
	[{_, 0, 0}] = ets:lookup(Tid, ConnPid),
	{async, NextRef} = gun_pool:get("/",
		#{<<"host">> => Authority},
		#{scope => ?FUNCTION_NAME}),
	{response, nofin, 200, _} = gun_pool:await(NextRef),
	{ok, <<"Hello world!">>} = gun_pool:await_body(NextRef).

do_await_pushed(PoolRef) ->
	case gun_pool:await(PoolRef) of
		{response, fin, 200, _} ->
			ok;
		{response, nofin, 200, _} ->
			{ok, _} = gun_pool:await_body(PoolRef),
			ok
	end.

stop_pool(Config) ->
	doc("Confirm the pool can be stopped."),
	Port = config(port, Config),
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		scope => ?FUNCTION_NAME
	}),
	gun_pool:await_up(ManagerPid),
	gun_pool:stop_pool("localhost", Port, #{scope => ?FUNCTION_NAME}).

degraded_configuration_error(Config) ->
	case os:type() of
		{win32, _} ->
			{skip, "The initial connect timeout on Windows is too large."};
		_ ->
			do_degraded_configuration_error(Config)
	end.

do_degraded_configuration_error(Config) ->
	doc("Confirm the pool ends up in a degraded state "
		"when connection is impossible because of bad configuration."),
	Port = config(port, Config),
	%% We attempt to connect to an unreachable IP.
	{ok, ManagerPid} = gun_pool:start_pool({20, 20, 20, 1}, Port, #{
		conn_opts => #{tcp_opts => [{ip, {127, 0, 0, 1}}]},
		scope => ?FUNCTION_NAME,
		size => 1
	}),
	%% Wait for the lookup/connect to fail.
	timer:sleep(500),
	{degraded, #{conns := Conns}} = gun_pool:info(ManagerPid),
	true = Conns =:= #{},
	%% We can stop the pool even if degraded.
	gun_pool:stop_pool({20, 20, 20, 1}, Port, #{scope => ?FUNCTION_NAME}).

metrics_idle(Config) ->
	doc("Confirm pool metrics reflect active connections and zero streams when idle."),
	Port = config(port, Config),
	Authority = iolist_to_binary(["localhost:", integer_to_binary(Port)]),
	Scope = ?FUNCTION_NAME,
	Size = 3,
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		scope => Scope,
		size => Size
	}),
	gun_pool:await_up(ManagerPid),
	%% Make a request so SETTINGS have been negotiated.
	{async, PoolStreamRef} = gun_pool:get("/",
		#{<<"host">> => Authority}, #{scope => Scope}),
	{response, nofin, 200, _} = gun_pool:await(PoolStreamRef),
	{ok, _} = gun_pool:await_body(PoolStreamRef),
	%% Flush pending manager messages (e.g. settings_changed) with a synchronous call.
	gun_pool:info(Authority, Scope),
	#{size := Size, active := Size, streams := 0, full := 0, high := 0,
		authority := Authority, scope := Scope} = gun_pool:metrics(Authority, Scope),
	[#{size := Size, active := Size, streams := 0, full := 0, high := 0,
		authority := Authority, scope := Scope}] = gun_pool:metrics(Scope).

metrics_streams(_Config) ->
	doc("Confirm pool metrics count in-flight streams and detect full/high connections."),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(?FUNCTION_NAME, [], ProtoOpts#{max_concurrent_streams => 5}),
	Port = ranch:get_port(?FUNCTION_NAME),
	Authority = iolist_to_binary(["localhost:", integer_to_binary(Port)]),
	Scope = ?FUNCTION_NAME,
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		scope => Scope,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	%% Fill the single connection to max capacity.
	[gun_pool:get("/delay", #{<<"host">> => Authority},
		#{scope => Scope}) || _ <- lists:seq(1, 5)],
	%% Poll until event handler has counted all 5 streams and manager has processed SETTINGS.
	ok = wait_for_metrics(Authority, Scope, #{size => 1, active => 1, streams => 5, full => 1,
		high => 1, max_streams => 5}).

cancel_releases_stream_slot(_Config) ->
	doc("Confirm cancelling a stream frees its slot in the pool's "
		"stream count, instead of leaking it until the "
		"connection itself disconnects."),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(?FUNCTION_NAME, [], ProtoOpts#{max_concurrent_streams => 5}),
	Port = ranch:get_port(?FUNCTION_NAME),
	Authority = iolist_to_binary(["localhost:", integer_to_binary(Port)]),
	Scope = ?FUNCTION_NAME,
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		scope => Scope,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	%% Fill the single connection to max capacity.
	PoolStreamRefs = [begin
		{async, PoolStreamRef} = gun_pool:get("/delay", #{<<"host">> => Authority},
			#{scope => Scope}),
		PoolStreamRef
	end || _ <- lists:seq(1, 5)],
	ok = wait_for_metrics(Authority, Scope, #{streams => 5, full => 1}),
	ok = gun_pool:cancel(hd(PoolStreamRefs)),
	%% Without releasing the slot on cancel, this would time out at streams => 5.
	ok = wait_for_metrics(Authority, Scope, #{streams => 4, full => 0}).

cancel_h1_survives_in_flight_response(_Config) ->
	doc("Cancelling an HTTP/1.1 stream doesn't crash the connection "
		"once the server's response for it still arrives."),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(?FUNCTION_NAME, [], ProtoOpts),
	Port = ranch:get_port(?FUNCTION_NAME),
	Authority = iolist_to_binary(["localhost:", integer_to_binary(Port)]),
	Scope = ?FUNCTION_NAME,
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => Scope,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	{operational, #{conns := Conns}} = gun_pool:info(ManagerPid),
	ConnPid = hd(maps:keys(Conns)),
	MRef = monitor(process, ConnPid),
	{async, PoolStreamRef} = gun_pool:get("/delay", #{<<"host">> => Authority},
		#{scope => Scope}),
	ok = gun_pool:cancel(PoolStreamRef),
	%% Give the server time to reply and Gun time to drain and parse
	%% the now-cancelled stream's response on the wire.
	receive
		{'DOWN', MRef, process, ConnPid, Reason} ->
			ct:fail({connection_crashed, Reason})
	after 3500 ->
		ok
	end,
	%% The pool and its connection must still be usable afterwards.
	{operational, _} = gun_pool:info(ManagerPid),
	{async, StreamRef} = gun_pool:get("/", #{<<"host">> => Authority}, #{scope => Scope}),
	{response, nofin, 200, _} = gun_pool:await(StreamRef),
	{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef).

%% Poll until metrics match the expected map.
%% Calls info/2 each iteration to flush pending manager messages (e.g. settings_changed).
wait_for_metrics(Authority, Scope, Expected) ->
	wait_for_metrics(Authority, Scope, Expected, 50).

wait_for_metrics(_Authority, _Scope, _Expected, 0) ->
	{error, timeout};
wait_for_metrics(Authority, Scope, Expected, N) ->
	gun_pool:info(Authority, Scope),
	Actual = gun_pool:metrics(Authority, Scope),
	Match = is_map(Actual) andalso maps:fold(
		fun(K, V, Acc) -> Acc andalso maps:get(K, Actual, undefined) =:= V end,
		true, Expected),
	case Match of
		true -> ok;
		false ->
			timer:sleep(20),
			wait_for_metrics(Authority, Scope, Expected, N - 1)
	end.
