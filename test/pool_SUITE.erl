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
-import(gun_test, [receive_from/1]).

all() ->
	[{group, random}, {group, least_loaded}].

groups() ->
	Tests = [
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
		checkout_deadline_expired,
		checkout_deadline_future
	],
	[{random, [], Tests ++ [random_release_noop]},
	 {least_loaded, [], Tests ++ [least_loaded_routing, least_loaded_round_robin,
		least_loaded_priority_release, least_loaded_release_restores_capacity,
		least_loaded_release_duplicate_no_underflow,
		least_loaded_release_stale_duplicate_does_not_erase_live_claim,
		least_loaded_release_after_stream_completion_is_noop,
		least_loaded_claim_consumed_for_divergent_reply_to]}].

init_per_suite(Config) ->
	{ok, _} = cowboy:start_clear({?MODULE, tcp}, [], do_proto_opts()),
	Port = ranch:get_port({?MODULE, tcp}),
	[{port, Port}|Config].

end_per_suite(_) ->
	%% Stop listeners started by individual test cases in both groups.
	ExtraListeners = [
		max_streams_h2_size_1_random,
		max_streams_h2_size_1_retry_random,
		max_streams_h2_size_2_random,
		max_streams_h2_size_2_retry_random,
		reconnect_h1_random,
		max_streams_h2_size_1_least_loaded,
		max_streams_h2_size_1_retry_least_loaded,
		max_streams_h2_size_2_least_loaded,
		max_streams_h2_size_2_retry_least_loaded,
		reconnect_h1_least_loaded
	],
	_ = [cowboy:stop_listener(Listener) || Listener <- ExtraListeners],
	ok.

init_per_group(GroupName, Config) ->
	[{lookup_strategy, GroupName} | Config].

end_per_group(_, _) ->
	ok.

do_proto_opts() ->
	Routes = [
		{"/", hello_h, []},
		{"/delay", delayed_hello_h, 3000},
		{"/ws", ws_echo_h, []}
	],
	#{
		env => #{dispatch => cowboy_router:compile([{'_', Routes}])}
	}.

%% Builds a per-group listener name to avoid collisions between group runs.
listener_name(FuncName, Config) ->
	Strategy = config(lookup_strategy, Config),
	list_to_atom(atom_to_list(FuncName) ++ "_" ++ atom_to_list(Strategy)).

%% Builds a per-group scope to avoid ETS key collisions between group runs.
scope(FuncName, Config) ->
	{config(lookup_strategy, Config), FuncName}.

%% Tests.

hello_pool_h1(Config) ->
	doc("Confirm the pool can be used for HTTP/1.1 connections."),
	Port = config(port, Config),
	Strategy = config(lookup_strategy, Config),
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => Strategy
	}),
	gun_pool:await_up(ManagerPid),
	Streams = [{async, _} = gun_pool:get("/",
		#{<<"host">> => ["localhost:", integer_to_binary(Port)]},
		#{scope => scope(?FUNCTION_NAME, Config)}
	) || _ <- lists:seq(1, 8)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams].

hello_pool_h2(Config) ->
	doc("Confirm the pool can be used for HTTP/2 connections."),
	Port = config(port, Config),
	Strategy = config(lookup_strategy, Config),
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => Strategy
	}),
	gun_pool:await_up(ManagerPid),
	Streams = [{async, _} = gun_pool:get("/",
		#{<<"host">> => ["localhost:", integer_to_binary(Port)]},
		#{scope => scope(?FUNCTION_NAME, Config)}
	) || _ <- lists:seq(1, 800)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams].

hello_pool_ws(Config) ->
	doc("Confirm the pool can be used for HTTP/1.1 connections upgraded to Websocket."),
	Port = config(port, Config),
	Strategy = config(lookup_strategy, Config),
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{
			protocols => [http],
			ws_opts => #{
				default_protocol => pool_ws_handler,
				user_opts => self()
			}
		},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => Strategy,
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
		scope => scope(?FUNCTION_NAME, Config)
	}) || _ <- lists:seq(1, 8)],
	%% The pool_ws_handler module sends frames back to us.
	_ = [receive
		{text, <<"Hello world!">>} ->
			ok
	end || _ <- lists:seq(1, 8)].

max_streams_h1(Config) ->
	doc("Confirm requests are rejected when the maximum number "
		"of streams is reached for HTTP/1.1 connections."),
	Port = config(port, Config),
	Strategy = config(lookup_strategy, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => Strategy,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	{async, _} = gun_pool:get("/delay",
		#{<<"host">> => Authority}, #{scope => scope(?FUNCTION_NAME, Config)}),
	timer:sleep(500),
	{error, no_connection_available, _} = gun_pool:get("/delay",
		#{<<"host">> => Authority}, #{scope => scope(?FUNCTION_NAME, Config)}).

max_streams_h1_retry(Config) ->
	doc("Confirm connection checkout is retried when the maximum number "
		"of streams is reached for HTTP/1.1 connections."),
	Port = config(port, Config),
	Strategy = config(lookup_strategy, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => Strategy,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	{async, _} = gun_pool:get("/delay",
		#{<<"host">> => Authority}, #{scope => scope(?FUNCTION_NAME, Config)}),
	timer:sleep(500),
	{error, no_connection_available, _} = gun_pool:get("/delay",
		#{<<"host">> => Authority}, #{scope => scope(?FUNCTION_NAME, Config)}),
	{async, _} = gun_pool:get("/delay", #{<<"host">> => Authority}, #{
		checkout_retry => [100, 500, 500, 500, 500, 500, 500],
		scope => scope(?FUNCTION_NAME, Config)
	}).

max_streams_h2_size_1(Config) ->
	doc("Confirm requests are rejected when the maximum number "
		"of streams is reached for HTTP/2 connections."),
	Strategy = config(lookup_strategy, Config),
	Listener = listener_name(?FUNCTION_NAME, Config),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(Listener, [], ProtoOpts#{
		max_concurrent_streams => 5
	}),
	Port = ranch:get_port(Listener),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		lookup_strategy => Strategy,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	[{async, _} = gun_pool:get("/delay", #{<<"host">> => Authority}) || _ <- lists:seq(1, 5)],
	timer:sleep(500),
	{error, no_connection_available, _} = gun_pool:get("/delay", #{<<"host">> => Authority}).

max_streams_h2_size_1_retry(Config) ->
	doc("Confirm connection checkout is retried when the maximum number "
		"of streams is reached for HTTP/2 connections."),
	Strategy = config(lookup_strategy, Config),
	Listener = listener_name(?FUNCTION_NAME, Config),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(Listener, [], ProtoOpts#{
		max_concurrent_streams => 5
	}),
	Port = ranch:get_port(Listener),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		lookup_strategy => Strategy,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	[{async, _} = gun_pool:get("/delay", #{<<"host">> => Authority}) || _ <- lists:seq(1, 5)],
	timer:sleep(500),
	{error, no_connection_available, _} = gun_pool:get("/delay", #{<<"host">> => Authority}),
	{async, _} = gun_pool:get("/delay", #{<<"host">> => Authority}, #{
		checkout_retry => [100, 500, 500, 500, 500, 500, 500]
	}).

max_streams_h2_size_2(Config) ->
	doc("Confirm requests are rejected when the maximum number "
		"of streams is reached for HTTP/2 connections."),
	Strategy = config(lookup_strategy, Config),
	Listener = listener_name(?FUNCTION_NAME, Config),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(Listener, [], ProtoOpts#{
		max_concurrent_streams => 5
	}),
	Port = ranch:get_port(Listener),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		lookup_strategy => Strategy,
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

max_streams_h2_size_2_retry(Config) ->
	doc("Confirm connection checkout is retried when the maximum number "
		"of streams is reached for HTTP/2 connections."),
	Strategy = config(lookup_strategy, Config),
	Listener = listener_name(?FUNCTION_NAME, Config),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(Listener, [], ProtoOpts#{
		max_concurrent_streams => 5
	}),
	Port = ranch:get_port(Listener),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		lookup_strategy => Strategy,
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

kill_restart_h1(Config) ->
	doc("Confirm the Gun process is restarted and the pool operational "
		"after an HTTP/1.1 Gun process has crashed."),
	Port = config(port, Config),
	Strategy = config(lookup_strategy, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => Strategy
	}),
	gun_pool:await_up(ManagerPid),
	Streams1 = [{async, _} = gun_pool:get("/",
		#{<<"host">> => Authority},
		#{scope => scope(?FUNCTION_NAME, Config)}
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
		#{scope => scope(?FUNCTION_NAME, Config)}
	) || _ <- lists:seq(1, 8)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams2].

kill_restart_h2(Config) ->
	doc("Confirm the Gun process is restarted and the pool operational "
		"after an HTTP/2 Gun process has crashed."),
	Port = config(port, Config),
	Strategy = config(lookup_strategy, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => Strategy
	}),
	gun_pool:await_up(ManagerPid),
	Streams1 = [{async, _} = gun_pool:get("/",
		#{<<"host">> => Authority},
		#{scope => scope(?FUNCTION_NAME, Config)}
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
		#{scope => scope(?FUNCTION_NAME, Config)}
	) || _ <- lists:seq(1, 800)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams2].

%% @todo kill_restart_ws

reconnect_h1(Config) ->
	doc("Confirm the Gun process reconnects automatically for HTTP/1.1 connections."),
	Strategy = config(lookup_strategy, Config),
	Listener = listener_name(?FUNCTION_NAME, Config),
	ProtoOpts = do_proto_opts(),
	{ok, _} = cowboy:start_clear(Listener, [], ProtoOpts#{
		idle_timeout => 500
	}),
	Port = ranch:get_port(Listener),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		lookup_strategy => Strategy
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
	Strategy = config(lookup_strategy, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => Strategy
	}),
	gun_pool:await_up(ManagerPid),
	Streams1 = [{async, _} = gun_pool:get("/",
		#{<<"host">> => Authority},
		#{scope => scope(?FUNCTION_NAME, Config)}
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
		#{scope => scope(?FUNCTION_NAME, Config)}
	) || _ <- lists:seq(1, 800)],
	_ = [begin
		{response, nofin, 200, _} = gun_pool:await(StreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(StreamRef)
	end || {async, StreamRef} <- Streams2].

%% @todo reconnect_ws

stop_pool(Config) ->
	doc("Confirm the pool can be stopped."),
	Port = config(port, Config),
	Strategy = config(lookup_strategy, Config),
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => Strategy
	}),
	gun_pool:await_up(ManagerPid),
	gun_pool:stop_pool("localhost", Port, #{scope => scope(?FUNCTION_NAME, Config)}).

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
	Strategy = config(lookup_strategy, Config),
	%% We attempt to connect to an unreachable IP.
	{ok, ManagerPid} = gun_pool:start_pool({20, 20, 20, 1}, Port, #{
		conn_opts => #{tcp_opts => [{ip, {127, 0, 0, 1}}]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => Strategy,
		size => 1
	}),
	%% Wait for the lookup/connect to fail.
	timer:sleep(500),
	{degraded, #{conns := Conns}} = gun_pool:info(ManagerPid),
	true = Conns =:= #{},
	%% We can stop the pool even if degraded.
	gun_pool:stop_pool({20, 20, 20, 1}, Port, #{scope => scope(?FUNCTION_NAME, Config)}).

least_loaded_routing(Config) ->
	doc("Confirm the least_loaded strategy always routes to the connection "
		"with the fewest active streams."),
	Port = config(port, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => least_loaded,
		size => 2
	}),
	gun_pool:await_up(ManagerPid),
	%% Occupy one connection with a slow request.
	{async, {BusyConn, _}} = gun_pool:get("/delay",
		#{<<"host">> => Authority}, #{scope => scope(?FUNCTION_NAME, Config)}),
	%% Send requests one at a time and await each before the next.
	%% This keeps the idle connection at 0-1 streams, always below the
	%% busy connection, so least_loaded must consistently pick it.
	%% With random selection this would pass with probability ~0.1%.
	_ = [begin
		{async, {ConnPid, _} = PoolStreamRef} = gun_pool:get("/",
			#{<<"host">> => Authority}, #{scope => scope(?FUNCTION_NAME, Config)}),
		true = ConnPid =/= BusyConn,
		{response, nofin, 200, _} = gun_pool:await(PoolStreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(PoolStreamRef),
		%% Wait for this request's asynchronous release before issuing
		%% the next one: with it still pending both connections sit at
		%% one stream and the tie-break can route to the busy one.
		ok = wait_stream_total(ManagerPid, 1)
	end || _ <- lists:seq(1, 10)].

least_loaded_round_robin(Config) ->
	doc("Confirm the least_loaded strategy rotates across equally-loaded "
		"connections: with 5 connections and 5 sequential requests, every 
		connection is used exactly once."),
	Port = config(port, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	Size = 5,
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http2]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => least_loaded,
		size => Size
	}),
	gun_pool:await_up(ManagerPid),

	ConnPids = [begin
		{async, {ConnPid, _} = PoolStreamRef} = gun_pool:get("/",
			#{<<"host">> => Authority}, #{scope => scope(?FUNCTION_NAME, Config)}),
		{response, nofin, 200, _} = gun_pool:await(PoolStreamRef),
		{ok, <<"Hello world!">>} = gun_pool:await_body(PoolStreamRef),
		%% The stream is released asynchronously (the event handler casts
		%% {release_stream, _} to the manager), so wait until every connection
		%% is back to 0 streams before issuing the next request.
		ok = wait_all_streams_released(ManagerPid),
		ConnPid
	end || _ <- lists:seq(1, Size)],
	%% Every connection must have been used exactly once.
	Size = length(lists:usort(ConnPids)).

least_loaded_priority_release(Config) ->
	doc("Confirm least_loaded stream releases overtake queued ordinary messages and update the manager."),
	ReleaseAlias = alias([priority]),
	StreamRef = make_ref(),
	Receiver = self(),
	ReplyTo = self(),
	_ = spawn(fun() ->
		Receiver ! ordinary_sentinel,
		Receiver ! ordinary_sentinel_enqueued
	end),
	receive
		ordinary_sentinel_enqueued -> ok
	end,
	PrioritySender = spawn(fun() ->
		_ = gun_pool_events_h:request_end(#{stream_ref => StreamRef}, #{
			strategy => least_loaded,
			release_to => ReleaseAlias,
			StreamRef => {nofin, fin, ReplyTo}
		}),
		Receiver ! priority_release_enqueued
	end),
	receive
		priority_release_enqueued -> ok
	end,
	receive
		{release_stream, PrioritySender, ReplyTo} -> ok;
		ordinary_sentinel -> ct:fail(priority_release_not_prioritized)
	end,
	_ = unalias(ReleaseAlias),
	Port = config(port, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => least_loaded,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	{operational, #{lookup := #{release_alias := ManagerReleaseAlias}}} = gun_pool:info(ManagerPid),
	true = is_reference(ManagerReleaseAlias),
	{async, PoolStreamRef} = gun_pool:get("/", #{<<"host">> => Authority}, #{
		scope => scope(?FUNCTION_NAME, Config)
	}),
	{response, nofin, 200, _} = gun_pool:await(PoolStreamRef),
	{ok, <<"Hello world!">>} = gun_pool:await_body(PoolStreamRef),
	ok = wait_all_streams_released(ManagerPid).

least_loaded_release_restores_capacity(Config) ->
	doc("Confirm gun_pool:release/1 restores checkout capacity for a "
		"least_loaded connection that was claimed but never used, "
		"for example when the caller's own deadline expired before "
		"issuing a request on the checked out connection."),
	Port = config(port, Config),
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => least_loaded,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	{ConnPid, Meta} = gun_pool:checkout(ManagerPid, #{}),
	true = is_map_key(claim_ref, Meta),
	undefined = gun_pool:checkout(ManagerPid, #{}),
	ok = gun_pool:release(Meta),
	{ConnPid, _Meta} = gun_pool:checkout(ManagerPid, #{}).

least_loaded_release_duplicate_no_underflow(Config) ->
	doc("Confirm a duplicate gun_pool:release/1 call for the same "
		"connection does not underflow its stream count below zero "
		"and leaves the pool usable."),
	Port = config(port, Config),
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => least_loaded,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	{ConnPid, Meta} = gun_pool:checkout(ManagerPid, #{}),
	ok = gun_pool:release(Meta),
	ok = gun_pool:release(Meta),
	{_, #{lookup := #{stream_counts := StreamCounts}}} = gun_pool:info(ManagerPid),
	0 = maps:get(ConnPid, StreamCounts),
	{ConnPid, _Meta} = gun_pool:checkout(ManagerPid, #{}).

least_loaded_release_stale_duplicate_does_not_erase_live_claim(Config) ->
	doc("Confirm a duplicate gun_pool:release/1 call for a claim already "
		"consumed does not erase a different, live claim made on the "
		"same connection in the meantime: the claims are tokens, matched "
		"individually, not a single count matched by connection pid."),
	Port = config(port, Config),
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => least_loaded,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	{ConnPid, MetaA} = gun_pool:checkout(ManagerPid, #{}),
	ok = gun_pool:release(MetaA),
	{ConnPid, _MetaB} = gun_pool:checkout(ManagerPid, #{}),
	%% Stale/duplicate: claim A's token was already consumed above.
	ok = gun_pool:release(MetaA),
	undefined = gun_pool:checkout(ManagerPid, #{}),
	{_, #{lookup := #{stream_counts := StreamCounts}}} = gun_pool:info(ManagerPid),
	1 = maps:get(ConnPid, StreamCounts).

least_loaded_release_after_stream_completion_is_noop(Config) ->
	doc("Confirm a gun_pool:release/1 call for a claim already consumed "
		"by ordinary end-of-stream bookkeeping does not erase a "
		"different, live claim made on the same connection afterwards."),
	Port = config(port, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => least_loaded,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	{ConnPid, MetaA} = gun_pool:checkout(ManagerPid, #{}),
	%% The request must be complete (fin): an unfinished request body
	%% makes the HTTP/1.1 connection unreusable and gun tears it down,
	%% racing the assertions below against the reconnect.
	StreamRef = gun:request(ConnPid, <<"GET">>, "/", #{<<"host">> => Authority}, <<>>),
	{response, nofin, 200, _} = gun_pool:await({ConnPid, StreamRef}),
	{ok, <<"Hello world!">>} = gun_pool:await_body({ConnPid, StreamRef}),
	ok = wait_all_streams_released(ManagerPid),
	{ConnPid, _MetaB} = gun_pool:checkout(ManagerPid, #{}),
	%% Stale: claim A's token was already consumed when its stream ended.
	ok = gun_pool:release(MetaA),
	undefined = gun_pool:checkout(ManagerPid, #{}),
	{_, #{lookup := #{stream_counts := StreamCounts}}} = gun_pool:info(ManagerPid),
	1 = maps:get(ConnPid, StreamCounts).

least_loaded_claim_consumed_for_divergent_reply_to(Config) ->
	doc("Confirm a stream whose reply_to differs from the checkout "
		"caller still consumes its claim at end of stream, so the "
		"claim cannot accumulate or fund a later stale release."),
	Port = config(port, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => least_loaded,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	{ConnPid, MetaA} = gun_pool:checkout(ManagerPid, #{}),
	ReplyTo = spawn(fun() -> receive stop -> ok end end),
	_StreamRef = gun:request(ConnPid, <<"GET">>, "/",
		#{<<"host">> => Authority}, <<>>, #{reply_to => ReplyTo}),
	%% The response goes to ReplyTo; observe completion via the
	%% manager's stream counts instead.
	ok = wait_all_streams_released(ManagerPid),
	{_, #{lookup := #{claims := Claims, claim_index := ClaimIndex}}}
		= gun_pool:info(ManagerPid),
	0 = map_size(Claims),
	0 = map_size(ClaimIndex),
	%% Stale: claim A must not be able to erase a live claim.
	{ConnPid, _MetaB} = gun_pool:checkout(ManagerPid, #{}),
	ok = gun_pool:release(MetaA),
	undefined = gun_pool:checkout(ManagerPid, #{}),
	{_, #{lookup := #{stream_counts := StreamCounts}}} = gun_pool:info(ManagerPid),
	1 = maps:get(ConnPid, StreamCounts),
	ReplyTo ! stop,
	ok.

random_release_noop(Config) ->
	doc("Confirm gun_pool:release/1 is an ok no-op for the random "
		"strategy, which does not claim a connection at checkout time."),
	Port = config(port, Config),
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => random,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	{ConnPid, Meta} = gun_pool:checkout(ManagerPid, #{}),
	false = is_map_key(release_to, Meta),
	false = is_map_key(claim_ref, Meta),
	ok = gun_pool:release(Meta).

checkout_deadline_expired(Config) ->
	doc("Confirm checkout with an already-expired checkout_deadline "
		"returns undefined immediately without claiming a connection."),
	Port = config(port, Config),
	Strategy = config(lookup_strategy, Config),
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => Strategy,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	Deadline = erlang:monotonic_time(millisecond) - 1,
	undefined = gun_pool:checkout(ManagerPid, #{checkout_deadline => Deadline}),
	%% Nothing was claimed: a follow-up checkout still succeeds.
	{_ConnPid, _Meta} = gun_pool:checkout(ManagerPid, #{}).

checkout_deadline_future(Config) ->
	doc("Confirm checkout with a checkout_deadline in the future "
		"behaves like an ordinary checkout."),
	Port = config(port, Config),
	Strategy = config(lookup_strategy, Config),
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => Strategy,
		size => 1
	}),
	gun_pool:await_up(ManagerPid),
	Deadline = erlang:monotonic_time(millisecond) + 5000,
	{_ConnPid, _Meta} = gun_pool:checkout(ManagerPid, #{checkout_deadline => Deadline}).

%% Poll the manager until every connection's stream count is back to 0.
wait_all_streams_released(ManagerPid) ->
	wait_all_streams_released(ManagerPid, 100).

wait_all_streams_released(_ManagerPid, 0) ->
	{error, timeout};
wait_all_streams_released(ManagerPid, N) ->
	{_, #{lookup := #{stream_counts := StreamCounts}}} = gun_pool:info(ManagerPid),
	%% The map_size check guards against a vacuous pass while every
	%% connection is down (down conns lose their stream_counts entry).
	case map_size(StreamCounts) > 0
			andalso lists:all(fun(Count) -> Count =:= 0 end, maps:values(StreamCounts)) of
		true ->
			ok;
		false ->
			timer:sleep(10),
			wait_all_streams_released(ManagerPid, N - 1)
	end.

%% Wait until the manager has processed enough end-of-stream releases
%% that the total stream count equals Total. Needed because releases
%% arrive asynchronously from the connection processes: awaiting a
%% response does not guarantee the manager's counts reflect it yet.
wait_stream_total(ManagerPid, Total) ->
	wait_stream_total(ManagerPid, Total, 100).

wait_stream_total(_ManagerPid, _Total, 0) ->
	{error, timeout};
wait_stream_total(ManagerPid, Total, N) ->
	{_, #{lookup := #{stream_counts := StreamCounts}}} = gun_pool:info(ManagerPid),
	case lists:sum(maps:values(StreamCounts)) of
		Total ->
			ok;
		_ ->
			timer:sleep(10),
			wait_stream_total(ManagerPid, Total, N - 1)
	end.
