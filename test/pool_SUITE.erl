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
		degraded_configuration_error
	],
	[{random, [], Tests},
	 {least_loaded, [], Tests ++ [least_loaded_routing, least_loaded_round_robin,
		least_loaded_claim_released_without_request,
		least_loaded_claim_not_released_during_request]}].

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
		{ok, <<"Hello world!">>} = gun_pool:await_body(PoolStreamRef)
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

least_loaded_claim_released_without_request(Config) ->
	doc("Confirm that a connection claimed at checkout is released when "
		"no request follows, for example when the caller dies between "
		"checkout and request."),
	Port = config(port, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => least_loaded,
		size => 1,
		claim_timeout => 100
	}),
	gun_pool:await_up(ManagerPid),
	%% Checkout the only connection and never issue a request, as
	%% happens when a caller dies between checkout and gun:request.
	Self = self(),
	CheckoutPid = spawn(fun() ->
		{ConnPid, _Meta} = gun_pool:checkout(ManagerPid, #{}),
		Self ! {self(), checked_out, ConnPid}
	end),
	receive {CheckoutPid, checked_out, _} -> ok
	after 5000 -> error(checkout_timeout) end,
	%% The claim must expire and the connection become available again.
	ok = wait_all_streams_released(ManagerPid),
	%% With size 1 and HTTP/1.1 (1 stream max) a leaked claim would
	%% make this request fail with no_connection_available.
	{async, PoolStreamRef} = gun_pool:get("/", #{<<"host">> => Authority},
		#{scope => scope(?FUNCTION_NAME, Config)}),
	{response, nofin, 200, _} = gun_pool:await(PoolStreamRef),
	{ok, <<"Hello world!">>} = gun_pool:await_body(PoolStreamRef).

least_loaded_claim_not_released_during_request(Config) ->
	doc("Confirm that a claim confirmed by a request is not released "
		"again when the claim timeout fires while the request is "
		"still in flight."),
	Port = config(port, Config),
	Authority = ["localhost:", integer_to_binary(Port)],
	{ok, ManagerPid} = gun_pool:start_pool("localhost", Port, #{
		conn_opts => #{protocols => [http]},
		scope => scope(?FUNCTION_NAME, Config),
		lookup_strategy => least_loaded,
		size => 1,
		claim_timeout => 100
	}),
	gun_pool:await_up(ManagerPid),
	%% The delayed response takes 3000ms, much longer than the
	%% claim timeout of 100ms.
	{async, PoolStreamRef} = gun_pool:get("/delay", #{<<"host">> => Authority},
		#{scope => scope(?FUNCTION_NAME, Config)}),
	%% Wait until the claim timeout has fired, then confirm the
	%% stream is still counted.
	timer:sleep(500),
	{_, #{lookup := #{stream_counts := StreamCounts}}} = gun_pool:info(ManagerPid),
	[1] = maps:values(StreamCounts),
	{response, nofin, 200, _} = gun_pool:await(PoolStreamRef),
	{ok, <<"Hello world!">>} = gun_pool:await_body(PoolStreamRef),
	%% The count must return to exactly 0 (not below) once the
	%% stream completes.
	ok = wait_all_streams_released(ManagerPid),
	{async, PoolStreamRef2} = gun_pool:get("/", #{<<"host">> => Authority},
		#{scope => scope(?FUNCTION_NAME, Config)}),
	{response, nofin, 200, _} = gun_pool:await(PoolStreamRef2),
	{ok, <<"Hello world!">>} = gun_pool:await_body(PoolStreamRef2).

%% Poll the manager until every connection's stream count is back to 0.
wait_all_streams_released(ManagerPid) ->
	wait_all_streams_released(ManagerPid, 100).

wait_all_streams_released(_ManagerPid, 0) ->
	{error, timeout};
wait_all_streams_released(ManagerPid, N) ->
	{_, #{lookup := #{stream_counts := StreamCounts}}} = gun_pool:info(ManagerPid),
	case lists:all(fun(Count) -> Count =:= 0 end, maps:values(StreamCounts)) of
		true ->
			ok;
		false ->
			timer:sleep(10),
			wait_all_streams_released(ManagerPid, N - 1)
	end.
