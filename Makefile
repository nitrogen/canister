all:
	rebar3 compile

run1:
	rebar3 shell --name can_1@127.0.0.1

run2:
	rebar3 shell --name can_2@127.0.0.1

run3:
	rebar3 shell --name can_3@127.0.0.1

run4:
	rebar3 shell --name can_4@127.0.0.1

