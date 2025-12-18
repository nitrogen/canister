# Canister

An Erlang Session Management system

The standard session management system for [Nitrogen](https://nitrogenproject.com)

## Features

- Data Persistence (stored in mnesia)
- Data is replicated across all connected node
- Automatic self-healing in the event of split brain, new nodes being added to the cluster, and more.
- Expired sessions automatically deleted accordingly

## Include the dependency

Add as a rebar dependency in your rebar.config

```erlang
{deps, [canister]}.
```

## Start It

Put it in your `app.src` file or start it explicitly with:

```
application:ensure_all_started(canister).
```

## Functionality

### Save a Session Variable

```erlang
canister:put(SessionID, Key, Value).
```

Returns: Previously stored `Value` or `undefined`

### Retrieving a Session Variable

```erlang
canister:get(SessionID, Key).
```

Returns: `Value` or `undefined`

### Inspecting specific Session Info

```erlang
canister:session_info(SessionID).
```

Retrieves all data variables as well as relevant dates and times: when it was last accessed, updated, or deleted

## Configuration

See [canister.config](https://github.com/nitrogen/canister/blob/master/src/canister_config.erl) for explanation of configuration variables.

**Note**: `session_timeout` variable will also check the `nitrogen` and `nitrogen_core` app configs, just in case.

## Changelog

### 0.1.1

- Fix some Warnings
- Improve some backwards compatibility with rebar2
- Switch `canister_log` to use `logger`
- Improve the build process a bit

### 0.1.0

- Initial Release

## License

Copyright 2022 Jesse Gumm

[MIT Licensed](https://github.com/nitrogen/canister/blob/master/LICENSE)
