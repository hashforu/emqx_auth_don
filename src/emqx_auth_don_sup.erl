%%--------------------------------------------------------------------
%% private
%%--------------------------------------------------------------------

-module(emqx_auth_don_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
	% in fact, start children here
    {ok, { {one_for_all, 0, 1}, []} }.

