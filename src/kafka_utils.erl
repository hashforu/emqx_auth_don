%% use cache for kafka to send msg in batch

-module(kafka_utils).
-behaviour(gen_server).

-export([
    start/1
]).

-export([loop/1]).

-record(msgbatch, {
    length = 0,
    buffer = []
}).


start_link()->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

send_batch(Msg)->
    gen_server:cast(?MODULE, Msg).

%%% ===================
%   gen_server callbacks
%%% ===================

init(_Env)->
    io:format("start kafka utils server...~n"),
    % do only if brod running
    {ok, _} = application:ensure_all_started(brod),

    % load cnf
    %% load cnf
    {ok, BrokerValues} = application:get_env(emqx_hook_don, broker),
    Host = proplists:get_value(host, BrokerValues),
    Port = proplists:get_value(port, BrokerValues),
    Topic = proplists:get_value(topic, BrokerValues),
    Strategy = proplists:get_value(strategy, BrokerValues),

    application:set_env(emqx_hook_don, kafka_hook_publish, list_to_binary(Topic)),
    application:set_env(emqx_hook_don, kafka_strategy, list_to_atom(Strategy)),

    KafkaBootstrapEndpoints = [{Host, Port}],
    ClientConfig = [],
    ok = brod:start_client(KafkaBootstrapEndpoints, brod_client_1, ClientConfig),
    ok = brod:start_producer(brod_client_1, list_to_binary(Topic), _ProducerConfig = []),

    io:format("Init kafka with ~p success ~n", [KafkaBootstrapEndpoints]),

    % make a store for msg batch
    MsgBatch = #msgbatch{length=0, buffer=[]},

    {ok, #msgbatch{length=0, buffer=[]}}.

handle_call(send_batch, Msg, #msgbatch{length:=Len, buffer:=Buff} = State)->
    case Len > 3 of
        true->
            brod


