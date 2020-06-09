%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------


%% 该插件只负责将所有发布消息聚合到kafka
-module(emqx_hook_don).

-include_lib("emqx/include/emqx.hrl").
-include("emqx_hook_don.hrl").

-export([ load/1
        , unload/0
        ]).

%% Message Pubsub Hooks
-export([ on_message_publish/2
        ]).

%% Called when the plugin application start
load(Env) ->
    % 初始化kafka客户端
    brod_init([Env]),
    % 声明一个消息数组，进行消息的批量发送
    % 该方式不可行，状态机需要在loop中，但是不可能重复hook和取消hook
    % 即store无法存储，除非单起一个进程，进行状态保存
    % 但是，测试发现，单独的worker崩溃导致插件app崩溃，且不reload
    % 暂时不使用批量发送，直接每条消息单独发送即可
    % 未知client实现方式，如果是一直维持的话，批量发送不一定会比单条消息发送节省多少资源
    % MsgBatch = [],
    % hook publish钩子
    emqx:hook('message.publish',     {?MODULE, on_message_publish, [Env]}).

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% Transform message and return
%% 这里是hook到系统消息，忽略
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};
%% hook到的正式消息
on_message_publish(Message, _Env) ->
    % io:format("Publish ~s~n", [emqx_message:format(Message)]),
    % 格式化消息
    {ok, Payload} = format_payload(Message),
    %send_msg(),
    % 发送消息，异步、不校验返回
    send_msg(Payload),
    % 消息继续流向下一个插件
    {ok, Message}.

%% Called when the plugin application stop
unload() ->
    emqx:unhook('message.publish',     {?MODULE, on_message_publish}).

%% utils for kafka

send_msg(Msg) ->
    Topic = get_topic(),
    Partition = get_partition(),
    Key = get_key(),
    % 异步发送，不校验返回值
    brod:produce(brod_client_1, Topic, Partition, Key, Msg),

    ok.

%% get random partition
%% or polling
%% depends on setting
get_partition() ->
    {ok, Strategy} = application:get_env(emqx_hook_don, kafka_strategy),
    io:format("kafka strategy is : ~p ~n", [Strategy]),
    case Strategy of
        random -> <<>>;
        _->0
    end.

%% get topic
get_topic() ->
    {ok, Topic} = application:get_env(emqx_hook_don, kafka_hook_publish),
    %io:format("kafka Topic is : ~p ~n", [Topic]),
    Topic.

%% get Key
get_key() ->
    <<>>.

format_payload(Message) ->
    io:format("Message is : ~p ~n", [Message]),
    {FromClientId, FromUsername} = parse_from(Message),

    Payload = [
        {clientid, FromClientId},
        {from_username, FromUsername},
        {topic, Message#message.topic},
        {qos, Message#message.qos},
        {payload, Message#message.payload},
        {ts, Message#message.timestamp}
    ],
    
    io:format("Message is : ~p ~n", [Payload]),
    {ok, Payload}.

brod_init(_Env) ->
    io:format("~n正在启动kafka客户端....~n"),
    {ok, _} = application:ensure_all_started(brod),

    %% load cnf
    {ok, BrokerValues} = application:get_env(emqx_hook_don, broker),
    Host = proplists:get_value(host, BrokerValues),
    Port = proplists:get_value(port, BrokerValues),
    Topic = proplists:get_value(topic, BrokerValues),
    Strategy = proplists:get_value(strategy, BrokerValues),

    application:set_env(emqx_hook_don, kafka_hook_publish, list_to_binary(Topic)),
    application:set_env(emqx_hook_don, kafka_strategy, list_to_atom(Strategy)),

    KafkaBootstrapEndpoints = [{Host, Port}],
    %io:format("~p~n~p", [BrokerValues, KafkaBootstrapEndpoints]),
    ClientConfig = [],
    ok = brod:start_client(KafkaBootstrapEndpoints, brod_client_1, ClientConfig),
    ok = brod:start_producer(brod_client_1, list_to_binary(Topic), _ProducerConfig = []),

    io:format("Init kafka with ~p~n", [KafkaBootstrapEndpoints]).


parse_from(#message{from = ClientId, headers = #{username := Username}}) ->
    {ClientId, maybe(Username)};
parse_from(#message{from = ClientId, headers = _HeadersNoUsername}) ->
    {ClientId, maybe(undefined)}.

maybe(undefined) -> null;
maybe(Str) -> Str.