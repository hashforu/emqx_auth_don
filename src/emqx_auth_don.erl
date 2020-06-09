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

-module(emqx_auth_don).

-include_lib("emqx/include/emqx.hrl").

-record(jwt_claims, {
    uid = undifined,
    expired = 0
}).

-export([ load/1
        , unload/0
        ]).

%% Client Lifecircle Hooks
-export([
        on_client_authenticate/3
        , on_client_check_acl/5
        ]).


%% Called when the plugin application start
load(Env) ->
    emqx:hook('client.authenticate', {?MODULE, on_client_authenticate, [Env]}),
    emqx:hook('client.check_acl',    {?MODULE, on_client_check_acl, [Env]}).

%%--------------------------------------------------------------------
%% Client Lifecircle Hooks
%%--------------------------------------------------------------------
%% 想在此处设计简单的密码验证
%% 并保留官方插件webhook的方式
%% md5盐配置
%% 该方式需要考虑加密盐的更换，并及时通知客户端进行key的重新获取
on_client_authenticate(_ClientInfo = #{clientid := ClientId, username := UserName, password := PassWord}, Result, _Env) ->
    io:format("Don:Client(~s) authenticate, Result:~n~p~n~p~n", [ClientId, Result, _ClientInfo]),
    io:format("username: ~s ~n password: ~s ~n", [UserName, PassWord]),
    %%{ok, Result}.
    %%From = 
    case binary:first(UserName) of
        100 ->
            % 100 means d, device
            io:format("comes from device ~s~n", [UserName]),
            {stop, Result#{
                auth_result => success,
                anonymous => false
            }};
        117 ->
            % 117 means u, user
            [_|Uid] = UserName,

            {Result, Detail} = decode_jwt(PassWord),

            case Result of
                ok ->
                    #jwt_claims{uid:= _Uid} = Detail,
                    {stop, Result#{
                        auth_result => success,
                        anonymous => false
                        }
                    };
                error->
                    {stop, Result#{
                        auth_result => Detail,
                        anonymous => false                    
                    };
                _->{
                   stop, Result#{
                        auth_result => unexpected,
                        anonymous => false                 
                   }
                }
            end;
        _ ->
            io:format("comes from others ~s~n", [UserName]),
            {stop, Result#{
                auth_result => unrecognize,
                anonymous => false
            }
            }
    end.
    %%{stop, deny}.

%% 设备端可以直接在此处完成简单的acl校验（设备订阅发布的主题格式固定）
%% 手机端，必须交出去由web服务，进行验证（用户订阅会根据拥有设备不同动态变化）
on_client_check_acl(_ClientInfo = #{clientid := ClientId}, Topic, PubSub, Result, _Env) ->
    io:format("Don:Client(~s) check_acl, PubSub:~p, Topic:~p, Result:~p~n",
              [ClientId, PubSub, Topic, Result]),
    {ok, Result}.

%%--------------------------------------------------------------------
%% Session Lifecircle Hooks
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% Called when the plugin application stop
unload() ->
    emqx:unhook('client.authenticate', {?MODULE, on_client_authenticate}),
    emqx:unhook('client.check_acl',    {?MODULE, on_client_check_acl})


%% utils for authenticate
decode_jwt(Jwt) ->
    try
        {ok, Claims} = jwerl:verify(Jwt),
        #{uid := Uid, expired := Expired} = Claims,
        io:format("Uid: ~p ~n Expired: ~p~n", [Uid,Expired]),

        ExpiredInt = safe_to_int(Expired), 

        case ExpiredInt  =< erlang:timestamp() of
            true ->
                io:format("token expired!"),
                {error, expired_token};
            _->
                io:format("verify success!~n uid: ~p ~n", [Uid]),
                {ok, Claims}
        end
    catch
        _:Reason ->
            io:format("error: ~p ~n" , [Reason]),
            {error, Reason}
    end.

safe_to_int(Int) ->
    case is_integer(Int) of
        true ->
            Int;
        false ->
            erlang:list_to_integer(Int)
    end.