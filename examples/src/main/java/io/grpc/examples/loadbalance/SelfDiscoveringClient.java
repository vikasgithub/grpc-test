/*
 * Copyright 2022 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.examples.loadbalance;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.grpc.*;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SelfDiscoveringClient {
    private static final Logger logger = Logger.getLogger(SelfDiscoveringClient.class.getName());

    public static final String exampleScheme = "example";
    public static final String exampleServiceName = "lb.example.grpc.io";

    public static final String model1ServiceName = "model1.grpc.io";
    public static final String model2ServiceName = "model2.grpc.io";
    public static final String model3ServiceName = "model3.grpc.io";
    private static List<ManagedChannel> channels = new ArrayList<>();

    private final GreeterGrpc.GreeterBlockingStub blockingStub;

    public SelfDiscoveringClient(Channel channel) {
        blockingStub = GreeterGrpc.newBlockingStub(channel);
    }

    public void greet(String name) {
        HelloRequest request = HelloRequest.newBuilder().setName(name).build();
        HelloReply response;
        try {
            response = blockingStub.sayHello(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        } catch (Exception e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e);
            return;
        }
        logger.info("Greeting: " + response.getMessage());
    }


    public static void main(String[] args) throws Exception {
        try {
            NameResolverRegistry.getDefaultRegistry().register(new SelfDiscoveringNameResolverProvider());
            Map<String, SelfDiscoveringClient> modelClientMap = createModelClientMap();
            for (int i = 0; i < 15; i++) {
                int j = i % 3;
                String modelName = "model" + (j + 1);
                SelfDiscoveringClient client = modelClientMap.get(modelName);
                logger.info("calling model " + modelName);
                client.greet(modelName);
            }
        } catch(Exception exception) {
            System.out.println(exception.getMessage());
        } finally {
            for (ManagedChannel channel : channels) {
                channel.shutdownNow();
            }
        }
    }

    private static Map<String, SelfDiscoveringClient> createModelClientMap() {
        String target1 = String.format("%s:///%s", exampleScheme, model1ServiceName);
        ManagedChannel channel1 = ManagedChannelBuilder.forTarget(target1)
                .defaultLoadBalancingPolicy("round_robin")
                .usePlaintext()
                .build();
        channels.add(channel1);
        SelfDiscoveringClient client1 = new SelfDiscoveringClient(channel1);

        String target2 = String.format("%s:///%s", exampleScheme, model2ServiceName);
        ManagedChannel channel2 = ManagedChannelBuilder.forTarget(target2)
                .defaultLoadBalancingPolicy("round_robin")
                .usePlaintext()
                .build();
        channels.add(channel2);
        SelfDiscoveringClient client2 = new SelfDiscoveringClient(channel2);

        String target3 = String.format("%s:///%s", exampleScheme, model3ServiceName);
        ManagedChannel channel3 = ManagedChannelBuilder.forTarget(target3)
                .defaultLoadBalancingPolicy("round_robin")
                .usePlaintext()
                .build();
        channels.add(channel3);
        SelfDiscoveringClient client3 = new SelfDiscoveringClient(channel3);
        Map<String, SelfDiscoveringClient> modelClientMap = ImmutableMap.<String, SelfDiscoveringClient>builder().
                put("model1", client1).
                put("model2", client2).
                put("model3", client3).build();
        return modelClientMap;
    }
}
