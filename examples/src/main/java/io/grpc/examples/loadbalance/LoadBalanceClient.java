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

import io.grpc.*;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.examples.helloworld.HelloWorldClient;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LoadBalanceClient {
    private static final Logger logger = Logger.getLogger(LoadBalanceClient.class.getName());

    public static final String exampleScheme = "example";
    public static final String exampleServiceName = "lb.example.grpc.io";

    public static final String model1ServiceName = "model1.grpc.io";
    public static final String model2ServiceName = "model2.grpc.io";
    public static final String model3ServiceName = "model3.grpc.io";


    private final GreeterGrpc.GreeterBlockingStub blockingStub;

    public LoadBalanceClient(Channel channel) {
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
        }
        logger.info("Greeting: " + response.getMessage());
    }


    public static void main(String[] args) throws Exception {
        NameResolverRegistry.getDefaultRegistry().register(new ExampleNameResolverProvider());

        String target = String.format("%s:///%s", exampleScheme, exampleServiceName);
        String target1 = String.format("%s:///%s", exampleScheme, model1ServiceName);
        String target2 = String.format("%s:///%s", exampleScheme, model2ServiceName);


        logger.info("Use default first_pick load balance policy");
//        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
//                .usePlaintext()
//                .build();
//        try {
//            LoadBalanceClient client = new LoadBalanceClient(channel);
//            for (int i = 0; i < 5; i++) {
//                client.greet("request" + i);
//            }
//        } finally {
//            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
//        }

        logger.info("Change to round_robin policy");
        ManagedChannel channel1 = ManagedChannelBuilder.forTarget(target1)
                .defaultLoadBalancingPolicy("round_robin")
                .usePlaintext()
                .build();
        ManagedChannel channel2 = ManagedChannelBuilder.forTarget(target2)
                .defaultLoadBalancingPolicy("round_robin")
                .usePlaintext()
                .build();
        try {
            LoadBalanceClient client1 = new LoadBalanceClient(channel1);
            LoadBalanceClient client2 = new LoadBalanceClient(channel2);

            while(true) {
                for (int j = 0; j < 1; j++) {
                    //Thread.sleep(20000);
                    for (int i = 0; i < 10; i++) {
                        if (i % 2 == 0) {
                            logger.info("calling client1");
                            client1.greet("request" + i);
                        } else {
                            logger.info("calling client2");
                            client2.greet("request" + i);
                        }
                    }
                }
                Thread.sleep(15000);
            }
        } finally {
            channel1.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
