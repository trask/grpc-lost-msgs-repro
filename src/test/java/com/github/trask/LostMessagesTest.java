package com.github.trask;

import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.github.trask.Hello.HelloReply;
import org.github.trask.Hello.HelloRequest;
import org.github.trask.HelloServiceGrpc;
import org.github.trask.HelloServiceGrpc.HelloService;
import org.junit.Test;

public class LostMessagesTest {

    @Test
    public void test() throws Exception {
        Server server = NettyServerBuilder.forPort(8025)
                .addService(HelloServiceGrpc.bindService(new HelloServiceImpl()))
                .build()
                .start();

        ManagedChannel channel = NettyChannelBuilder
                .forAddress("localhost", 8025)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();

        HelloService client = HelloServiceGrpc.newStub(channel);
        CountingStreamObserver countingStreamObserver = new CountingStreamObserver();

        final int total = 1000;

        for (int i = 0; i < total; i++) {
            client.hello(HelloRequest.getDefaultInstance(), countingStreamObserver);
        }

        System.out.println("sent " + total + " messages");

        while (countingStreamObserver.count.get() < total) {
            Thread.sleep(1000);
            System.out.println("received " + countingStreamObserver.count.get() + " messages");
        }

        channel.shutdown();
        server.shutdown();
    }

    private static class HelloServiceImpl implements HelloService {
        @Override
        public void hello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            responseObserver.onNext(HelloReply.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }

    private static class CountingStreamObserver implements StreamObserver<HelloReply> {

        private final AtomicInteger count = new AtomicInteger();

        @Override
        public void onNext(HelloReply value) {}

        @Override
        public void onError(Throwable t) {
            t.printStackTrace();
            count.getAndIncrement();
        }

        @Override
        public void onCompleted() {
            count.getAndIncrement();
        }
    }
}
