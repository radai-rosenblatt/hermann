package net.radai.hermann.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.DefaultThreadFactory;
import net.radai.hermann.util.KafkaWireFormat;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ResponseHeader;

import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleNettyKafkaClient {
    private final AtomicInteger reqIdGenerator = new AtomicInteger(0);
    private final ConcurrentHashMap<Integer, OutstandingRequest> inFlight = new ConcurrentHashMap<>();
    private Channel channel;

    public SimpleNettyKafkaClient(String connectionString) {
        String[] parts = connectionString.split("\\s*:\\s*");
        String brokerHost = parts[0];
        int brokerPort = Integer.parseInt(parts[1]);

        ThreadFactory threadFactory = new DefaultThreadFactory("client");
        EventLoopGroup group = new EpollEventLoopGroup(1, threadFactory);
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group);
        bootstrap.channel(EpollSocketChannel.class); //TODO - provide factory to avoid reflection ctr
        bootstrap.option(ChannelOption.SO_KEEPALIVE, Boolean.TRUE);
        bootstrap.handler(new ChannelInitializer<EpollSocketChannel>() {
            @Override
            protected void initChannel(EpollSocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(
                        new LengthFieldBasedFrameDecoder(
                                ByteOrder.BIG_ENDIAN,
                                Integer.MAX_VALUE,
                                0,
                                4,
                                0,
                                0,
                                true
                        )
                );
                pipeline.addLast(new ResponseDispatcher());
            }
        });
        
        try {
            ChannelFuture channelFuture = bootstrap.connect(brokerHost, brokerPort).sync();
            channel = channelFuture.channel();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    
    public AbstractResponse syncRequest(AbstractRequest req) {
        int reqId = reqIdGenerator.incrementAndGet();
        ApiKeys requestType = KafkaWireFormat.keyFor(req);
        short version = requestType.latestVersion();
        byte[] payload = KafkaWireFormat.serialize(req, version, "bob", reqId);
        OutstandingRequest outstandingRequest = record(reqId, requestType, version);
        ChannelFuture future = channel.writeAndFlush(Unpooled.wrappedBuffer(payload));
        System.err.println("wrote " + req + " as " + payload.length + " bytes");
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                //TODO - be fancy and null out payload to save memory
                int h = 8;
            }
        });
        
        try {
            future.awaitUninterruptibly(); //wait for the flush to complete 1st
            return outstandingRequest.future.get(); //then wait for a response
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private OutstandingRequest record(int reqId, ApiKeys reqType, short reqVersion) {
        OutstandingRequest newOutstanding = new OutstandingRequest(reqId, reqType, reqVersion);
        OutstandingRequest oldOutstanding = inFlight.putIfAbsent(reqId, newOutstanding);
        if (oldOutstanding != null) {
            throw new IllegalStateException("request IDs non unique?!");
        }
        return newOutstanding;
    }
    
    private class ResponseDispatcher extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf responseFrame = (ByteBuf) msg;
            int size = responseFrame.capacity();
            byte[] bytes = new byte[size];
            responseFrame.readBytes(bytes);
            ResponseHeader responseHeader = KafkaWireFormat.deserializeHeader(bytes, 4);
            int correlation = responseHeader.correlationId();
            OutstandingRequest request = inFlight.remove(correlation);
            if (request == null) {
                throw new IllegalStateException("got response to unknown request " + correlation);
            }
            AbstractResponse parsedResponse = KafkaWireFormat.deserialize(bytes, request.type, request.version, 4);
            request.complete(parsedResponse);
            super.channelRead(ctx, msg);
        }
    }
    
    private static class OutstandingRequest {
        private final int correlationId;
        private final ApiKeys type;
        private final short version;
        private final CompletableFuture<AbstractResponse> future;

        OutstandingRequest(int correlationId, ApiKeys type, short version) {
            this.correlationId = correlationId;
            this.type = type;
            this.version = version;
            this.future = new CompletableFuture<>();
        }
        
        void complete(AbstractResponse response) {
            future.complete(response);
        }
    }
}
