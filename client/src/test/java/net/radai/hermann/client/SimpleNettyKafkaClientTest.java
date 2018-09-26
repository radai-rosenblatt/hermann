package net.radai.hermann.client;

import net.radai.hermann.testutil.AbstractIntegrationTest;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SimpleNettyKafkaClientTest extends AbstractIntegrationTest {
    
    @Test
    public void testBasicScenario() throws Exception {
        SimpleNettyKafkaClient client = new SimpleNettyKafkaClient(kafka.getConnectionString());

        int timeout = (int) TimeUnit.SECONDS.toMillis(30);
        
        ApiVersionsRequest.Builder versionsReqBuilder = new ApiVersionsRequest.Builder();
        ApiVersionsRequest versionsReq = versionsReqBuilder.build(ApiKeys.API_VERSIONS.latestVersion());
        ApiVersionsResponse versionsResp = (ApiVersionsResponse) client.syncRequest(versionsReq);

        MetadataRequest.Builder mdReqBuilder = new MetadataRequest.Builder(null, false);
        MetadataRequest mdReq = mdReqBuilder.build(ApiKeys.METADATA.latestVersion());
        MetadataResponse mdResp = (MetadataResponse) client.syncRequest(mdReq);
        Assert.assertEquals(0, mdResp.topicMetadata().size());

        Map<String, CreateTopicsRequest.TopicDetails> toCreate = new HashMap<>(1);
        toCreate.put("topicName", new CreateTopicsRequest.TopicDetails(1, (short) 1));
        CreateTopicsRequest.Builder createTopicReqBuilder = new CreateTopicsRequest.Builder(toCreate, timeout);
        CreateTopicsRequest ctReq = createTopicReqBuilder.build(ApiKeys.CREATE_TOPICS.latestVersion());
        CreateTopicsResponse ctResp = (CreateTopicsResponse) client.syncRequest(ctReq);

        MetadataResponse mdResp2 = (MetadataResponse) client.syncRequest(mdReq);
        Assert.assertEquals(1, mdResp2.topicMetadata().size());
    }
    
}
