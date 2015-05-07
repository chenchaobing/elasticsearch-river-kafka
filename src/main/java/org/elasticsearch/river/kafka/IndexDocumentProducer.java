/*
 * Copyright 2014 Mariam Hakobyan
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
package org.elasticsearch.river.kafka;

import kafka.message.MessageAndMetadata;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.codahale.metrics.Meter;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Producer to index documents. Creates index document requests, which are executed with Bulk API.
 *
 * @author Mariam Hakobyan
 */
public class IndexDocumentProducer extends ElasticSearchProducer {
	
	private static final ESLogger logger = ESLoggerFactory.getLogger(IndexDocumentProducer.class.getName());
	
	private static final String INDEX = "index";
	private static final String TYPE = "type";
	private static final String ID = "id";
	private static final String SOURCE = "source";
	private static final String TTL = "ttl";
	private static final long DEFAULT_TTL = 14 * 24 * 3600 * 1000l;
	
	private transient Meter messageInMeter;
    private transient Meter messageSucMeter;
	private transient Meter messageFailMeter;

    public IndexDocumentProducer(Client client, RiverConfig riverConfig, KafkaConsumer kafkaConsumer, GangliaMetricsFactory metricsFactory) {
        super(client, riverConfig, kafkaConsumer , metricsFactory);
        this.messageInMeter = metricsFactory.getMeter(IndexDocumentProducer.class, "inNum");
        this.messageSucMeter = metricsFactory.getMeter(IndexDocumentProducer.class, "succNum");
        this.messageFailMeter = metricsFactory.getMeter(IndexDocumentProducer.class, "failNum");
    }

    /**
     * For the given messages creates index document requests and adds them to the bulk processor queue, for
     * processing later when the size of bulk actions is reached.
     *
     * @param messageSet given set of messages
     */
    public void addMessagesToBulkProcessor(final Set<MessageAndMetadata> messageSet) {

        for (MessageAndMetadata messageAndMetadata : messageSet) {
        	this.messageInMeter.mark();
            final byte[] messageBytes = (byte[]) messageAndMetadata.message();

            if (messageBytes == null || messageBytes.length == 0) return;

            try {
                // TODO - future improvement - support for protobuf messages

                String message = null;
                IndexRequest request = null;

                switch (riverConfig.getMessageType()) {
                    case STRING:
                        message = XContentFactory.jsonBuilder()
                                .startObject()
                                .field("value", new String(messageBytes, "UTF-8"))
                                .endObject()
                                .string();
                        request = Requests.indexRequest(riverConfig.getIndexName()).
                                type(riverConfig.getTypeName()).
                                id(UUID.randomUUID().toString()).
                                source(message);
                        break;
                    case JSON:
                    	final Map<String, Object> messageMap = reader.readValue(messageBytes);
                        String index = (String) messageMap.get(INDEX);
                        String type = (String) messageMap.get(TYPE);
                        String id = (String) messageMap.get(ID);
                        Map<String , Object> source = (Map<String , Object>)messageMap.get(SOURCE);
                        long ttl = getTTL(messageMap);
                        if(logger.isDebugEnabled()){
                        	logger.debug("addMessagesToBulkProcessor , index: [" + index + "] type: [" + type + "] id: [" + id + "] source: [" + source + "] ttl: [" + ttl + "]");
                        }
                        request = Requests.indexRequest(index).type(type).id(id).source(source).ttl(ttl);
                }

                bulkProcessor.add(request);
                this.messageSucMeter.mark();
            } catch (Exception ex) {
                ex.printStackTrace();
                this.messageFailMeter.mark();
            }
        }
    }
    
    private long getTTL(Map<String, Object> messageMap){
    	long ttl = DEFAULT_TTL;
    	try {
    		Object value = messageMap.get(TTL);
    		ttl = (value != null) ? Long.valueOf(value.toString()) : DEFAULT_TTL;
		}catch(Exception e){
			logger.error("ttl is invalid");
		}
    	return ttl;
    }
    
}
