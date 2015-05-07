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

import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.ChannelBufferBytesReference;
import org.elasticsearch.common.netty.buffer.ByteBufferBackedChannelBuffer;

import com.codahale.metrics.Meter;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Producer to executed raw messages as bytes array directly using Bulk API.
 *
 * @author Mariam Hakobyan
 */
public class RawMessageProducer extends ElasticSearchProducer {
	
	private transient Meter messageInMeter;
    private transient Meter messageSucMeter;
	private transient Meter messageFailMeter;

    public RawMessageProducer(Client client, RiverConfig riverConfig, KafkaConsumer kafkaConsumer, GangliaMetricsFactory metricsFactory) {
        super(client, riverConfig, kafkaConsumer, metricsFactory);
        this.messageInMeter = metricsFactory.getMeter(RawMessageProducer.class, "inNum");
        this.messageSucMeter = metricsFactory.getMeter(RawMessageProducer.class, "succNum");
        this.messageFailMeter = metricsFactory.getMeter(RawMessageProducer.class, "failNum");
    }

    /**
     * Adds the given raw messages to the bulk processor queue, for processing later
     * when the size of bulk actions is reached.
     *
     * @param messageSet given set of messages
     */
    public void addMessagesToBulkProcessor(final Set<MessageAndMetadata> messageSet) {
        for (MessageAndMetadata messageAndMetadata : messageSet) {
        	this.messageInMeter.mark();
            final byte[] messageBytes = (byte[]) messageAndMetadata.message();
            try {
                ByteBuffer byteBuffer = ByteBuffer.wrap(messageBytes);
                bulkProcessor.add(
                        new ChannelBufferBytesReference(new ByteBufferBackedChannelBuffer(byteBuffer)),
                        false,
                        riverConfig.getIndexName(),
                        riverConfig.getTypeName()
                );
                this.messageSucMeter.mark();
            } catch (Exception e) {
                e.printStackTrace();
                this.messageFailMeter.mark();
            }
        }
    }
}
