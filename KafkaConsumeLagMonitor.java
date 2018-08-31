#from https://www.aliyun.com/jiaocheng/775267.html

#注意:kafka-clients版本需要0.10.1.0以上,因为调用了新增接口endOffsets;
#logsize通过consumer的endOffsets接口获得;offset通过consumer的committed接口获得;

public class KafkaConsumeLagMonitor {
    public static Properties getConsumeProperties(String groupID, String bootstrap_server) {
        Properties props = new Properties();
        props.put("group.id", groupID);
        props.put("bootstrap.servers", bootstrap_server);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
    public static void main(String[] args) {
        String bootstrap_server = "localhost:9092";
        String groupID = "Consumer7";
        String topic ="kafkatopic2";
        Map<Integer, Long> endOffsetMap = new HashMap();
        Map<Integer, Long> commitOffsetMap = new HashMap ();
        Properties consumeProps = getConsumeProperties(groupID, bootstrap_server);
        System.out.println("consumer properties:" + consumeProps);
//查询topic partitions
        KafkaConsumer consumer = new KafkaConsumer (consumeProps);
        List<TopicPartition> topicPartitions = new ArrayList();
        List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);
        for (PartitionInfo partitionInfo : partitionsFor) {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitions.add(topicPartition);
        }
//查询log size
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
        for (TopicPartition partitionInfo : endOffsets.keySet()) {
            endOffsetMap.put(partitionInfo.partition(), endOffsets.get(partitionInfo));
        }
        for (Integer partitionId : endOffsetMap.keySet()) {
            System.out.println(String.format("at %s, topic:%s, partition:%s, logSize:%s", System.currentTimeMillis(), topic, partitionId, endOffsetMap.get(partitionId)));
        }
//查询消费offset
        for (TopicPartition topicAndPartition : topicPartitions) {
            OffsetAndMetadata committed = consumer.committed(topicAndPartition);
            commitOffsetMap.put(topicAndPartition.partition(), committed.offset());
        }
//累加lag
        long lagSum = 0l;
        if (endOffsetMap.size() == commitOffsetMap.size()) {
            for (Integer partition : endOffsetMap.keySet()) {
                long endOffSet = endOffsetMap.get(partition);
                long commitOffSet = commitOffsetMap.get(partition);
                long diffOffset = endOffSet - commitOffSet;
                lagSum += diffOffset;
                System.out.println("Topic:" + topic + ", groupID:" + groupID + ", partition:" + partition + ", endOffset:" + endOffSet + ", commitOffset:" + commitOffSet + ", diffOffset:" + diffOffset);
            }
            System.out.println("Topic:" + topic + ", groupID:" + groupID + ", LAG:" + lagSum);
        } else {
            System.out.println("this topic partitions lost");
        }
        consumer.close();
    }
}
