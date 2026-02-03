using Librdkafka
function simple_producer()
    # disable_logs!()
    p = KafkaProducer("te-test-vm-app-01.mgt:9092", config=Dict("log_level" => "7"))

    partition = 0
    
    produce(p, "quickstart-events", partition, "message key", "fasfsa")
    println("successfully produced")
    close(p)
end
