function set_log_level!(p::KafkaProducer, level::Integer)
    _ensure_producer_open(p, :producer_set_log_level, "changing log level")
    ok = _with_kafka_error(:producer_set_log_level, "KafkaProducer set_log_level failed.",
        _details(:producer_id => p.id, :bootstrap_servers => p.bootstrap_servers, :level => level)) do
        producer_set_log_level(p.id, Int(level))
    end
    if !ok
        _throw_error(:usage, :producer_set_log_level,
            "KafkaProducer handle not found. It may be closed or invalid.",
            details=_details(:producer_id => p.id, :bootstrap_servers => p.bootstrap_servers, :level => level))
    end
    return p
end

function set_log_level!(c::KafkaConsumer, level::Integer)
    _ensure_consumer_open(c, :consumer_set_log_level, "changing log level")
    ok = _with_kafka_error(:consumer_set_log_level, "KafkaConsumer set_log_level failed.",
        _details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id, :level => level)) do
        consumer_set_log_level(c.id, Int(level))
    end
    if !ok
        _throw_error(:usage, :consumer_set_log_level,
            "KafkaConsumer handle not found. It may be closed or invalid.",
            details=_details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id, :level => level))
    end
    return c
end

disable_logs!() = (logging_disable(); nothing)
set_log_format!(format::AbstractString=DEFAULT_LOG_FORMAT) = (logging_set_format(String(format)); nothing)
enable_default_logs!() = (logging_enable_default(); nothing)
