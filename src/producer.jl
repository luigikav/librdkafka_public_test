function KafkaProducer(bootstrap_servers::AbstractString; config::AbstractDict=Dict())
    props_id = _build_properties(bootstrap_servers; config=config)
    id = create_kafka_producer(props_id)
    if id == 0
        _throw_error(:operation, :producer_create,
            "Failed to create KafkaProducer connection.",
            details=_details(:bootstrap_servers => bootstrap_servers))
    end
    producer = KafkaProducer(id, String(bootstrap_servers), false)
    finalizer(close, producer)
    return producer
end

function _ensure_producer_open(p::KafkaProducer, operation::Symbol, action::AbstractString)
    if p.closed
        _throw_error(:usage, operation,
            "KafkaProducer is closed. Create a new producer before $action.",
            details=_details(:producer_id => p.id, :bootstrap_servers => p.bootstrap_servers))
    end
end

function Base.close(p::KafkaProducer)
    p.closed && return nothing
    producer_close(p.id)
    p.closed = true
    return nothing
end

function _bytes_length(s::AbstractString)
    return ncodeunits(String(s))
end

function _handle_produce_error(err::AbstractString, operation::Symbol, details::Vector{String})
    if occursin("Producer not found", err) || occursin("handle not found", err)
        _throw_error(:usage, operation,
            "KafkaProducer handle not found. It may be closed or invalid.",
            details=details)
    end
    _throw_error(:operation, operation,
        "Kafka producer failed to send message: $(err)",
        details=vcat(details, "kafka_error=$(err)"))
end

function produce(p::KafkaProducer, topic::AbstractString, partition::Integer, key::AbstractString, value::AbstractString)
    _ensure_producer_open(p, :produce, "producing messages")
    topic_str = String(topic)
    isempty(topic_str) && _throw_error(:usage, :produce, "Topic is empty. Provide a topic name.",
        details=_details(:producer_id => p.id, :bootstrap_servers => p.bootstrap_servers))
    key_str = String(key)
    value_str = String(value)
    details = _details(:producer_id => p.id, :bootstrap_servers => p.bootstrap_servers,
                       :topic => topic_str, :partition => partition,
                       :key_bytes => _bytes_length(key_str), :value_bytes => _bytes_length(value_str))
    err = _with_kafka_error(:produce, "Kafka producer send failed.", details) do
        nb_produce(Int(p.id), topic_str, Int(partition), key_str, value_str)
    end
    if !isempty(err)
        _handle_produce_error(err, :produce, details)
    end
    return nothing
end

function produce_binary(p::KafkaProducer, topic::AbstractString, partition::Integer, key::AbstractString, value::Vector{UInt8})
    _ensure_producer_open(p, :produce_binary, "producing messages")
    topic_str = String(topic)
    isempty(topic_str) && _throw_error(:usage, :produce_binary, "Topic is empty. Provide a topic name.",
        details=_details(:producer_id => p.id, :bootstrap_servers => p.bootstrap_servers))
    key_str = String(key)
    details = _details(:producer_id => p.id, :bootstrap_servers => p.bootstrap_servers,
                       :topic => topic_str, :partition => partition,
                       :key_bytes => _bytes_length(key_str), :value_bytes => length(value))
    err = _with_kafka_error(:produce_binary, "Kafka producer send failed.", details) do
        nb_produce_binary(Int(p.id), topic_str, Int(partition), key_str, value)
    end
    if !isempty(err)
        _handle_produce_error(err, :produce_binary, details)
    end
    return nothing
end
