using Base64

function KafkaConsumer(bootstrap_servers::AbstractString;
                       group_id::Union{Nothing,AbstractString}=nothing,
                       config::AbstractDict=Dict())
    props_id = _build_properties(bootstrap_servers; group_id=group_id, config=config)
    id = create_kafka_consumer(props_id)
    if id == 0
        _throw_error(:operation, :consumer_create,
            "Failed to create KafkaConsumer connection.",
            details=_details(:bootstrap_servers => bootstrap_servers, :group_id => group_id))
    end
    consumer = KafkaConsumer(id, String(bootstrap_servers),
                             isnothing(group_id) ? nothing : String(group_id), false,
                             :none, String[], Tuple{String,Int}[])
    finalizer(close, consumer)
    return consumer
end

function _ensure_consumer_open(c::KafkaConsumer, operation::Symbol, action::AbstractString)
    if c.closed
        _throw_error(:usage, operation,
            "KafkaConsumer is closed. Create a new consumer before $action.",
            details=_details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id))
    end
end

function _ensure_consumer_ready(c::KafkaConsumer, operation::Symbol, action::AbstractString)
    if c.subscription_mode == :none
        _throw_error(:usage, operation,
            "KafkaConsumer is not subscribed or assigned. Call subscribe(...) or assign(...) before $action.",
            details=_details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id))
    end
end

function Base.close(c::KafkaConsumer)
    c.closed && return nothing
    consumer_close(c.id)
    c.closed = true
    return nothing
end

function _topics_set(topics::AbstractVector{<:AbstractString})
    s = StdSet{CxxWrap.StdString}()
    for t in topics
        push!(s, CxxWrap.StdString(String(t)))
    end
    return s
end

subscribe(c::KafkaConsumer, topic::AbstractString) = subscribe(c, [topic])
function subscribe(c::KafkaConsumer, topics::AbstractVector{<:AbstractString})
    _ensure_consumer_open(c, :subscribe, "subscribing")
    isempty(topics) && _throw_error(:usage, :subscribe,
        "No topics provided to subscribe.",
        details=_details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id))
    topic_list = String.(topics)
    any(isempty, topic_list) && _throw_error(:usage, :subscribe,
        "Topic names must be non-empty.",
        details=_details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id,
                         :topics => topic_list))
    _with_kafka_error(:subscribe, "KafkaConsumer subscribe failed.",
        _details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id, :topics => topic_list)) do
        consumer_subscribe(c.id, _topics_set(topic_list))
    end
    c.subscription_mode = :subscribe
    c.subscribed_topics = topic_list
    empty!(c.assigned_partitions)
    return nothing
end

function _base64decode_bytes(s::AbstractString)
    return base64decode(Vector{UInt8}(codeunits(s)))
end

function _decode_base64_string(s::AbstractString; context::Symbol=:poll, details::Vector{String}=String[])
    isempty(s) && return ""
    bytes = try
        _base64decode_bytes(s)
    catch e
        _throw_error(:internal, context,
            "Kafka consumer returned malformed base64 payload.",
            details=vcat(details, _details(:payload_length => length(s)), "exception=$(e)"))
    end
    try
        return String(bytes)
    catch
        return String(Char.(bytes))
    end
end

decode_base64_bytes(s::AbstractString) = _base64decode_bytes(s)

function decode_base64_string(s::AbstractString)
    return String(_base64decode_bytes(s))
end

function _parse_int_field(field::AbstractString, field_name::Symbol, details::Vector{String})
    try
        return parse(Int, field)
    catch
        _throw_error(:internal, :poll,
            "Kafka consumer returned malformed record data.",
            details=vcat(details, _details(:field => field_name, :value => field)))
    end
end

function _parse_records(raw::AbstractString; details::Vector{String}=String[])
    records = ConsumerRecord[]
    i = firstindex(raw)
    n = lastindex(raw)
    while i <= n
        t1 = findnext('\t', raw, i)
        t1 === nothing && _throw_error(:internal, :poll,
            "Kafka consumer returned malformed record data.",
            details=vcat(details, _details(:raw_length => n, :position => i)))
        t2 = findnext('\t', raw, nextind(raw, t1))
        t2 === nothing && _throw_error(:internal, :poll,
            "Kafka consumer returned malformed record data.",
            details=vcat(details, _details(:raw_length => n, :position => i)))
        t3 = findnext('\t', raw, nextind(raw, t2))
        t3 === nothing && _throw_error(:internal, :poll,
            "Kafka consumer returned malformed record data.",
            details=vcat(details, _details(:raw_length => n, :position => i)))
        t4 = findnext('\t', raw, nextind(raw, t3))
        t4 === nothing && _throw_error(:internal, :poll,
            "Kafka consumer returned malformed record data.",
            details=vcat(details, _details(:raw_length => n, :position => i)))
        value_start = nextind(raw, t4)

        newline = findnext('\n', raw, value_start)
        last_tab = nothing
        while newline !== nothing
            last_tab = findprev('\t', raw, prevind(raw, newline))
            if last_tab !== nothing && last_tab >= value_start
                ts_start = nextind(raw, last_tab)
                ts_end = prevind(raw, newline)
                ts_str = ts_start > ts_end ? "" : raw[ts_start:ts_end]
                if !isempty(ts_str) && all(isdigit, ts_str)
                    break
                end
            end
            newline = findnext('\n', raw, nextind(raw, newline))
        end
        (newline === nothing || last_tab === nothing) && _throw_error(:internal, :poll,
            "Kafka consumer returned malformed record data.",
            details=vcat(details, _details(:raw_length => n, :position => i)))

        topic = raw[i:prevind(raw, t1)]
        partition = _parse_int_field(raw[nextind(raw, t1):prevind(raw, t2)], :partition, details)
        offset = _parse_int_field(raw[nextind(raw, t2):prevind(raw, t3)], :offset, details)
        key_raw = raw[nextind(raw, t3):prevind(raw, t4)]
        value_raw = (last_tab > value_start) ? raw[value_start:prevind(raw, last_tab)] : ""
        timestamp_ms = _parse_int_field(raw[nextind(raw, last_tab):prevind(raw, newline)], :timestamp_ms, details)
        key = _decode_base64_string(key_raw; details=details)
        value = _decode_base64_string(value_raw; details=details)
        push!(records, ConsumerRecord(topic, partition, offset, key, value, timestamp_ms))
        i = nextind(raw, newline)
    end
    return records
end

function _record_from_raw(record::AbstractString)
    parts = split(record, '\t', limit=6)
    length(parts) < 6 && _throw_error(:internal, :poll,
        "Kafka consumer returned malformed record data.",
        details=_details(:raw_parts => length(parts)))
    partition = _parse_int_field(parts[2], :partition, String[])
    offset = _parse_int_field(parts[3], :offset, String[])
    key = _decode_base64_string(parts[4])
    value = _decode_base64_string(parts[5])
    timestamp_ms = _parse_int_field(parts[6], :timestamp_ms, String[])
    return ConsumerRecord(parts[1], partition, offset, key, value, timestamp_ms)
end

function poll(c::KafkaConsumer; timeout_ms::Integer=1000)
    _ensure_consumer_open(c, :poll, "polling")
    _ensure_consumer_ready(c, :poll, "polling")
    timeout_ms < 0 && _throw_error(:usage, :poll, "timeout_ms must be non-negative.",
        details=_details(:timeout_ms => timeout_ms, :consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers))
    raw = try
        consumer_poll(c.id, Int(timeout_ms))
    catch e
        _throw_error(:operation, :poll, "Kafka consumer poll failed.",
            details=vcat(_details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id,
                                  :timeout_ms => timeout_ms),
                         "exception=$(e)"))
    end
    if startswith(raw, "ERROR: ")
        _throw_error(:usage, :poll, "KafkaConsumer handle not found. It may be closed or invalid.",
            details=_details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id))
    end
    isempty(raw) && return ConsumerRecord[]
    return _parse_records(raw; details=_details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id))
end

function poll_one(c::KafkaConsumer; timeout_ms::Integer=1000)
    records = poll(c; timeout_ms=timeout_ms)
    return isempty(records) ? nothing : first(records)
end

function commit(c::KafkaConsumer)
    _ensure_consumer_open(c, :commit, "committing offsets")
    _ensure_consumer_ready(c, :commit, "committing offsets")
    err = try
        consumer_commit_sync(c.id)
    catch e
        _throw_error(:operation, :commit, "Kafka consumer commit failed.",
            details=vcat(_details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id),
                         "exception=$(e)"))
    end
    if err == -1
        _throw_error(:usage, :commit,
            "KafkaConsumer handle not found. It may be closed or invalid.",
            details=_details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id))
    elseif err != 0
        _throw_error(:operation, :commit, "Offset commit failed.",
            details=_details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id,
                             :error_code => err))
    end
    return nothing
end

function assign(c::KafkaConsumer, topic::AbstractString, partition::Integer; offset::Integer=RD_KAFKA_OFFSET_INVALID)
    _ensure_consumer_open(c, :assign, "assigning partitions")
    topic_str = String(topic)
    isempty(topic_str) && _throw_error(:usage, :assign, "Topic is empty. Provide a topic name.",
        details=_details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id))
    _with_kafka_error(:assign, "KafkaConsumer assign failed.",
        _details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id,
                 :topic => topic_str, :partition => partition, :offset => offset)) do
        consumer_assign(c.id, topic_str, Int(partition), Int(offset))
    end
    c.subscription_mode = :assign
    c.assigned_partitions = [(topic_str, Int(partition))]
    empty!(c.subscribed_topics)
    return nothing
end

function seek_to_beginning(c::KafkaConsumer, topic::AbstractString, partition::Integer)
    _ensure_consumer_open(c, :seek_to_beginning, "seeking to beginning")
    _ensure_consumer_ready(c, :seek_to_beginning, "seeking to beginning")
    topic_str = String(topic)
    _with_kafka_error(:seek_to_beginning, "KafkaConsumer seek_to_beginning failed.",
        _details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id,
                 :topic => topic_str, :partition => partition)) do
        consumer_seek_to_beginning(c.id, topic_str, Int(partition))
    end
    return nothing
end

function commit_record(c::KafkaConsumer, topic::AbstractString, partition::Integer, offset::Integer)
    _ensure_consumer_open(c, :commit_record, "committing record offsets")
    _ensure_consumer_ready(c, :commit_record, "committing record offsets")
    topic_str = String(topic)
    _with_kafka_error(:commit_record, "KafkaConsumer commit_record failed.",
        _details(:consumer_id => c.id, :bootstrap_servers => c.bootstrap_servers, :group_id => c.group_id,
                 :topic => topic_str, :partition => partition, :offset => offset)) do
        consumer_commit_record(c.id, topic_str, Int(partition), Int(offset))
    end
    return nothing
end
