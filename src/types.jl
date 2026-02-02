using Dates

mutable struct KafkaProducer
    id::Int
    bootstrap_servers::String
    closed::Bool
end

mutable struct KafkaConsumer
    id::Int
    bootstrap_servers::String
    group_id::Union{Nothing,String}
    closed::Bool
    subscription_mode::Symbol
    subscribed_topics::Vector{String}
    assigned_partitions::Vector{Tuple{String,Int}}
end

struct ConsumerRecord
    topic::String
    partition::Int
    offset::Int
    key::String
    value::String
    timestamp_ms::Int
end

function Base.show(io::IO, p::KafkaProducer)
    state = p.closed ? "closed" : "open"
    print(io, "KafkaProducer(", p.bootstrap_servers, ", id=", p.id, ", ", state, ")")
end

function Base.show(io::IO, c::KafkaConsumer)
    state = c.closed ? "closed" : "open"
    group = isnothing(c.group_id) ? "no-group" : c.group_id
    print(io, "KafkaConsumer(", c.bootstrap_servers, ", group=", group, ", id=", c.id, ", ", state, ")")
end

function Base.show(io::IO, r::ConsumerRecord)
    ts = Dates.unix2datetime(r.timestamp_ms / 1000)
    print(io, "Message(", r.topic, ":", r.partition, " @", r.offset,", key=\"", r.key, "\", value=\"", r.value, "\", ts=", ts, ")")
end
