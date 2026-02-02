abstract type KafkaClientError <: Exception end

struct KafkaError <: KafkaClientError
    kind::Symbol
    operation::Symbol
    message::String
    details::Vector{String}
end

function Base.showerror(io::IO, e::KafkaError)
    print(io, "Kafka ", e.kind, " error during ", e.operation, ": ", e.message)
    if !isempty(e.details)
        print(io, " (", join(e.details, "; "), ")")
    end
end

_detail_value(v::AbstractVector) = "[" * join(string.(v), ", ") * "]"
_detail_value(v) = string(v)

function _details(pairs::Pair{Symbol, <:Any}...)
    out = String[]
    for (k, v) in pairs
        v === nothing && continue
        s = _detail_value(v)
        isempty(s) && continue
        push!(out, string(k, "=", s))
    end
    return out
end

function _throw_error(kind::Symbol, operation::Symbol, message::AbstractString; details::Vector{String}=String[])
    throw(KafkaError(kind, operation, String(message), details))
end

function _with_kafka_error(operation::Symbol, message::AbstractString, details::Vector{String}, f::Function)
    try
        return f()
    catch e
        _throw_error(:operation, operation, message,
            details=vcat(details, "exception=$(e)"))
    end
end

function _with_kafka_error(f::Function, operation::Symbol, message::AbstractString, details::Vector{String})
    return _with_kafka_error(operation, message, details, f)
end
