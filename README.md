# Librdkafka.jl

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/bhftbootcamp/Librdkafka.jl)
[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://bhftbootcamp.github.io/Librdkafka.jl/stable/)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://bhftbootcamp.github.io/Librdkafka.jl/dev/)
[![Build Status](https://github.com/bhftbootcamp/Librdkafka.jl/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/bhftbootcamp/Librdkafka.jl/actions/workflows/ci.yml?query=branch%3Amaster)
[![Registry](https://img.shields.io/badge/registry-Green-green)](https://github.com/bhftbootcamp/Green)

Julia wrapper for librdkafka for producing and consuming Apache Kafka messages from Julia.

## Installation

If you haven't installed our local registry yet, do that first:

```
] registry add https://github.com/bhftbootcamp/Green.git
```

To install Librdkafka, simply use the Julia package manager:

```
] add Librdkafka
```

## Usage

### Producer

```julia
using Librdkafka

topic = "julia-demo"

p = KafkaProducer("localhost:9092")

try
    i = 1
    while true
        msg = "hello #$i from julia"
        produce(p, topic, 0, "key", msg)
        @info "Sent" msg
        i += 1
        sleep(1)
    end
finally
    close(p) # unreachable here, Ctrl+C to stop
end
```

### Consumer

```julia
using Librdkafka

topic = "julia-demo"

c = KafkaConsumer(
    "localhost:9092";
    group_id = "julia-demo-consumer",
    config = Dict(
        AUTO_OFFSET_RESET => "earliest",
        ENABLE_AUTO_COMMIT => "false",
    ),
)

subscribe!(c, [topic])

try
    while true
        for r in poll(c; timeout_ms=1000)
            @info "Got" key=r.key value=r.value offset=r.offset
            commit_record(c, r)
        end
    end
finally
    close(c) # unreachable here, Ctrl+C to stop
end
```

## Useful Links

- [librdkafka](https://github.com/confluentinc/librdkafka) – Official library repository.
- [librdkafka_jll](https://github.com/JuliaBinaryWrappers/librdkafka_jll.jl) – Julia binary wrapper for librdkafka.

## Contributing

Contributions to Librdkafka are welcome! If you encounter a bug, have a feature request, or would like to contribute code, please open an issue or a pull request on GitHub.
