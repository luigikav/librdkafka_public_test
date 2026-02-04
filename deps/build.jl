using EasyCurl

# Auto-download wrapper from GitHub releases or use local build
const libkafka = let
    pkg_dir = dirname(@__DIR__)
    lib_dir = joinpath(pkg_dir, "lib")
    lib_name = Sys.iswindows() ? "libkafka.dll" :
               Sys.isapple() ? "libkafka.dylib" : "libkafka.so"
    lib_path = joinpath(lib_dir, lib_name)

    # If not in lib/, try to download from GitHub release
    if !isfile(lib_path)
        platform = if Sys.islinux()
            "linux-x86_64"
        elseif Sys.isapple()
            Sys.ARCH === :aarch64 ? "macos-aarch64" : "macos-x86_64"
        elseif Sys.iswindows()
            "windows-x86_64"
        end

        # Detect Julia minor version (1.10, 1.11, etc)
        julia_version = "$(VERSION.major).$(VERSION.minor)"

        try
            mkpath(lib_dir)
            # Get version from Project.toml
            project_toml = read(joinpath(pkg_dir, "Project.toml"), String)
            version = match(r"version\s*=\s*\"([^\"]+)\"", project_toml)[1]

            url = "https://github.com/luigikav/librdkafka_public_test/releases/download/v$version/$platform-julia$julia_version.tar.gz"

            @info "Attempting to download pre-built binary from GitHub releases" url

            response = http_request("GET", url; read_timeout=30, connect_timeout=10)

            if http_status(response) == 200
                temp_file = tempname()
                write(temp_file, http_body(response))
                run(`tar -xzf $temp_file -C $lib_dir`)
                rm(temp_file, force=true)
                @info "Successfully downloaded and extracted pre-built binary"
            else
                @warn "Pre-built binary not found" status=http_status(response) version=version platform=platform julia=julia_version
            end
        catch e
            @warn "Failed to download binary" exception=(e, catch_backtrace())
        end
    end

    # Fallback to local build directories
    if !isfile(lib_path)
        candidates = (
            joinpath(pkg_dir, "deps", "src", "build", "lib", lib_name),
        )
        for path in candidates
            if isfile(path)
                lib_path = path
                break
            end
        end
    end

    if !isfile(lib_path)
        platform_name = if Sys.islinux()
            "Linux"
        elseif Sys.isapple()
            "macOS"
        elseif Sys.iswindows()
            "Windows"
        else
            "$(Sys.KERNEL)"
        end

        error("""
        Could not locate $lib_name for $platform_name.

        Pre-built binaries are currently only available for:
        - Linux x86_64 (Julia 1.10)

        To build from source on $platform_name:
        1. cd ~/.julia/packages/Librdkafka/*/
        2. cmake -S deps/src -B deps/src/build
        3. cmake --build deps/src/build

        Or install from dev mode:
        julia> using Pkg
        julia> Pkg.develop(url="https://github.com/maxfadson/Librdkafka.jl")
        julia> cd ~/.julia/dev/Librdkafka/src && cmake -S . -B build && cmake --build build
        """)
    end

    lib_path
end