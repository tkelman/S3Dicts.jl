#__precompile__()
module S3Dicts

using JSON
using AWSCore
using AWSS3
using Retry
using Libz 
using Memoize
import BigArrays: NoSuchKeyException 

const NEUROGLANCER_CONFIG_FILENAME = "info"
const AWS_CREDENTIAL = AWSCore.aws_config()
const IS_GZIP = true

const DEFAULT_METADATA = Dict{String, String}(
            "Content-Type"      => "binary/octet-stream", 
            "Content-Encoding"  => "gzip")

# map datatype of python to Julia
const DATATYPE_MAP = Dict{String, String}(
    "uint8"     => "UInt8",
    "uint16"    => "UInt16",
    "uint32"    => "UInt32",
    "uint64"    => "UInt64",
    "float32"   => "Float32",
    "float64"   => "Float64"
)

export S3Dict

immutable S3Dict <: Associative
    dir         ::String
    configDict  ::Dict{Symbol,Any}
end

function get_config_dict( h::S3Dict )
    h.configDict
end

"""
split a s3 path to bucket name and key
"""
function splits3(path::AbstractString)
    path = replace(path, "s3://", "")
    bkt, key = split(path, "/", limit = 2)
    return String(bkt), String(key)
end

@memoize function get_config_dict( dir::String )
    configDict=Dict{Symbol,Any}()
    try
        finfo = joinpath( dirname(strip(dir, '/')), NEUROGLANCER_CONFIG_FILENAME )
        @show finfo
        bkt,key = splits3( finfo )
        data = s3_get( AWS_CREDENTIAL, bkt, key )
        if isa(data, Vector{UInt8})
            data = String(data)
        end
        @show data
        configDict = JSON.parse( data, dicttype=Dict{Symbol, Any} )
    catch e
        warn("this is not a neuroglancer formatted dict, did not find the info file: $e")
    end
    @show configDict
    
    if haskey(configDict, :data_type)
        # a neuroglancer format
        ## postprocessing for neuroglancer format
        configDict[:dataType] = DATATYPE_MAP[ configDict[:data_type] ]

        for d in configDict[:scales]
            if configDict[:num_channels] == 1
                configDict[:chunkSize] = d[:chunk_sizes][1]
            else
                configDict[:chunkSize] = [d[:chunk_sizes][1]..., configDict[:num_channels]]
            end
            configDict[:coding]     = d[:encoding]
            configDict[:size]  = d[:size]
            configDict[:offset]     = d[:voxel_offset]
        end
    end
    return configDict
end

"""
    S3Dict( dir::String )
construct S3Dict from a directory path of s3
"""
function S3Dict( dir::String, 
                metadata::Dict{String,String}=DEFAULT_METADATA )
    configDict = get_config_dict(dir)
    configDict[:metadata] = metadata
    S3Dict(dir, configDict)
end

function Base.setindex!(h::S3Dict, v::Array, key::AbstractString)
    @assert ismatch(r"^s3://", h.dir)
    dstFileName = joinpath(h.dir, key)
    bkt, key = splits3(dstFileName)
    @repeat 4 try 
        v = Libz.deflate(reinterpret(UInt8, v[:]))
        resp = s3_put(AWS_CREDENTIAL, bkt, key, v, 
                      metadata=h.configDict[:metadata])
    catch e
        println("catch error while saving in BigArrays: $e")
        @show typeof(e)
        @delay_retry if true end
    end
end

function Base.getindex(h::S3Dict, key::AbstractString)
    @assert ismatch(r"^s3://", h.dir)
    bucket,key = splits3( joinpath(h.dir, key) )
    
	@repeat 2 try
        data = AWSS3.s3(AWS_CREDENTIAL, "GET", bucket; path = key, version="")
        return Libz.inflate(data)
    catch e
        @show e 
        @show typeof(e)
        @delay_retry if e.code != "NoSuchKey" end
        if e.code == "NoSuchKey"
            throw( NoSuchKeyException() )
        end 
    end
end

function Base.delete!( h::S3Dict, key::AbstractString)
    @assert ismatch(r"^s3://", h.dir)
    fileName = joinpath( h.dir, key )
    bkt, key = splits3( fileName ) 
    s3_delete(AWS_CREDENTIAL, bkt, key)
end

function Base.keys( h::S3Dict )
    bkt, key = splits3( h.dir )
    keyList = s3_list_objects(AWS_CREDENTIAL, bkt, key)
    return keyList
end

function Base.values(h::S3Dict)
    error("normally values are too large to get them all to RAM")
end

function Base.haskey(h::S3Dict, key::String)
    bkt, dir = splits3( h.dir )
    lst = s3_list_objects(AWS_CREDENTIAL, bkt, joinpath(dir, key))
    return !isempty(lst)
end 

end # end of module S3Dicts
