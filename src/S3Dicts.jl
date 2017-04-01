__precompile__()
module S3Dicts
#using AWS, AWS.S3
using JSON
using Memoize

import BigArrays: get_config_dict

#const awsEnv = AWS.AWSEnv();
const CONFIG_FILE_NAME = "config.json"
const NEUROGLANCER_CONFIG_FILENAME = "info"

# map datatype of python to Julia
const DATATYPE_MAP = Dict{String, String}(
    "uint8"     => "UInt8",
    "uint16"    => "UInt16",
    "uint32"    => "UInt32",
    "float32"   => "Float32",
    "float64"   => "Float64"
)

export S3Dict, get_config_dict

immutable S3Dict <: Associative
    dir         ::String
    configDict  ::Dict{Symbol,Any}
    function S3Dict( dir::AbstractString, configDict::Dict{Symbol, Any} )
        @assert ismatch(r"^s3://", dir)
        new(dir, configDict)
    end
end

function get_config_dict( h::S3Dict )
    h.configDict
end

@memoize function get_config_dict( dir::String )
    configDict=Dict{Symbol,Any}()
    try
        #bkt,key = S3.splits3( joinpath(dir, CONFIG_FILE_NAME) )
        #resp = S3.get_object(awsEnv, bkt, key)
        #configDict = JSON.parse( takebuf_string(IOBuffer(resp.obj)), dicttype=Dict{Symbol, Any} )
        tempFile = tempname()
        run(`aws s3 cp $(joinpath(dir, CONFIG_FILE_NAME)) $tempFile`)
        configDict = JSON.parse(readstring(tempFile), dicttype=Dict{Symbol, Any})
        rm(tempFile)
    catch e
        println("not ND format: $e")
    end
    try
        finfo = joinpath( dirname(strip(dir, '/')), NEUROGLANCER_CONFIG_FILENAME )
        #bkt,key = S3.splits3( finfo )
        #resp = S3.get_object(awsEnv, bkt, key)
        #configDict = JSON.parse( takebuf_string(IOBuffer(resp.obj)), dicttype=Dict{Symbol, Any} )
        tempFile = tempname()
        run(`aws s3 cp $finfo $tempFile`)
        configDict = JSON.parse(readstring(tempFile), dicttype=Dict{Symbol, Any})
        rm(tempFile)
    catch e
        println("not a neuroglancer format: $e")
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
            configDict[:totalSize]  = d[:size]
            configDict[:offset]     = d[:voxel_offset]
        end
    end
    return configDict
end

"""
    S3Dict( dir::String )
construct S3Dict from a directory path of s3
"""
function S3Dict( dir::String )
    configDict = get_config_dict(dir)
    S3Dict(dir, configDict)
end

function Base.setindex!(h::S3Dict, v, key::AbstractString)
    @assert ismatch(r"^s3://", h.dir)
    tempFile = tempname()
    write(tempFile, v)
    dstFileName = joinpath(h.dir, key)
    run(`aws s3 mv $(tempFile) $(dstFileName)`)
end

# function Base.getindex(h::S3Dict, key::AbstractString)
#     @assert ismatch(r"^s3://", h.dir)
#     bkt,key = S3.splits3( joinpath(h.dir, key) )
#     resp = S3.get_object(awsEnv, bkt, key)
#     return resp.obj
# end

"""
    Base.getindex(h::S3Dict, key::AbstractString)
read binary file using aws command line
"""
function Base.getindex(h::S3Dict, key::AbstractString)
    @assert ismatch(r"^s3://", h.dir)
    tempFile = tempname()
    srcFileName = joinpath(h.dir, key)
    try
        run(`aws s3 cp $srcFileName $tempFile`)
    catch e
        #if isa(e, ErrorException)
            # return the error
         #   return e
        #end
        println("download s3 file error: $e")
        return e
    end
    # the file exist and downloaded, read and remove it.
    ret = read(tempFile)
    rm(tempFile)
    return ret
end

function Base.haskey( h::S3Dict, key::AbstractString)
    list = AWS.S3.s3_list_objects( joinpath(h.dir, key) )
    return !isempty( list[2] )
end

function Base.delete!( h::S3Dict, key::AbstractString)
    @assert ismatch(r"^s3://", h.dir)
    fileName = joinpath( h.dir, key )
    run(`aws s3 rm $(fileName)`)
end

function Base.keys( h::S3Dict )
    buket, keyList = AWS.S3.list_objects(awsEnv, h.dir)
    for i in eachindex(keyList)
        keyList[i] = basename( keyList[i] )
        if CONFIG_FILE_NAME == basename(keyList[i])
            deleteat!(keyList, i)
        end
    end
    return keyList
end

function Base.values(h::S3Dict)
    error("normally values are too large to get them all to RAM")
end


end # end of module S3Dicts
