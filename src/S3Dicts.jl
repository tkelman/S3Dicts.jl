__precompile__()
module S3Dicts
using AWS, AWS.S3
using JSON

import BigArrays: get_config_dict

const awsEnv = AWS.AWSEnv();
const CONFIG_FILE_NAME = "config.json"

export S3Dict, get_config_dict

# immutable S3Dict <: Associative end

immutable S3Dict <: Associative
    dir         ::String
    function S3Dict( dir::AbstractString )
        @assert ismatch(r"^s3://", dir)
        new(dir)
    end
end

function get_config_dict( h::S3Dict )
    bkt,key = S3.splits3( joinpath(h.dir, CONFIG_FILE_NAME) )
    resp = S3.get_object(awsEnv, bkt, key)
    JSON.parse( takebuf_string(IOBuffer(resp.obj)), dicttype=Dict{Symbol, Any} )
end

function Base.setindex!(h::S3Dict, v, key::AbstractString)
    @assert ismatch(r"^s3://", h.dir)
    tempFile = tempname()
    write(tempFile, v)
    dstFileName = joinpath(h.dir, key)
    run(`aws s3 mv $(tempFile) $(dstFileName)`)
end

function Base.getindex(h::S3Dict, key::AbstractString)
    @assert ismatch(r"^s3://", h.dir)
    bkt,key = S3.splits3( joinpath(h.dir, key) )
    resp = S3.get_object(awsEnv, bkt, key)
    return resp.obj
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
