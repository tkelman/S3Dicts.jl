__precompile__()
module S3Dicts
using AWS, AWS.S3

export S3Dict

# immutable S3Dict <: Associative end

immutable S3Dict <: Associative
    dir         ::String
end


function Base.setindex!(h::S3Dict, v, key)
    @assert ismatch(r"^s3://", h.dir)
    write("/tmp/$(key)", v)
    dstFileName = joinpath(h.dir, string(key))
    run(`aws s3 mv /tmp/$(key) $(dstFileName)`)
end

function Base.getindex(h::S3Dict, key)
    @assert ismatch(r"^s3://", h.dir)
    awsEnv = AWS.AWSEnv()
    bkt,key = S3.splits3( joinpath(h.dir, string(key)) )
    resp = S3.get_object(awsEnv, bkt, key)
    return resp.obj
end


function Base.haskey( h::S3Dict, key)
    list = AWS.S3.s3_list_objects( joinpath(h.dir, string(key)) )
    return !isempty( list[2] )
end

function Base.delete!( h::S3Dict, key)
    @assert ismatch(r"^s3://", h.dir)
    fileName = joinpath( h.dir, string(key) )
    run(`aws s3 rm $(fileName)`)
end

end # end of module S3Dicts
