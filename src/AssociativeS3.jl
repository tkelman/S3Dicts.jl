__precompile__()
module AssociativeS3

using AWS.S3

# immutable AssociativeS3 <: Associative end

immutable AssociativeS3{K,V} <: Associative{K,V}
    dir         ::String
end

function Base.setindex!{K,V}(h::AssociativeS3{K,V}, v::V, key::K)
    @assert ismatch(r"^s3://", h.dir)
    write("/tmp/$(key)", v)
    dstFileName = joinpath(h.dir, string(key))
    run(`aws s3 mv /tmp/$(key) $(dstFileName)`)
end

function Base.getindex{K,V}(h::AssociativeS3{K,V}, key::K)
    @assert ismatch(r"^s3://", h.dir)
    awsEnv = AWS.AWSEnv()
    bkt,key = S3.splits3( joinpath(h.dir, string(key)) )
    resp = S3.get_object(awsEnv, bkt, key)
    return resp.obj
end


function Base.haskey{K,V}( h::AssociativeS3{K,V}, key::K)
    list = AWS.S3.s3_list_objects( joinpath(h.dir, string(key)) )
    return !isempty( list[2] )
end

function delete!{K,V}( h::AssociativeS3{K,V}, key::K)
    @assert ismatch(r"^s3://", h.dir)
    fileName = joinpath( h.dir, string(key) )
    run(`aws s3 rm $(fileName)`)
end

end # end of module AssociativeS3
