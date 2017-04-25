using S3Dicts

as3  = S3Dict("s3://seunglab/jpwu/")

a = rand(UInt8, 50)

as3["test"] = a

b = as3["test"]

b = reinterpret(UInt8, b)

@assert all(a .== b)

info("delete the file in s3")
delete!(as3, "test")


info("test fetching object that is not exist")

try 
    delete!(as3, "test")
end

try 
    resp = as3["test"]
catch e
    @show typeof(e)
    println("catched error from a non-exist key: $e")
end
