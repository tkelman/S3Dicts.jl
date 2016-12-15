using S3Dicts

as3  = S3Dict("s3://seunglab/jpwu/")

a = rand(UInt8, 50)

as3["test"] = a

b = as3["test"]

b = reinterpret(UInt8, b)

@assert all(a .== b)
