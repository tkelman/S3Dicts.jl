S3Dicts.jl
==========
[![Build Status](https://travis-ci.org/seung-lab/S3Dicts.jl.svg?branch=master)](https://travis-ci.org/seung-lab/S3Dicts.jl)

use AWS S3 as a Dict. 

The value is saved and read with binary format, so you may need do some decoding after you get the value.

# Installation

    Pkg.clone("https://github.com/seung-lab/S3Dicts.jl.git")

# Usage 
```
ing S3Dicts

as3  = S3Dict("s3://bucketname/keyname/")

a = rand(UInt8, 50)

as3["test"] = a

b = as3["test"]

b = reinterpret(UInt8, b)

@assert all(a .== b)

info("delete the file in s3")
delete!(as3, "test")
```

