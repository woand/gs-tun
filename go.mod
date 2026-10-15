module gscript-tunnel

go 1.24

require (
	github.com/cs8425/smux v2.0.0+incompatible
	github.com/golang/snappy v0.0.1
	github.com/pkg/errors v0.8.1
	github.com/urfave/cli v1.22.17
	github.com/xtaci/kcp-go v5.1.3+incompatible
	golang.org/x/crypto v0.0.0-20190320223903-b7391e95e576
)

replace github.com/xtaci/kcp-go => ./third_party/kcp-go

require (
	github.com/cpuguy83/go-md2man/v2 v2.0.7 // indirect
	github.com/klauspost/cpuid v1.2.0 // indirect
	github.com/klauspost/reedsolomon v1.9.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/templexxx/cpufeat v0.0.0-20180724012125-cef66df7f161 // indirect
	github.com/templexxx/xor v0.0.0-20181023030647-4e92f724b73b // indirect
	github.com/tjfoc/gmsm v1.0.1 // indirect
	golang.org/x/net v0.0.0-20190322120337-addf6b3196f6 // indirect
)
