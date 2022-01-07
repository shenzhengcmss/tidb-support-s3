# tidb-s3

编译：

cd $GOPATH/src

git clone https://github.com/shenzhengcmss/tidb-support-s3.git

mv tidb-support-s3 tidb-s3

cd tidb-s3/tidb/tidb-server

go build -o tidb-server main.go

运行：

PD1：100.73.10.32

PD2：100.73.10.33

PD3：100.73.10.42

./tidb-server --store=tikv --path=100.73.10.32:4379,100.73.10.33:4379,100.73.10.42:4379 -P 4000 



