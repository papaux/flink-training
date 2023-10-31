# Flink Tutorial

## Flink setup

1. Download Flink from:

2. Extract tar

```bash
tar -xzf ~/Downloads/flink-1.18.0-bin-scala_2.12.tgz
```

3. Run flink
```bash
./bin/start-cluster.sh
```

4. Run the application
Run:

```bash
 ~/workspace/sandbox/flink/flink-1.18.0/bin/flink run build/libs/flink-tuto-0.1-SNAPSHOT-all.jar --input file:///data/john/workspace/sandbox/flink-tuto/wc.txt --output file:///data/john/workspace/sandbox/flink-tuto/output.txt
```
