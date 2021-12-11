from MARC_MR import Count
from MARC_MR import output_process
import json
import pydoop.hdfs as hdfs
count = Count(args=["-r", "hadoop", "hdfs://localhost:9000/test/dataset_ja_test.json"
,"hdfs://localhost:9000/test/dataset_zh_test.json","hdfs://localhost:9000/test/dataset_es_test.json"
,"hdfs://localhost:9000/test/dataset_de_test.json","hdfs://localhost:9000/test/dataset_en_test.json"
,"hdfs://localhost:9000/test/dataset_fr_test.json",
                    "--hadoop-streaming-jar",
                    "/home/pablo/Downloads/hadoop-3.2.2/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar",
                    "--hadoop-bin", "/home/pablo/Downloads/hadoop-3.2.2/bin/hadoop"])
with count.make_runner() as runner:
    runner.run()
    x = count.parse_output(runner.cat_output())
    output_json = output_process(x)
    print(output_json)
    with open('output.json', 'w') as outfile:
        json.dump(output_json, outfile)
    from_path = "/home/pablo/Desktop/Entrega/Local/output.json"
    to_path = "hdfs://localhost:9000/"
    hdfs.put(from_path, to_path)
