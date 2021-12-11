from MARC_MR import Count
from MARC_MR import output_process
import json

count = Count()
with count.make_runner() as runner:
    runner.run()
    x = count.parse_output(runner.cat_output())
    output_json = output_process(x)
    print(output_json)
    with open('output_emr.json', 'w') as outfile:
        json.dump(output_json, outfile)
  
