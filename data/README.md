This directory contains:

- `Spider_Filtered_Data.csv`: contains queries from SPIDER dataset that are filtered accrording ot DB names.
- `Spider_Filtered_Data_generic.csv`:We then manually thorugh the file ``Spider_Filtered_Data.csv`` and (i) remove queries that are not generic, such as those containing Singer_ID, and (ii) add query type as a column indicating wether it contains a select operation (S), a projection (P), a where condition (W), an aggregation (A), a nested query (C), and a join (J).
- `Final_Queries.csv`: The final 46 queries used for the experiments with columns indicating the query type.
- `question_maps.json`: Manual mapping from duck operations to natural language.
- `fixed_spider.zip`: Spider DBs
- `spider_files/`: files used by Spider data
- `results/`: Experiment results