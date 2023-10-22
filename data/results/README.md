This directory contains the following:
- `cardinality`: results shown in Table 1.
- `chat_gpt/`: results for ChatGPT
- `instruct_gpt/`: results for InstructGPT
- `flan_ul2/`: results for Flan UL2
- `tk_instruct/`: results for Tk-Instruct


For `ChatGPT` and `InstructGPT`, we manually label the answers in `Chat_GPT_label.csv` and `Inst_GPT_label.csv`.
Evaluation was done as following:
- If a list is expected, e.g. list of cities whose elevation is between A and B, then we use:
    - Precision
    - Recall

- Otherwise, e.g. length of a river, we use:
    - Exact Match (EM)
    - Fuzzy Exact Match (Fuzzy EM): This is when the answer given by the model is correct but in a non-structured format. For example, the answer given by the model is __Massachusetts is a state located in the northeastern region of the United States.__ while the answer returned from the DB is __northeastern__.

Both files contain the following columns:

- Database: name of database used
- Query: query

- Single Question Answer: answer if we ask the question directly	
- Single QA Precision: precision of single QA
- Single QA Recall: recall of single QA
- Single QA EM: Exact match of single QA
- Single QA Fuzzy EM: Fuzzy match of single QA

- Galois Answer: answer provided by Galois
- Galois Precision: precision of galois
- Galois Recall: recall of galois
- Galois EM: exact match of galois
- Galois Fuzzy EM: fuzzy match of galois

- Note: side note