from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    wait_fixed
)  
import openai
@retry(wait=wait_fixed(61), stop=stop_after_attempt(6))
def completion_with_backoff_chat(**kwargs):
    #print('=======Calling API=======')
    return openai.ChatCompletion.create(**kwargs)


def construct_chat_dict(role,content):
  return {"role":role,"content":content}

def construct_message_dict(instruction,question_answers):
    messages=[]

    messages.append(construct_chat_dict("system",instruction))

    for x in question_answers:
        q=x[0]
        messages.append(construct_chat_dict("user",q))
        a=x[1]
        messages.append(construct_chat_dict("assistant",a))    
    return messages



# model_arch='text-davinci-003'
# if SEQ_SCAN
#temp_unfilt_ans=[]
#temp_questions=[]
def add_more_seq_scan(temp_unfilt_ans,model_arch,temp_questions,old_pr,old_ans,max_tries=7,increase_threshold=5,verbose=True):
    i=0
    final_ans = old_ans

    while i < max_tries:
        if verbose:
            print('#',i+1)

        #pr = old_pr +old_ans+'\n\nQ: Give me more.\nA:'

        pr = old_pr + [ construct_chat_dict("assistant",old_ans)] + [ construct_chat_dict("user",'Give me more.')]
        messages = pr
        #print(pr)
        temp_questions.append("Give me more.")

        #run gpt3
        #response = completion_with_backoff(model=model_arch, prompt=pr, temperature=0,max_tokens= max_len)
        print(model_arch, messages)
        max_len = 400
        print(model_arch, messages)
        response = completion_with_backoff_chat(model=model_arch, messages=messages, temperature=0,max_tokens= max_len)

        #ans = response['choices'][0]['text']
        ans = response['choices'][0]['message']['content']

        if verbose:
            print(ans)
        temp_unfilt_ans.append(ans)

        final_ans+=ans

        len1 = len(set(old_ans.split(',')))
        len2 = len(set(old_ans.split(',')+ans.split(',')))
        #print(len1,len2)
        old_ans= ans
        old_pr = pr

        if (len2-len1)<increase_threshold:
            break

        i+=1
    #final_ans = list(set(final_ans.split(',')))

    return final_ans





import os
import json

def answer_batch_questions_chat(model_questions,pr,label,cache_fn,model_arch,max_len,verbose=False):
    cache_fn=label+'_cache.json'
    mode = 'r' if cache_fn in os.listdir('.') else 'w'
    cache=json.load(open(cache_fn,'r')) if mode=='r' else dict()
    ans=[]
    batch_pr = pr[:]   
    batch_mq = model_questions[:]  
    batch_ans = [None]*len(batch_pr)
    to_fetch_indices=[]

    for ind,pro in enumerate(batch_pr):
        pro_key = json.dumps(pro) if isinstance(pro,list) else pro
        if pro_key in cache:
            #print('Already in cache: ',batch_mq[ind],'\n')
            batch_ans[ind]=cache[pro_key]
        else:
            to_fetch_indices.append(ind)
    #print('INITIAL BATCH_ANS:',batch_ans)
    if verbose:
        print(f'In Cache: {len(batch_ans)-len(to_fetch_indices)}/{len(batch_ans)}')
    if to_fetch_indices:
        batch_pr_to_fetch = [batch_pr[tfi] for tfi in to_fetch_indices]
        batch_mq_to_fetch = [batch_mq[tfi] for tfi in to_fetch_indices]

        for i in range(len(batch_pr_to_fetch)):
            #print(batch_pr_to_fetch[i])
            response = completion_with_backoff_chat(model=model_arch, messages=batch_pr_to_fetch[i], temperature=0,max_tokens= max_len)

            if 'choices' in response:
                #print('RESPONSE:',response)
                batch_ans_fetched = response['choices'][0]['message']['content'] 

                #print('BATCH ANS FETCHED:',batch_ans_fetched)


                bpr = batch_pr_to_fetch[i]
                bmq= batch_mq_to_fetch[i]
                bans= batch_ans_fetched
                bpr_key = json.dumps(bpr) if isinstance(bpr,list) else bpr
                cache[bpr_key]=bans
                print('Added to cache:',bmq,'\n\n')
                print(f'Len of cache: {len(cache)}')
                json.dump(cache,open(cache_fn,"w"),indent=2)
            else: batch_ans_fetched=[]

            batch_ans[to_fetch_indices[i]] =batch_ans_fetched

        ans.extend(batch_ans)
    return batch_ans




import pandas as pd
from utils import *
from itertools import compress

def compute_node(node,model_arch,instr,few_shots,inst_funct,label,augmented_question_maps,verbose=False):

    cache_fn=label+'_cache.json'
    mode = 'r' if cache_fn in os.listdir('.') else 'w'
    if verbose: print('Mode: ',mode)
    cache=json.load(open(cache_fn,'r')) if mode=='r' else dict()
    #print(f'Len of cache: {len(cache)}')
    status = 'FINISHED'

    if node.op=='JOIN':
        #pr= instr+few_shots+""
        pr=construct_message_dict(instr,few_shots)




        left_questions = [node.key_left.replace('!!x!!',x) for x in node.l.answers[-1]]
        print('left questions',left_questions)


        #lpr = [pr + inst_funct(x) for x in left_questions]
        lpr = [pr + [ construct_chat_dict("user",x)] for x in left_questions]

        #batch_left_ans = answer_batch_questions(left_questions,lpr,label,cache_fn,model_arch,50,verbose=verbose)

        batch_left_ans = answer_batch_questions_chat(left_questions,lpr,label,cache_fn,model_arch,50,verbose=verbose)

        print('left answer',batch_left_ans)

        right_questions = [node.key_right.replace('!!x!!',x) for x in node.r.answers[-1]]
        print('right questions',right_questions)

        #rpr = [pr + inst_funct(x) for x in right_questions]
        rpr = [pr + [ construct_chat_dict("user",x)] for x in right_questions]



        #batch_right_ans = answer_batch_questions(right_questions,rpr,label,cache_fn,model_arch,400,verbose=verbose)
        batch_right_ans = answer_batch_questions_chat(right_questions,rpr,label,cache_fn,model_arch,50,verbose=verbose)

        print('right answer',batch_right_ans)

        left = pd.DataFrame({'left':node.l.answers[-1],"key":batch_left_ans})

        right = pd.DataFrame({'right':node.r.answers[-1],"key":batch_right_ans})
        print(left)
        print(right)
        print(left.merge(right,on='key',how='inner'))
        ans = list (left.merge(right,on='key',how='inner')[node.filter_key])
        print('JOINED ANSWER',ans)
        node.answers = ans
        node.status = status

        return



    node_text_list = node.text
    print(node_text_list)
    adjusted_nodes_list = node.adjusted_nodes
    if verbose:
        print('Tree Nodes: ',(["_".join(x) for x in adjusted_nodes_list]))

    for adjusted_node in  adjusted_nodes_list:
        op = adjusted_node[0]    

        if 'AGGREGATE_count_star()' in "_".join(adjusted_node) or 'AGGREGATE_OP_count' in "_".join(adjusted_node):
            try:

                ans = len(node.answers[-1] if len(adjusted_nodes_list)>1 else node.l.answers[-1])

            except:
                status = 'FAILED AGGREGATE OPERATION'
                ans=[]

            #temp_questions.append('COUNT')
            node.questions.append('COUNT')
            node.status = status
            #temp_unfilt_ans.append(ans)
            node.unfiltered_answers.append(ans)

            #temp_ans.append(ans)
            node.answers.append(ans)

            if verbose:
                print('Answer: ',ans)


        elif 'AGGREGATE_OP' in op:

            func = adjusted_node[1]
            #proj_ind = node_ind-1

            #if verbose:
            #    print('PROJ IND:',proj_ind)
            #    print('Answers: ',temp_ans[proj_ind])

            #temp_questions.append(func+'('+",".join(temp_ans[proj_ind])+')')
            node.questions.append(func+'('+",".join(node.answers[-1])+')')



            #prev_ans=[]

            try:
                #prev_ans = [x[:-1] if x[-1]=='.' else x for x in temp_ans[proj_ind]]

                prev_ans = node.l.ans[-1]
                prev_ans = [x[:-1] if x[-1]=='.' else x for x in prev_ans]

                if func!='count':
                    numer_ans = [replace_units(x) for x in prev_ans]
                    #print('Replacing UNITS:',numer_ans)
                    numer_ans = [re.sub("[^0-9.*]","",x) for x in numer_ans]
                    #print('Remove other text:',numer_ans)
                    numer_ans = [x for x in numer_ans if x]
                    numer_ans = [x[:-1] if x[-1]=='.' else x for x in numer_ans]
                    numer_ans = [x for x in numer_ans if x]
                    numer_ans = [eval(x) for x in numer_ans]
                    #print('Evaluate numbers:',numer_ans)
                else:
                    numer_ans = [x for x in numer_ans if x]
                #if verbose:
                    #print('Removing non-nuemrical: ',numer_ans)
                if func!='count':
                    numer_ans = [ float(x) for x in numer_ans]
                if verbose:
                    print('Numerical Parsing: ',numer_ans)
                ans = map_func(func)(numer_ans)
                node.status = 'FINISHED'

            except:
                node.status = 'FAILED AGGREGATE OPERATION'
                ans=[]

            #temp_unfilt_ans.append(ans)
            node.unfiltered_answers.append(ans)
            #temp_ans.append(ans)
            node.answers.append(ans)

            if verbose:
            #    print('Function used: ',map_func(func))
                print('Unfiltered answer:',ans)
                print('Filtered answer:',ans)
                print('Status: ',status)
        else:
            k = '_'.join(adjusted_node)
            question = node.filled_question if hasattr(node,'filled_question') else augmented_question_maps[k]
            if verbose:
                print('OP: ',k)
                print('Q: ',question)

            # adjust max length generation
            if op == 'SEQ_SCAN':max_len = 400 
            elif op=='FILTER': max_len=1 if 'turbo' not in model_arch else 2
            else: max_len = 50
            
            #proj_ind = -1

            if op=='AGGREGATE_PROJ':
                prev_ans = node.l.answers[-1]
            elif  op =='PROJECTION':
                prev_ans = node.l.answers[-1]
            else:
                prev_ans = node.l.answers[-1] if node.l and node.l.answers else []


            #     proj_ind = [ind for ind,x in enumerate(tree_nodes) if x[0]=='AGGREGATE_PROJ'][0]-1
            # elif  op=='PROJECTION':
            #     proj_ind = [ind for ind,x in enumerate(tree_nodes) if x[0]=='PROJECTION'][0]-1

            if '!!x!!' in question: model_questions = [question.replace('!!x!!',x) for x in prev_ans ]
            #elif '!!list!!' in question:
            #    prev_ans = temp_ans[proj_ind]
            #    model_questions = [question.replace('!!list!!',", ".join(prev_ans))]
            else: model_questions = [question]

            node.questions.append(model_questions)
            #pr= instr+few_shots+""
            #pr = [pr + inst_funct(x) for x in model_questions]

            pr=construct_message_dict(instr,few_shots)
            pr = [pr +[ construct_chat_dict("user",x)] for x in model_questions]




            #if verbose:    print('Prompt: ',pr)


            ans=[]
            if op == 'SEQ_SCAN':
                if k in cache: 
                    ans = cache[k]
                else:
                    print('NOT IN CACHE')
                    old_pr = pr[0]
                    #if verbose:
                    #    print('RUNNING SEQUENTIAL SCANS...')
                    #    print(old_pr)

                    #response = completion_with_backoff(model=model_arch, prompt=old_pr, temperature=0,max_tokens= max_len)
                    print(model_arch,old_pr)
                    response = completion_with_backoff_chat(model=model_arch, messages=old_pr, temperature=0,max_tokens= max_len)

                    old_pr_key = json.dumps(old_pr) if isinstance(old_pr,list) else old_pr
                    cache[old_pr_key] = response

                    json.dump(cache,open(cache_fn,"w"),indent=2)

                    #old_ans = response['choices'][0]['text']
                    old_ans = response['choices'][0]['message']['content']
                    if verbose:
                        print(old_ans)
                    temp_unfilt_ans = []
                    temp_questions = []
                    ans = [add_more_seq_scan(temp_unfilt_ans,model_arch,temp_questions,old_pr,old_ans,max_tries=7,increase_threshold=5)]

                    node.unfiltered_answers.append(temp_unfilt_ans)
                    node.questions.append(temp_questions)

                    cache[k] = ans
                    json.dump(cache,open(cache_fn,"w"),indent=2)
                    

            else:
                    batch_ans = answer_batch_questions_chat(model_questions,pr,label,cache_fn,model_arch,max_len,verbose=verbose)
                    ans.extend(batch_ans)


            node.unfiltered_answers.append(ans)
            if verbose:   print('Unfiltered Answer: ',ans)

            if op == 'SEQ_SCAN':
              if ans:
                  ans = ans[0]
                  ans = ans[:-1] if ans[-1]=='.' else ans
                  ans = ans.replace(' and ','')
                  ans = ans.split(',') #if ',' in ans else ans.split(' ')
                  ans = [x for x in ans if '.' not in x] #remove 'India.Yemen'
                  ans = list(set(ans))
                  ans =[x for x in ans if x]
              node.answers.append(ans)
              if verbose:
                  print('Final Answer: ',ans)

            elif op == 'FILTER':
                filtered_ans=[]
                if ans:
                    bool_index = [x.replace('.','').strip() for x in ans]
                    bool_index = [x=='Yes' for x in bool_index]
                    filtered_ans = list(compress(node.l.answers[-1], bool_index))
                if verbose:
                    print('Final Answer: ', filtered_ans)
                node.answers.append(filtered_ans)
                if not filtered_ans: 
                    status = 'EMPTY'
                    if verbose:
                        print('EMPTY')
                    break

            elif op != 'AGGREGATE_OP' and op!='AGGREGATE_count_star()':
                  #ans = ans[0]
                  node.answers.append(ans)
                  if verbose:
                      print('Final Answer: ', ans)
            print('\n')
            node.status = status


def compute_tree(node,model_arch,instr,few_shots,inst_funct,label,augmented_question_maps,verbose=False):
    if node and node.text:
        compute_tree(node.l,model_arch,instr,few_shots,inst_funct,label,augmented_question_maps,verbose=verbose)
        compute_tree(node.r,model_arch,instr,few_shots,inst_funct,label,augmented_question_maps,verbose=verbose)
        compute_node(node,model_arch,instr,few_shots,inst_funct,label,augmented_question_maps,verbose=verbose)



from QueryTree import *

import time

c5=None

def GPT_SPW_seq(model_arch,df,instr,few_shots,inst_funct,label,augmented_question_maps,query_plan_dict,verbose=False):
    global b
    # create file for answers
    json.dump([],open(label+".json","w"),indent=3)
    
    # check if there is cache data
    cache_fn=label+'_cache.json'
    mode = 'r' if cache_fn in os.listdir('.') else 'w'
    if verbose: print('Mode: ',mode)
    cache=json.load(open(cache_fn,'r')) if mode=='r' else dict()

    for index,row in df.iterrows():

        # get query
        query = row.Query
        # get duckdb con
        con = run_db(db_files[row.Database])
        con.execute("PRAGMA enable_profiling='query_tree';")
        con.execute("PRAGMA explain_output='ALL';")
        #get logical execution plan
        if verbose:
            print(query)
            print('\n')
        if query in query_plan_dict: 
            root = query_plan_dict[query]
        else:
            try:
                con.execute("EXPLAIN "+query.replace('"',"'"))
                s = con.fetchall()[0][1].split('\n')
                if verbose:
                    print("\n".join(s))
                    print('\n')
                root = parse_query_tree(s)
            except:
                continue
        b=root
        #tree_nodes = []
        #get_tree_elements(root,tree_nodes)


        #invert tree nodes
        #tree_nodes = tree_nodes[::-1]

        #separate filter
        #tree_nodes = adjust_nodes(tree_nodes)
        tree_adjust_nodes(root)

        #if verbose:
        #    print_tree(root)

        # temp_ans=[]
        # temp_unfilt_ans=[]
        # temp_questions=[]

        
        compute_tree(root,model_arch,instr,few_shots,inst_funct,label,augmented_question_maps,verbose=verbose)
        tree_nodes,questions,answers,unfiltered_answers = get_snippet(root,[],[],[],[])

        snippet = {'Gold Question':row.Question,'Gold Answer':row.Answer,'Query':row.Query,
                   'Tree Nodes':tree_nodes,'LP Questions':questions,'LP Answers':answers,
                   'LP Unfiltered Answers':unfiltered_answers,'qqqqqqqqqqqq':root.status}

        log = json.load(open(label+".json","r"))
        log.append(snippet)
        json.dump(log,open(label+".json","w"),indent=3)
        #json.dump(cache,open(cache_fn,"w"),indent=2)
        time.sleep(3)
        print("===================================================================================")

