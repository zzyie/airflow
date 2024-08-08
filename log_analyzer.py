from pathlib import Path

log_dir = '/Users/zyi26/airflow_project/logs'

file_list = Path(log_dir).rglob('*.log')

def analyze_file(file): 

    cur_list = []

    ct = 0
    
    for line in file: 

        if line.startswith('['): 

            l = line.split()

            if l[2] == 'ERROR':

                cur_list.append(line)
                ct += 1

    return ct, cur_list

    log_list = []
    ct = 0

    for file in file_list: 

        with open(file) as f: 

            cur_ct, cur_list = analyze_file(file)

            ct += cur_ct
            log_list.append(cur_list)
    
    print(f"Total number of errors: {ct}")
    print(f"Here are all the errors: {log_list}")
