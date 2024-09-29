
def second_task():
    print('@@@@@@@@@@@@@@@@@@@@@@@ Hello from second_task @@@@@@@@@@@@@@@@@@@@@@@')
    # raise ValueError('This will turns the python task in failed state')

def third_task():
    print('@@@@@@@@@@@@@@@@@@@@@@@ Hello from third_task @@@@@@@@@@@@@@@@@@@@@@@')
    # raise ValueError('This will turns the python task in failed state')

def on_success_dag(dict) :
    print('@@@@@@@@@@@@@@@@@@@@@@@ Success DAG @@@@@@@@@@@@@@@@@@@@@@@')
    print(dict)

def on_failure_dag(dict) : 
    print('@@@@@@@@@@@@@@@@@@@@@@@ Fail DAG @@@@@@@@@@@@@@@@@@@@@@@')
    print(dict)

def on_success_task(dict) :
    print('@@@@@@@@@@@@@@@@@@@@@@@ Success Task @@@@@@@@@@@@@@@@@@@@@@@')
    print(dict)

def on_failure_task(dict) : 
    print('@@@@@@@@@@@@@@@@@@@@@@@ Fail Task @@@@@@@@@@@@@@@@@@@@@@@')
    print(dict)
